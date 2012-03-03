{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}

module Database.PostgreSQL.Simple.FromRowTH 
    ( deriveFromRow
    ) where

-------------------------------------------------------------------------------
import Control.Applicative
import Data.Char                            (isLower)
import Data.Maybe
import Language.Haskell.TH
-------------------------------------------------------------------------------
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.Param
import Database.PostgreSQL.Simple.Result
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- | Try to collect the first level type names from the type
-- constructor given.
collectTypeNames (ConT nm) = [nm]
collectTypeNames (VarT nm) = [nm]
collectTypeNames (ForallT tvars _ ty) = collectTypeNames ty

-- Don't collect nested type constructors beyond the first level. It
-- screws stuff up later.
collectTypeNames (AppT (ConT nm1) (AppT (ConT nm2) (ConT _))) = [nm1,nm2]
collectTypeNames (AppT (AppT (ConT nm1) (ConT _)) (ConT nm2)) = [nm1,nm2]
collectTypeNames (AppT x y) = collectTypeNames x ++ collectTypeNames y
collectTypeNames _ = []


-------------------------------------------------------------------------------
-- | Only work with data and newtype declarations
withType name f = do
  typ <- reify name
  case typ of
    TyConI dec -> 
      case dec of
        DataD _ _ tvars cons _ -> f tvars cons
        NewtypeD _ _ tvars con _ -> f tvars [con]
        TySynD _ tvars ty -> error "Can't derive type synonyms yet"


-------------------------------------------------------------------------------
-- | Get the instances of a given Typeclass
reifyInstances nm = do
  i <- reify nm
  case i of
    ClassI _ is -> return $ concatMap ci_tys is
    _ -> error "reifyInstances should only be called on a class name"


-------------------------------------------------------------------------------
-- | Intelligently derive a FromRow instance for the given Type
deriveFromRow :: TypeQ -> Q [Dec]
deriveFromRow ty = do
  ty' <- ty
  runIO $ print ty'
  let names@(name:_) = collectTypeNames ty'
  withType name $ \ tvars cons -> (: []) `fmap` fromCons ty' names tvars cons
  where
    appParse a f = [| $(a) <*> $(f)  |]
    fromCons ty' (consName : consFields) tvars cons = do
      -- Get instances for FromRow and FromField
      rows <- reifyInstances $ mkName "FromRow"
      fields <- reifyInstances $ mkName "FromField"
      -- superclass generator
      let classMk t = case isVarName t of
                       True -> Just $ ClassP (mkName "FromField") [VarT t]
                       False -> Nothing
          -- decide whether to use field or rowParser for this field
          func t = if elem t rows then [|rowParser|] 
                   else if elem t fields then [|field|]
                   else if fieldIsTypeParam (head $ collectTypeNames t) then [|field|]
                   else [|rowParser|]
          -- lookup from data types defined type vars to the type
          -- constructor fields passed in at splie time
          paramIndex = zip (map tvbName tvars) consFields
          
          -- if it's a type variable, then we use field, if it's an
          -- unknown concrete type, then we assume it has a FromRow
          -- instance
          fieldIsTypeParam x = case lookup x paramIndex of
                               Just x' -> isVarName x'
                               Nothing -> True
          -- construct the (MyData a b c d) part in instance declaration
          classNameRoot = conT consName
          classNameStep acc var = acc `appT` (if isVarName var then varT var else conT var)
          -- insType = foldl classNameStep classNameRoot consFields
          insType = do
            case ty' of
              ForallT _ _ x -> return x
              x -> return x
      bodyExp <- case cons of
        [] -> error "deriveFromRow can only process regular data declarations"
        (NormalC cname vars) : _ -> 
          let step acc (_,ty) = do appParse acc (func ty)
          in foldl step ([| return $(conE cname) |]) vars
        (RecC cname vars) : _ -> 
          let step acc (_,_,ty) = do
                             -- runIO $ print ty
                             -- runIO $ print consFields
                             -- runIO . print  =<< func ty
                             appParse acc (func ty)
          in foldl step ([| return $(conE cname) |]) vars
      instanceD
        (return $ catMaybes $ map classMk consFields)
        (conT ''FromRow `appT` insType)
        ([return $ FunD (mkName "rowParser") [Clause [] (NormalB bodyExp) []]])



-------------------------------------------------------------------------------
isVarType (VarT{}) = True
isVarType _ = False


-------------------------------------------------------------------------------
isVarName a = if (isLower . head . show) a then True else False
    
    
-------------------------------------------------------------------------------
-- | Extracts the name from a type variable binder.
tvbName :: TyVarBndr -> Name
tvbName (PlainTV  name  ) = name
tvbName (KindedTV name _) = name