{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DoAndIfThenElse #-}

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
-- | Intelligently derive a FromRow instance for the given type. For
-- each field in the given data type, we try to infer, albeit crudely,
-- whether the field is an instance of 'FromField' or 'FromRow'. If we
-- can't figure it out, we default to 'FromField'. This will hopefully
-- cover over 90% of (even complex) cases.
--
-- We pass the desired type with the slighly more complex than usual
-- QQ syntax to enable more flexible use. See below for some examples.
--
-- @
-- data Key e = Key { unKey :: Int }
--   deriving (Eq,Show,Read,Ord)
--
-- data Entity e = E {
--       eKey :: Key e
--     , eRec :: e
--     } deriving (Eq,Show,Read,Ord)
--
-- instance FromField (Key e) where
--     fromField f v = Key `fmap` fromField f v
--
-- instance (FromRow k) => FromRow (Entity k) where
--     rowParser = E <$> field <*> rowParser
--
-- data Car = Car {
--       carName :: Text
--     , carModel :: Text
--     }
-- 
-- data PersonG c d = Person {
--       personCar :: c
--     , personWhatever :: d
--     , personCarPointer :: Key Car
--     , personName :: Text
--     , personCar2 :: Entity Car
--     }
--
-- $(deriveFromRow [t| Car |])
-- $(deriveFromRow [t| forall b. PersonG (Entity Car) b |])
-- $(deriveFromRow [t| forall b. PersonG Car b |])
-- $(deriveFromRow [t| forall a b. PersonG a b |])
-- $(deriveFromRow [t| forall a b. PersonG a (Entity (PersonG Int b)) |])
-- @
deriveFromRow :: TypeQ -> Q [Dec]
deriveFromRow ty = do
  ty' <- ty
  let names@(name:_) = collectTypeNames ty'
  withType name $ \ tvars cons -> (: []) `fmap` fromCons ty' names tvars cons
  where
    appParse a f = [| $(a) <*> $(f)  |]
    fromCons ty' (consName : consFields) tvars cons = do
      -- Get instances for FromRow and FromField
      -- rows <- reifyInstances $ mkName "FromRow"
      -- fields <- reifyInstances $ mkName "FromField"
      -- superclass generator
      let classMk t = case isVarName t of
                       True -> Just $ ClassP (mkName "FromField") [VarT t]
                       False -> Nothing
          -- decide whether to use field or rowParser for this field
          -- func t = if elem t' rows then [|rowParser|] 
          --          else if elem t' fields then [|field|]
          --          else if fieldIsTypeParam t then [|field|]
          --          else [|rowParser|]
          --     where t' = head $ flattenCons t
                   
          -- this would be the elegant way to figure out if current
          -- record field's types belong to FromField or FromRow
          -- typeclasses. Unfortunately, GHC panics when using this.
          -- 
          func t = 
            case t of
              VarT{} -> 
                case fieldIsTypeParam t of
                  False -> [|rowParser|] 
                  True -> [|field|]
              _ -> do
                isRow <- isInstance (mkName "FromRow") [t]
                isField <- isInstance (mkName "FromField") [t]
                dbg $ "RowValue: " ++ show isRow
                dbg $ "FieldValue: " ++ show isField
                if isRow then [|rowParser|]
                  else if isField then [|field|]
                  else if fieldIsTypeParam t then [|field|]
                  else [|rowParser|]

          -- lookup from data types defined type vars to the type
          -- constructor fields passed in at splie time
          paramIndex = zip (map tvbName tvars) consFields
          
          -- if it's a type variable, then we use 'field', if it's an
          -- unknown concrete type, then we assume it has a 'FromRow'
          -- instance
          fieldIsTypeParam t = case lookup (head $ collectTypeNames t) paramIndex of
                               Just x' -> isVarName x'
                               Nothing -> isVarType t
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
          let step acc ty = appParse acc (func ty)
          in foldl step ([| return $(conE cname) |]) $ map snd vars
        (RecC cname vars) : _ -> 
          let step acc x = do
                             dbg $ "Current record type " ++ show x
                             dec <- func x
                             dbg $ "Decision is " ++ show dec
                             appParse acc (func x)
          in foldl step ([| return $(conE cname) |]) $ 
             map (\ (_,_,x) -> x) vars
      instanceD
        (return $ catMaybes $ map classMk consFields)
        (conT ''FromRow `appT` insType)
        ([return $ FunD (mkName "rowParser") [Clause [] (NormalB bodyExp) []]])


-- dbg = runIO . putStrLn
dbg _ = return ()

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
        TySynD _ tvars ty -> 
          case ty of
            ConT nm -> withType nm f
            AppT (ConT nm) _ -> withType nm f
            _ -> error "Can't process this type synonym"



-------------------------------------------------------------------------------
flattenCons (AppT c1 c2) = flattenCons c1 ++ flattenCons c2
flattenCons c = [c]


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