{-# LANGUAGE BangPatterns, OverloadedStrings, DeriveDataTypeable, FlexibleInstances,
             ScopedTypeVariables, TypeOperators, GeneralizedNewtypeDeriving #-}

------------------------------------------------------------------------------
-- |
-- Module:      Database.PostgreSQL.Simple.FromRow
-- Copyright:   (c) 2011 MailRank, Inc.
--              (c) 2011 Leon P Smith
--              (c) 2012 Ozgun Ataman
-- License:     BSD3
-- Maintainer:  Leon P Smith <leon@melding-monads.com>
-- Stability:   experimental
-- Portability: portable
--
-- The 'FromRow' typeclass, for converting a row of results
-- returned by a SQL query into a more useful Haskell representation.
--
-- Predefined instances are provided for tuples containing up to ten
-- elements.
------------------------------------------------------------------------------

module Database.PostgreSQL.Simple.FromRow
    ( FromRow(..)
    , convertError
    -- * Helpers For Parsing Custom Data Types
    , RowParser
    , field
    , peekField
    , skip
    , rewind
    , (:.) (..)
    , Z (..)
    , runRowParser
    , RowParseError(..)
    ) where

-------------------------------------------------------------------------------
import           Control.Applicative                  (Applicative(..), (<$>))
import           Control.Exception                    (SomeException(..), Exception(..), throw)
import           Control.Monad.State.Strict
import           Data.ByteString                      (ByteString)
import qualified Data.ByteString.Char8                as B
import           Data.Typeable
import qualified Database.PostgreSQL.LibPQ            as LibPQ (Result)
-------------------------------------------------------------------------------
import           Database.PostgreSQL.Simple.FromField (ResultError(..), FromField(..))
import           Database.PostgreSQL.Simple.Internal
import           Database.PostgreSQL.Simple.Ok
import           Database.PostgreSQL.Simple.Types     (Only(..))
-------------------------------------------------------------------------------


-- | A collection type that can be converted from a list of strings.
--
-- Instances should use the 'fromField' method of the 'FromField' class
-- to perform conversion of each element of the collection.
--
-- This example instance demonstrates how to convert a two-column row
-- into a Haskell pair. Each field in the metadata is paired up with
-- each value from the row, and the two are passed to 'fromField'.
--
-- @
-- instance ('FromField' a, 'FromField' b) => 'FromRow' (a,b) where
--     'convertResults' [fa,fb] [va,vb] = do
--               !a <- 'fromField' fa va
--               !b <- 'fromField' fb vb
--               'return' (a,b)
--     'convertResults' fs vs  = 'convertError' fs vs 2
-- @
--
-- Notice that this instance evaluates each element to WHNF before
-- constructing the pair.  This property is important enough that its
-- a rule all 'FromRow' instances should follow:
--
--   * Evaluate every 'Result' value to WHNF before constructing
--     the result
--
-- Doing so keeps resource usage under local control by preventing the
-- construction of potentially long-lived thunks that are forced
-- (or not) by the consumer.
--
-- This is important to postgresql-simple-0.0.4 because a wayward thunk
-- causes the entire LibPQ.'LibPQ.Result' to be retained. This could lead
-- to a memory leak, depending on how the thunk is consumed.
--
-- Note that instances can be defined outside of postgresql-simple,
-- which is often useful.   For example,  here is an attempt at an
-- instance for a user-defined pair:
--
-- @
-- data User = User { firstName :: String, lastName :: String }
--
-- instance 'FromRow' User where
--     'convertResults' [fa,qfb] [va,vb] = User \<$\> a \<*\> b
--        where  !a =  'fromField' fa va
--               !b =  'fromField' fb vb
--     'convertResults' fs vs  = 'convertError' fs vs 2
-- @
--
-- In this example,  the bang patterns are not used correctly.  They force
-- the data constructors of the 'Either' type,  and are not forcing the
-- 'Result' values we need to force.  This gives the consumer of the
-- 'FromRow' the ability to cause the memory leak,  which is an
-- undesirable state of affairs.
--
-- Minimum complete definition: 'convertResults' or 'rowParser'
class FromRow a where

    ---------------------------------------------------------------------------
    -- | 'rowParser' offers a convenient way to parse rows into custom
    -- data types using an applicative and/or monadic parsing style.
    -- See 'RowParser' for documentation.
    rowParser :: RowParser a
    rowParser = rowParserError ParserNotDefined

    convertResults :: [Field] -> [Maybe ByteString] -> Ok a
    convertResults = runRowParser rowParser
    -- ^ Convert values from a row into a Haskell collection.
    --
    -- This function will return a 'ResultError' if conversion of the
    -- collection fails.
    

-------------------------------------------------------------------------------
instance (FromField a) => FromRow (Only a) where
    convertResults [fa] [va] = do
              !a <- fromField fa va
              return (Only a)
    convertResults fs vs  = convertError fs vs 1

instance (FromField a, FromField b) => FromRow (a,b) where
    convertResults [fa,fb] [va,vb] = do
              !a <- fromField fa va
              !b <- fromField fb vb
              return (a,b)
    convertResults fs vs  = convertError fs vs 2

instance (FromField a, FromField b, FromField c) => FromRow (a,b,c) where
    convertResults [fa,fb,fc] [va,vb,vc] = do
              !a <- fromField fa va
              !b <- fromField fb vb
              !c <- fromField fc vc
              return (a,b,c)
    convertResults fs vs  = convertError fs vs 3

instance (FromField a, FromField b, FromField c, FromField d) =>
    FromRow (a,b,c,d) where
    convertResults [fa,fb,fc,fd] [va,vb,vc,vd] = do
              !a <- fromField fa va
              !b <- fromField fb vb
              !c <- fromField fc vc
              !d <- fromField fd vd
              return (a,b,c,d)
    convertResults fs vs  = convertError fs vs 4

instance (FromField a, FromField b, FromField c, FromField d, FromField e) =>
    FromRow (a,b,c,d,e) where
    convertResults [fa,fb,fc,fd,fe] [va,vb,vc,vd,ve] = do
              !a <- fromField fa va
              !b <- fromField fb vb
              !c <- fromField fc vc
              !d <- fromField fd vd
              !e <- fromField fe ve
              return (a,b,c,d,e)
    convertResults fs vs  = convertError fs vs 5

instance (FromField a, FromField b, FromField c, FromField d, FromField e, FromField f) =>
    FromRow (a,b,c,d,e,f) where
    convertResults [fa,fb,fc,fd,fe,ff] [va,vb,vc,vd,ve,vf] = do
              !a <- fromField fa va
              !b <- fromField fb vb
              !c <- fromField fc vc
              !d <- fromField fd vd
              !e <- fromField fe ve
              !f <- fromField ff vf
              return (a,b,c,d,e,f)
    convertResults fs vs  = convertError fs vs 6

instance (FromField a, FromField b, FromField c, FromField d, FromField e, FromField f,
          FromField g) =>
    FromRow (a,b,c,d,e,f,g) where
    convertResults [fa,fb,fc,fd,fe,ff,fg] [va,vb,vc,vd,ve,vf,vg] = do
              !a <- fromField fa va
              !b <- fromField fb vb
              !c <- fromField fc vc
              !d <- fromField fd vd
              !e <- fromField fe ve
              !f <- fromField ff vf
              !g <- fromField fg vg
              return (a,b,c,d,e,f,g)
    convertResults fs vs  = convertError fs vs 7

instance (FromField a, FromField b, FromField c, FromField d, FromField e, FromField f,
          FromField g, FromField h) =>
    FromRow (a,b,c,d,e,f,g,h) where
    convertResults [fa,fb,fc,fd,fe,ff,fg,fh] [va,vb,vc,vd,ve,vf,vg,vh] = do
              !a <- fromField fa va
              !b <- fromField fb vb
              !c <- fromField fc vc
              !d <- fromField fd vd
              !e <- fromField fe ve
              !f <- fromField ff vf
              !g <- fromField fg vg
              !h <- fromField fh vh
              return (a,b,c,d,e,f,g,h)
    convertResults fs vs  = convertError fs vs 8

instance (FromField a, FromField b, FromField c, FromField d, FromField e, FromField f,
          FromField g, FromField h, FromField i) =>
    FromRow (a,b,c,d,e,f,g,h,i) where
    convertResults [fa,fb,fc,fd,fe,ff,fg,fh,fi] [va,vb,vc,vd,ve,vf,vg,vh,vi] =
           do
              !a <- fromField fa va
              !b <- fromField fb vb
              !c <- fromField fc vc
              !d <- fromField fd vd
              !e <- fromField fe ve
              !f <- fromField ff vf
              !g <- fromField fg vg
              !h <- fromField fh vh
              !i <- fromField fi vi
              return (a,b,c,d,e,f,g,h,i)
    convertResults fs vs  = convertError fs vs 9

instance (FromField a, FromField b, FromField c, FromField d, FromField e, FromField f,
          FromField g, FromField h, FromField i, FromField j) =>
    FromRow (a,b,c,d,e,f,g,h,i,j) where
    convertResults [fa,fb,fc,fd,fe,ff,fg,fh,fi,fj]
                   [va,vb,vc,vd,ve,vf,vg,vh,vi,vj] =
           do
              !a <- fromField fa va
              !b <- fromField fb vb
              !c <- fromField fc vc
              !d <- fromField fd vd
              !e <- fromField fe ve
              !f <- fromField ff vf
              !g <- fromField fg vg
              !h <- fromField fh vh
              !i <- fromField fi vi
              !j <- fromField fj vj
              return (a,b,c,d,e,f,g,h,i,j)
    convertResults fs vs  = convertError fs vs 10

f <$!> (!x) = f <$> x
infixl 4 <$!>

instance FromField a => FromRow [a] where
    convertResults fs vs = foldr fromField' (pure []) (zip fs vs)
      where fromField' (f,v) as = (:) <$!> fromField f v <*> as
    {-# INLINE convertResults #-}





-------------------------------------------------------------------------------
-- | Throw a 'ConversionFailed' exception, indicating a mismatch
-- between the number of columns in the 'Field' and row, and the
-- number in the collection to be converted to.
convertError :: [Field]
             -- ^ Descriptors of fields to be converted.
             -> [Maybe ByteString]
             -- ^ Contents of the row to be converted.
             -> Int
             -- ^ Number of columns expected for conversion.  For
             -- instance, if converting to a 3-tuple, the number to
             -- provide here would be 3.
             -> Ok a
convertError fs vs n = Errors . return . SomeException $ ConversionFailed
    (show (length fs) ++ " values: " ++ show (zip (map typename fs)
                                                  (map (fmap ellipsis) vs)))
    (show n ++ " slots in target type")
    "mismatch between number of columns to convert and number in target type"


-------------------------------------------------------------------------------
ellipsis :: ByteString -> ByteString
ellipsis bs
    | B.length bs > 15 = B.take 10 bs `B.append` "[...]"
    | otherwise        = bs


                              ------------------
                              -- Parsing Help --
                              ------------------


-------------------------------------------------------------------------------
type FTuple = (Field, Maybe ByteString)


-------------------------------------------------------------------------------
-- | Our parse state is a tuple of unconsumed and consumed states.
-- Note that consumed states are in reverse order for performance
-- reasons.
type RowParserState = ([FTuple], [FTuple])


-------------------------------------------------------------------------------
-- | A stateful row parsing environment.
--
-- While parsing each row, we go field-by-field and consume the parsed
-- fields along the way. This allows us to use the convenient and
-- minimal applicative/monad forms while parsing rows into custom data
-- types:
--
-- @
-- data User = User name lastname age
--
-- instance "FromRow" User where
--     "rowParser" = do
--           "skip" 1    -- skip the first column
--           name      <- "field"
--           lastname  <- "field"
--           "skip" 1    -- skip another column
--           age       <- "field"
--           return $ User name lastname age
-- @
newtype RowParser a 
  = RowParser { unRowParser :: (StateT RowParserState Ok a) }
  deriving (Functor, Applicative, Monad, MonadState RowParserState)


-------------------------------------------------------------------------------
-- | Exceptions that may be raised while trying to parse a row
data RowParseError 
    = ParserNotDefined
    | NoConsumedFields
  deriving (Eq, Show, Typeable)

instance Exception RowParseError


-------------------------------------------------------------------------------
-- | 
rowParserError :: (Exception e) => e -> RowParser a
rowParserError = RowParser . lift . Errors . return . SomeException


-------------------------------------------------------------------------------
notEnoughCols :: RowParser a
notEnoughCols = do
  let disp cons = map conv $ reverse cons
      conv (f,v) = (typename f, ellipsis `fmap` v)
  st <- get
  case st of
    ([], cons) -> rowParserError $ ConversionFailed
                     (show (length cons) ++ " values: " ++ show (disp cons))
                     (show "target haskell type")
                     ("Not enough columns to parse the target field")
    _ -> error "notEnoughCols called with unconsumed columns."
                     


-------------------------------------------------------------------------------
-- | Parse the next field in line, consuming it in process
field :: FromField a => RowParser a
field = do
  st <- get
  case st of
    ((f,v) : rest, cons) -> do
      res <- RowParser . lift $ fromField f v
      put (rest, (f,v) : cons)
      return res
    _ -> notEnoughCols 


-------------------------------------------------------------------------------
-- | Parse the next field in line without consuming it
peekField :: FromField a => RowParser a
peekField = do
  st <- get
  case st of
    ((f,v) : _, _) -> RowParser . lift $ fromField f v
    _ -> notEnoughCols


-------------------------------------------------------------------------------
-- | Rewind n consumed fields, raising an exception if there aren't
-- enough fields that have been consumed.
rewind :: Int -> RowParser ()
rewind n = replicateM_ n $ do
  st <- get
  case st of
    (xs, (y:ys)) -> put (y:xs, ys)
    _ -> rowParserError NoConsumedFields


-------------------------------------------------------------------------------
-- | Consume the next n fields in line without parsing
skip :: Int -> RowParser ()
skip n = modify $ \ (xs, ys) -> let xs' = take n xs
                                in (drop n xs, reverse xs' ++ ys)


-------------------------------------------------------------------------------
-- | Run a RowParser
runRowParser :: RowParser a -> [Field] -> [Maybe ByteString] -> Ok a
runRowParser p fs vs = evalStateT (unRowParser p) (zip fs vs, [])


infixl 3 :.

-------------------------------------------------------------------------------
-- | A composite type to parse your custom data structures without
-- having to define dummy newtype wrappers every time.
--
--
-- > instance FromRow MyData where ...
-- 
-- > instance FromRow MyData2 where ...
--
--
-- then I can do the following for free:
--
-- @
-- res <- query' c "..."
-- forM res $ \\(MyData{..} :. MyData2{..}) -> do
--   ....
-- @
data h :. t = h :. t deriving (Eq,Show,Read)

-------------------------------------------------------------------------------
-- | A zero datatype when extracting just a single field in a
-- composite data type.
--
-- > MyData :. Z
data Z = Z deriving (Eq,Show,Read)



-------------------------------------------------------------------------------
-- | Any composite type that ends in 'Z' is parseable
instance (FromRow a) => FromRow (a :. Z) where
    rowParser = (:. Z) <$> rowParser


-------------------------------------------------------------------------------
-- | Composite types get the instance for free
instance (FromRow a, FromRow b) => FromRow (a :. b) where
    rowParser = (:.) <$> rowParser <*> rowParser
      

-------------------------------------------------------------------------------
-- | Trying to parse into a 'Z' will simply skip a field and return Z
instance FromRow Z where
    rowParser = skip 1 *> return Z