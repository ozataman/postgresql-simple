{-# LANGUAGE BangPatterns, OverloadedStrings, DeriveDataTypeable,  ScopedTypeVariables,
             GeneralizedNewtypeDeriving #-}

------------------------------------------------------------------------------
-- |
-- Module:      Database.PostgreSQL.Simple.FromRow
-- Copyright:   (c) 2011 MailRank, Inc.
--              (c) 2011 Leon P Smith
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
    (
      FromRow(..)
    , convertError
    -- * Helpers For Parsing Custom Data Types
    , RowParser
    , field
    , peekField
    , skip
    , rewind
    , runRowParser
    , RowParseError(..)
    ) where

import Data.Typeable
import Control.Monad.State.Strict
import Control.Applicative (Applicative(..), (<$>))
import Control.Exception (SomeException(..), Exception(..), throw)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Either()
import Database.PostgreSQL.Simple.Internal
import Database.PostgreSQL.Simple.Result (ResultError(..), Result(..))
import Database.PostgreSQL.Simple.Types (Only(..))
import qualified Database.PostgreSQL.LibPQ as LibPQ (Result)

-- | A collection type that can be converted from a list of strings.
--
-- Instances should use the 'convert' method of the 'Result' class
-- to perform conversion of each element of the collection.
--
-- This example instance demonstrates how to convert a two-column row
-- into a Haskell pair. Each field in the metadata is paired up with
-- each value from the row, and the two are passed to 'convert'.
--
-- @
-- instance ('Result' a, 'Result' b) => 'FromRow' (a,b) where
--     'convertResults' [fa,fb] [va,vb] = do
--               !a <- 'convert' fa va
--               !b <- 'convert' fb vb
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
--        where  !a =  'convert' fa va
--               !b =  'convert' fb vb
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

    convertResults :: [Field] -> [Maybe ByteString] -> Either SomeException a
    convertResults = runRowParser rowParser
    -- ^ Convert values from a row into a Haskell collection.
    --
    -- This function will return a 'ResultError' if conversion of the
    -- collection fails.
    

-------------------------------------------------------------------------------
instance (Result a) => FromRow (Only a) where
    convertResults [fa] [va] = do
              !a <- convert fa va
              return (Only a)
    convertResults fs vs  = convertError fs vs 1

instance (Result a, Result b) => FromRow (a,b) where
    convertResults [fa,fb] [va,vb] = do
              !a <- convert fa va
              !b <- convert fb vb
              return (a,b)
    convertResults fs vs  = convertError fs vs 2

instance (Result a, Result b, Result c) => FromRow (a,b,c) where
    convertResults [fa,fb,fc] [va,vb,vc] = do
              !a <- convert fa va
              !b <- convert fb vb
              !c <- convert fc vc
              return (a,b,c)
    convertResults fs vs  = convertError fs vs 3

instance (Result a, Result b, Result c, Result d) =>
    FromRow (a,b,c,d) where
    convertResults [fa,fb,fc,fd] [va,vb,vc,vd] = do
              !a <- convert fa va
              !b <- convert fb vb
              !c <- convert fc vc
              !d <- convert fd vd
              return (a,b,c,d)
    convertResults fs vs  = convertError fs vs 4

instance (Result a, Result b, Result c, Result d, Result e) =>
    FromRow (a,b,c,d,e) where
    convertResults [fa,fb,fc,fd,fe] [va,vb,vc,vd,ve] = do
              !a <- convert fa va
              !b <- convert fb vb
              !c <- convert fc vc
              !d <- convert fd vd
              !e <- convert fe ve
              return (a,b,c,d,e)
    convertResults fs vs  = convertError fs vs 5

instance (Result a, Result b, Result c, Result d, Result e, Result f) =>
    FromRow (a,b,c,d,e,f) where
    convertResults [fa,fb,fc,fd,fe,ff] [va,vb,vc,vd,ve,vf] = do
              !a <- convert fa va
              !b <- convert fb vb
              !c <- convert fc vc
              !d <- convert fd vd
              !e <- convert fe ve
              !f <- convert ff vf
              return (a,b,c,d,e,f)
    convertResults fs vs  = convertError fs vs 6

instance (Result a, Result b, Result c, Result d, Result e, Result f,
          Result g) =>
    FromRow (a,b,c,d,e,f,g) where
    convertResults [fa,fb,fc,fd,fe,ff,fg] [va,vb,vc,vd,ve,vf,vg] = do
              !a <- convert fa va
              !b <- convert fb vb
              !c <- convert fc vc
              !d <- convert fd vd
              !e <- convert fe ve
              !f <- convert ff vf
              !g <- convert fg vg
              return (a,b,c,d,e,f,g)
    convertResults fs vs  = convertError fs vs 7

instance (Result a, Result b, Result c, Result d, Result e, Result f,
          Result g, Result h) =>
    FromRow (a,b,c,d,e,f,g,h) where
    convertResults [fa,fb,fc,fd,fe,ff,fg,fh] [va,vb,vc,vd,ve,vf,vg,vh] = do
              !a <- convert fa va
              !b <- convert fb vb
              !c <- convert fc vc
              !d <- convert fd vd
              !e <- convert fe ve
              !f <- convert ff vf
              !g <- convert fg vg
              !h <- convert fh vh
              return (a,b,c,d,e,f,g,h)
    convertResults fs vs  = convertError fs vs 8

instance (Result a, Result b, Result c, Result d, Result e, Result f,
          Result g, Result h, Result i) =>
    FromRow (a,b,c,d,e,f,g,h,i) where
    convertResults [fa,fb,fc,fd,fe,ff,fg,fh,fi] [va,vb,vc,vd,ve,vf,vg,vh,vi] =
           do
              !a <- convert fa va
              !b <- convert fb vb
              !c <- convert fc vc
              !d <- convert fd vd
              !e <- convert fe ve
              !f <- convert ff vf
              !g <- convert fg vg
              !h <- convert fh vh
              !i <- convert fi vi
              return (a,b,c,d,e,f,g,h,i)
    convertResults fs vs  = convertError fs vs 9

instance (Result a, Result b, Result c, Result d, Result e, Result f,
          Result g, Result h, Result i, Result j) =>
    FromRow (a,b,c,d,e,f,g,h,i,j) where
    convertResults [fa,fb,fc,fd,fe,ff,fg,fh,fi,fj]
                   [va,vb,vc,vd,ve,vf,vg,vh,vi,vj] =
           do
              !a <- convert fa va
              !b <- convert fb vb
              !c <- convert fc vc
              !d <- convert fd vd
              !e <- convert fe ve
              !f <- convert ff vf
              !g <- convert fg vg
              !h <- convert fh vh
              !i <- convert fi vi
              !j <- convert fj vj
              return (a,b,c,d,e,f,g,h,i,j)
    convertResults fs vs  = convertError fs vs 10

f <$!> (!x) = f <$> x
infixl 4 <$!>

instance Result a => FromRow [a] where
    convertResults fs vs = foldr convert' (pure []) (zip fs vs)
      where convert' (f,v) as = (:) <$!> convert f v <*> as
    {-# INLINE convertResults #-}

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
             -> Either SomeException a
convertError fs vs n = Left . SomeException $ ConversionFailed
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
--           "pass"      -- discard the first column
--           name      <- "field"
--           lastname  <- "field"
--           "pass"      -- skip another column
--           age       <- "field"
--           return $ User name lastname age
-- @
newtype RowParser a 
  = RowParser { unRowParser :: (StateT RowParserState (Either SomeException) a) }
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
rowParserError = RowParser . lift . Left . SomeException


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
field :: Result a => RowParser a
field = do
  st <- get
  case st of
    ((f,v) : rest, cons) -> do
      res <- RowParser . lift $ convert f v
      put (rest, (f,v) : cons)
      return res
    _ -> notEnoughCols 


-------------------------------------------------------------------------------
-- | Parse the next field in line without consuming it
peekField :: (Result a) => RowParser a
peekField = do
  st <- get
  case st of
    ((f,v) : _, _) -> RowParser . lift $ convert f v
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
-- | Discard the next n fields in line
skip :: Int -> RowParser ()
skip n = modify $ \ (xs, ys) -> let xs' = take n xs
                                in (drop n xs, reverse xs' ++ ys)


-------------------------------------------------------------------------------
-- | Run a RowParser
runRowParser :: RowParser a -> [Field] -> [Maybe ByteString] -> Either SomeException a
runRowParser p fs vs = evalStateT (unRowParser p) (zip fs vs, [])
