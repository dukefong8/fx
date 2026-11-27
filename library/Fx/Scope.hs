-- |
-- Scope abstraction for resource management
module Fx.Scope
  ( -- * Scope
    Scope (..),
    acquire,
    releasing,
    registerRelease,
    scoping,
  )
where

import Fx.Fx
import Fx.Prelude

-- |
-- Instructions of how to acquire and release a resource of type `env` which may fail with `err`.
newtype Scope err env = Scope (Fx () err (env, Fx () err ()))

instance Functor (Scope err) where
  fmap f (Scope m) = Scope $ do
    (env, release) <- m
    return (f env, release)

instance Applicative (Scope err) where
  pure env = Scope (pure (env, pure ()))
  -- Composition is sequential with LIFO release, which is exactly
  -- what the 'Monad' instance expresses, so reuse it instead of
  -- duplicating the rollback logic.
  (<*>) = ap

instance Monad (Scope err) where
  return = pure
  Scope (Fx m1) >>= f =
    Scope $ Fx $ ReaderT $ \(FxEnv unmask crash live ()) -> ExceptT $ do
      r1 <- runExceptT (runReaderT m1 (FxEnv unmask crash live ()))
      case r1 of
        Left e -> pure (Left e)
        -- Teardown dispatch is pinned so it does not depend on the ambient mask.
        Right (env, Fx rel1) -> uninterruptibleMask_ $ do
          let runRel1 =
                withCleanupPhase unmask live $
                  void (runExceptT (runReaderT rel1 (FxEnv unmask crash live ())))
          r2 <- (do
            Scope (Fx m2) <- evaluate (f env)
            runExceptT (runReaderT m2 (FxEnv unmask crash live ()))
            ) `onException` runRel1
          case r2 of
            Left e2 -> runRel1 >> pure (Left e2)
            Right (env2, Fx rel2) -> pure (Right (env2, andReleases (Fx rel2) (Fx rel1)))

-- |
-- Run two releases, attempting the second even if the first fails.
-- The first error wins. Thrown exceptions follow 'finally' semantics:
-- the second still runs, and a throw from the second replaces the first.
andReleases :: Fx () err () -> Fx () err () -> Fx () err ()
andReleases (Fx later) (Fx earlier) =
  Fx $ ReaderT $ \(FxEnv unmask crash live ()) -> ExceptT $
    uninterruptibleMask_ $ do
      firstResult <-
        runExceptT (runReaderT later (FxEnv unmask crash live ()))
          `onException` void (runExceptT (runReaderT earlier (FxEnv unmask crash live ())))
      case firstResult of
        Left firstError -> do
          _ <- runExceptT (runReaderT earlier (FxEnv unmask crash live ()))
          pure (Left firstError)
        Right () ->
          runExceptT (runReaderT earlier (FxEnv unmask crash live ()))

instance Bifunctor Scope where
  bimap lf rf (Scope m) = Scope (bimap lf (bimap rf (first lf)) m)
  second = fmap

instance MonadError err (Scope err) where
  throwError err = Scope (throwError err)
  catchError (Scope m) handler =
    Scope $
      catchError m $ \err ->
        let Scope m2 = handler err
         in m2

instance MonadIO (Scope SomeException) where
  liftIO io = Scope (liftIO io >>= \env -> return (env, pure ()))

-- |
-- Create a scope from an acquiring effect.
--
-- To add a release action, use 'registerRelease'.
--
-- Example:
--
-- > fileInWriteMode :: FilePath -> Scope IOError Handle
-- > fileInWriteMode path = do
-- >   handle <- acquire (openFile path WriteMode)
-- >   registerRelease (hClose handle)
-- >   return handle
acquire :: Fx () err env -> Scope err env
acquire acquireFx = Scope do
  env <- acquireFx
  return (env, pure ())

-- |
-- Add a release action to an existing resource provider.
--
-- Example:
--
-- > fileInWriteMode :: FilePath -> Scope IOError Handle
-- > fileInWriteMode path =
-- >   releasing hClose (acquire (openFile path WriteMode))
releasing :: Fx env err () -> Scope err env -> Scope err env
releasing release (Scope m) = Scope $ do
  (env, existingRelease) <- m
  return (env, andReleases existingRelease (closeEnv env release))

-- |
-- Schedule an action to be run after the scope is exited.
registerRelease :: Fx () err () -> Scope err ()
registerRelease release =
  Scope $ pure ((), release)

-- |
-- Execute Fx in the scope of a provided environment.
scoping :: Scope err env -> Fx env err res -> Fx env' err res
scoping (Scope (Fx acquire)) (Fx fx) =
  Fx $
    ReaderT $
      \(FxEnv unmask crash live _) -> ExceptT $ uninterruptibleMask_ $ do
        let providerFxEnv = FxEnv unmask crash live ()
        acquisition <- runExceptT (runReaderT acquire providerFxEnv)
        case acquisition of
          Left err -> return (Left err)
          -- Everything from acquisition on stays inside 'uninterruptibleMask_'
          -- so a cancel cannot skip cleanup between acquisition, the body and
          -- the release. Release leaves still unmask via 'unmask' by design.
          Right (env, (Fx release)) -> do
            let runRelease = runExceptT (runReaderT release providerFxEnv)
            resOrErr <- runExceptT (runReaderT fx (FxEnv unmask crash live env)) `onException` withCleanupPhase unmask live runRelease
            releasing <- withCleanupPhase unmask live runRelease
            return (resOrErr <* releasing)

-- Run an action while the current thread is in its cleanup phase.
--
-- If an abandon claim was already committed for this thread, the kill is
-- absorbed before the action, so the action runs to completion. The kill is
-- rethrown after the action, so cancellation still ends the thread. The
-- nesting depth is tracked, so an inner cleanup does not re-arm the thread
-- while an outer release is still running.
withCleanupPhase :: (forall a. IO a -> IO a) -> TVar [(ThreadId, ThreadPhase)] -> IO a -> IO a
withCleanupPhase unmask liveThreads action = do
  claimed <- enterCleanup liveThreads
  let runAction = action `finally` exitCleanup liveThreads
  if claimed
    then do
      deferred <- absorbClaimDeferring unmask liveThreads Nothing
      _ <- runAction
      case deferred of
        Just exc -> throwIO exc
        Nothing -> throwIO ThreadKilled
    else runAction

-- |
-- Absorb the claimed kill, deferring other asynchronous exceptions.
--
-- Another asynchronous exception can already be queued around the claimed
-- kill. Catching one and running the release immediately would leave the
-- others pending, so one could fire inside the release. Keep waiting until
-- the claimed kill is delivered, then drain the rest, then let the caller
-- run the release and rethrow the first deferred exception afterwards.
absorbClaimDeferring :: (forall a. IO a -> IO a) -> TVar [(ThreadId, ThreadPhase)] -> Maybe SomeException -> IO (Maybe SomeException)
absorbClaimDeferring unmask liveThreads deferred = do
  outcome <- try (absorbClaim unmask liveThreads) :: IO (Either SomeException ())
  case outcome of
    Right () -> drainPending unmask deferred
    Left exc -> absorbClaimDeferring unmask liveThreads (deferred <|> Just exc)

-- |
-- Deliver and defer asynchronous exceptions that are already queued behind
-- the claimed kill.
--
-- 'unmask (pure ())' is 'allowInterrupt': it lets the next pending
-- asynchronous exception in, and returns immediately when there is none.
drainPending :: (forall a. IO a -> IO a) -> Maybe SomeException -> IO (Maybe SomeException)
drainPending unmask deferred = do
  outcome <- try (unmask (pure ())) :: IO (Either SomeException ())
  case outcome of
    Right () -> pure deferred
    Left exc ->
      case fromException exc of
        Just ThreadKilled -> drainPending unmask deferred
        Nothing -> drainPending unmask (deferred <|> Just exc)

closeEnv :: env -> Fx env err res -> Fx env' err res
closeEnv env (Fx fx) =
  Fx $
    ReaderT $
      \(FxEnv unmask crash live _) ->
        ExceptT $
          runExceptT (runReaderT fx (FxEnv unmask crash live env))

instance RunsFx () err (Scope err) where
  runFx fx = Scope (fmap (\env -> (env, pure ())) fx)
