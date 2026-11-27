-- |
-- Core Fx operations and execution
module Fx.Fx
  ( Fx (..),
    FxEnv (..),
    FxException (..),
    FxExceptionReason (..),
    ThreadPhase (..),
    ClaimState (..),
    initialThreadPhase,
    enterCleanup,
    exitCleanup,
    absorbClaim,

    -- * Execution
    runFxInIO,
    RunsFx (..),

    -- ** Environment handling
    mapEnv,

    -- ** Error handling
    throwErr,
    handleErr,
    mapErr,
    exposeErr,
    absorbErr,

    -- ** IO execution
    runTotalIO,
    runPartialIO,
    runExceptionalIO,
  )
where

import Fx.Prelude
import qualified Fx.Strings as Strings
import GHC.Stack (CallStack, HasCallStack, callStack, prettyCallStack)

-- * Helper types and instances

-- |
-- Abandonment claim state for a thread of the current run.
data ClaimState
  = Unclaimed
  | -- | The sweep claimed the thread and is sending a kill.
    Claimed
  | -- | The claimed kill was delivered.
    Delivered
  deriving stock (Eq, Show)

-- |
-- Lifecycle phase of a thread of the current run.
--
-- Cleanup nesting depth and abandonment claim state are tracked separately
-- so recording kill delivery never clobbers the active cleanup depth.
data ThreadPhase = ThreadPhase
  { cleanupDepth :: !Int,
    claimState :: !ClaimState
  }
  deriving stock (Eq, Show)

initialThreadPhase :: ThreadPhase
initialThreadPhase = ThreadPhase 0 Unclaimed

-- |
-- Runtime and application environment.
data FxEnv env
  = FxEnv
      -- | Masking policy for user IO in the current context.
      (forall a. IO a -> IO a)
      -- | Restore the ambient masking state, used to absorb a claimed kill.
      (forall a. IO a -> IO a)
      -- | Crash.
      (CallStack -> [ThreadId] -> FxExceptionReason -> IO ())
      -- | Threads forked by the current run with their lifecycle phase.
      -- 'abandonRun' claims a thread when its cleanup depth is zero before
      -- sending a kill, and records delivery afterward.
      (TVar [(ThreadId, ThreadPhase)])
      -- | User environment.
      env

-- |
-- Fatal failure of an `Fx` application.
data FxException = FxException [ThreadId] FxExceptionReason CallStack

-- |
-- Reason of a fatal failure of an `Fx` application.
data FxExceptionReason
  = UncaughtExceptionFxExceptionReason SomeException
  | AsyncFxExceptionReason SomeAsyncException
  | ErrorCallFxExceptionReason ErrorCall
  | BugFxExceptionReason String

instance Show FxException where
  show (FxException tids reason stack) =
    Strings.fatalErrorAtThreadPath tids (show reason) ++ "\n\nCallStack (from origin of error):\n" ++ prettyCallStack stack

instance Exception FxException

instance Show FxExceptionReason where
  show = \case
    UncaughtExceptionFxExceptionReason exc -> Strings.uncaughtException exc
    AsyncFxExceptionReason asyncExc -> Strings.asyncException asyncExc
    ErrorCallFxExceptionReason errorCall -> show errorCall
    BugFxExceptionReason details -> Strings.bug details

-- * Fx type and instances

-- |
-- Effectful computation with explicit errors in the context of provided environment.
--
-- Think of it as a combination of `ReaderT` and `ExceptT` over `IO`, with some extra features for concurrency and error handling.
newtype Fx env err res = Fx (ReaderT (FxEnv env) (ExceptT err IO) res)

deriving instance Functor (Fx env err)

deriving instance Applicative (Fx env err)

deriving instance (Monoid err) => Alternative (Fx env err)

deriving instance Monad (Fx env err)

deriving instance (Monoid err) => MonadPlus (Fx env err)

instance MonadFail (Fx env err) where
  fail msg = Fx $
    ReaderT $
      \(FxEnv _ _ crash _ _) -> liftIO $ do
        crash callStack [] (ErrorCallFxExceptionReason (ErrorCall msg))
        fail "Crashed"

instance MonadIO (Fx env SomeException) where
  liftIO io = Fx $ ReaderT $ \(FxEnv runIO _ _ _ _) -> ExceptT $
    (Right <$> runIO io) `catch` \(e :: SomeException) ->
      case fromException e of
        Just (SomeAsyncException _) -> throwIO e
        Nothing                     -> pure (Left e)

instance MonadError err (Fx env err) where
  throwError err = Fx (lift (throwE err))
  catchError fx handler = Fx $ ReaderT $ \fxEnv -> ExceptT $ do
    a <- runExceptT (let Fx m = fx in runReaderT m fxEnv)
    case a of
      Right res -> return (Right res)
      Left err -> case handler err of
        Fx m -> runExceptT (runReaderT m fxEnv)

instance Bifunctor (Fx env) where
  bimap lf rf = mapTransformers (mapReaderT (mapExceptT (fmap (bimap lf rf))))

mapTransformers ::
  ( ReaderT (FxEnv env1) (ExceptT err1 IO) res1 ->
    ReaderT (FxEnv env2) (ExceptT err2 IO) res2
  ) ->
  Fx env1 err1 res1 ->
  Fx env2 err2 res2
mapTransformers fn (Fx m) = Fx (fn m)

-- * IO Execution

-------------------------

-- |
-- Execute an effect with no environment and all errors handled.
--
-- Conventionally, this is what should be placed in the @main@ function.
runFxInIO :: (HasCallStack) => Fx () Void res -> IO res
runFxInIO (Fx m) = uninterruptibleMask $ \unmask -> do
  fatalErrChan <- newTQueueIO
  resVar <- newEmptyTMVarIO
  liveThreads <- newTVarIO []
  workerReady <- newEmptyMVar

  workerTid <- forkIO $ do
    takeMVar workerReady
    tid <- myThreadId

    finalize <-
      let crash stack tids reason = atomically (writeTQueue fatalErrChan (FxException (tid : tids) reason stack))
          fxEnv = FxEnv unmask unmask crash liveThreads ()
       in catch
            ( do
                resOrVoid <- runExceptT (runReaderT m fxEnv)
                return $ case resOrVoid of
                  Right res -> atomically (putTMVar resVar res)
                  Left a ->
                    catch
                      ( do
                          _ <- evaluate a
                          crash callStack [] (BugFxExceptionReason "Unexpected void")
                      )
                      (\errorCall -> crash callStack [] (ErrorCallFxExceptionReason errorCall))
            )
            ( \exc -> return $
                case fromException exc of
                  -- An asynchronous exception that reached the worker, e.g. a
                  -- kill during unmasked user IO. It is posted as a fatal failure
                  -- with its own reason rather than `Bug`. The channel post
                  -- (not a rethrow) is what wakes the waiter, so this stays a `crash`.
                  Just (asyncExc :: SomeAsyncException) -> crash callStack [] (AsyncFxExceptionReason asyncExc)
                  Nothing -> case fromException exc of
                    -- Catch calls to `error`.
                    Just (errorCall :: ErrorCall) -> crash callStack [] (ErrorCallFxExceptionReason errorCall)
                    -- Anything else is a defect we could miss.
                    Nothing -> crash callStack [] (BugFxExceptionReason (Strings.unexpectedException exc))
            )

    -- Throw errors or post the result
    finalize

  -- Registered by this thread under the entry mask before releasing the
  -- worker, so the worker cannot enter cleanup before registration and the
  -- sweep cannot miss it between fork and its first instruction.
  atomically (modifyTVar' liveThreads ((workerTid, initialThreadPhase) :))
  putMVar workerReady ()

  -- Wait for fatal error or result
  join $
    catch
      ( unmask $
          atomically $
            asum
              [ do
                  fatalErr <- readTQueue fatalErrChan
                  return $ throwIO fatalErr,
                do
                  res <- readTMVar resVar
                  return $ return res
              ]
      )
      ( \(exc :: SomeException) -> do
          -- The waiter was abandoned from outside or failed unexpectedly.
          -- Kill the worker's threads before leaving so none outlive the run.
          abandonRun liveThreads
          case fromException exc of
            Just (_ :: SomeAsyncException) ->
              throwIO exc
            Nothing ->
              throwIO (FxException [] (BugFxExceptionReason (Strings.failedWaitingForFinalResult exc)) callStack)
      )

-- |
-- Terminate the threads of an abandoned run.
--
-- A thread is killed only if the sweep can claim it while it is in its body.
-- The claim is committed in STM before the kill is sent, so a thread that
-- enters cleanup first is never killed. A thread that loses the claim to the
-- sweep waits for delivery and absorbs the kill before its release runs, so
-- no kill can land inside the release.
--
-- Threads are claimed oldest-first (worker before children) so the worker
-- stops spawning new work before existing children are killed. Each kill
-- blocks until delivered, so a thread stuck in an uninterruptible foreign
-- call or mask blocks both abandonment return and the remaining kills.
abandonRun :: TVar [(ThreadId, ThreadPhase)] -> IO ()
abandonRun liveThreads = uninterruptibleMask_ $ do
  tids <- reverse . map fst <$> atomically (readTVar liveThreads)
  forM_ tids \tid -> do
    claimed <- atomically $ do
      entries <- readTVar liveThreads
      case lookup tid entries of
        Just (ThreadPhase 0 Unclaimed) -> do
          writeTVar liveThreads (updatePhase tid (\p -> p {claimState = Claimed}) entries)
          pure True
        _ -> pure False
    when claimed $ do
      killThread tid `catch` \(_ :: SomeException) -> pure ()
      atomically (modifyTVar' liveThreads (updatePhase tid (\p -> p {claimState = Delivered})))

updatePhase :: ThreadId -> (ThreadPhase -> ThreadPhase) -> [(ThreadId, ThreadPhase)] -> [(ThreadId, ThreadPhase)]
updatePhase tid f = go
  where
    go [] = []
    go (entry@(t, p) : xs)
      | t == tid = let !p' = f p in (t, p') : xs
      | otherwise = let !xs' = go xs in entry : xs'

-- |
-- Enter the cleanup phase for the current thread.
--
-- Returns 'True' when an abandon claim was already committed for this
-- thread at cleanup entry. The caller must then absorb the kill before
-- running its release.
enterCleanup :: TVar [(ThreadId, ThreadPhase)] -> IO Bool
enterCleanup liveThreads = do
  tid <- myThreadId
  atomically do
    entries <- readTVar liveThreads
    case lookup tid entries of
      Just (ThreadPhase depth claim) -> do
        writeTVar liveThreads (updatePhase tid (\p -> p {cleanupDepth = depth + 1}) entries)
        pure (depth == 0 && claim == Claimed)
      Nothing -> pure False

-- |
-- Leave one level of the cleanup phase.
exitCleanup :: TVar [(ThreadId, ThreadPhase)] -> IO ()
exitCleanup liveThreads = do
  tid <- myThreadId
  atomically (modifyTVar' liveThreads (updatePhase tid (\p -> p {cleanupDepth = max 0 (cleanupDepth p - 1)})))

-- |
-- Wait for the claimed kill and swallow it, so the following release runs
-- without an asynchronous exception pending.
--
-- The wait runs unmasked: the kill is delivered by the sweep's 'killThread',
-- which blocks until delivery, and the sweep then records 'Delivered'.
absorbClaim :: (forall a. IO a -> IO a) -> TVar [(ThreadId, ThreadPhase)] -> IO ()
absorbClaim unmask liveThreads = do
  tid <- myThreadId
  catch
    ( unmask $
        atomically do
          entries <- readTVar liveThreads
          case lookup tid entries of
            Just (ThreadPhase _ Delivered) -> pure ()
            Nothing -> pure ()
            _ -> retry
    )
    ( \(exc :: SomeException) ->
        case fromException exc of
          Just ThreadKilled -> pure ()
          Nothing -> throwIO exc
    )

-- |
-- Turn a non-failing IO action into an effect.
--
-- Asynchronous exceptions are rethrown rather than turned into a fatal
-- failure, so cancellation stays effective.
--
-- __Warning:__
-- It is your responsibility to ensure that it does not throw synchronous exceptions!
runTotalIO :: (env -> IO res) -> Fx env err res
runTotalIO io = Fx $
  ReaderT $
    \(FxEnv runIO _ crash _ env) ->
      lift $
        catch
          (runIO (io env))
          ( \(exc :: SomeException) -> do
              case fromException exc of
                Just (asyncExc :: SomeAsyncException) -> throwIO asyncExc
                Nothing -> do
                  crash callStack [] (UncaughtExceptionFxExceptionReason exc)
                  fail "Unhandled exception in runTotalIO. Got propagated to top."
          )

-- |
-- Run IO which produces either an error or result.
--
-- __Warning:__
-- It is your responsibility to ensure that it does not throw exceptions!
runPartialIO :: (env -> IO (Either err res)) -> Fx env err res
runPartialIO io = runTotalIO io >>= either throwErr return

-- |
-- Run IO which only throws a specific type of exception.
--
-- Asynchronous exceptions are rethrown rather than turned into a fatal
-- failure, so cancellation stays effective.
--
-- __Warning:__
-- It is your responsibility to ensure that it doesn't throw any other exceptions!
runExceptionalIO :: (Exception exc) => (env -> IO res) -> Fx env exc res
runExceptionalIO io =
  Fx $
    ReaderT $
      \(FxEnv runIO _ crash _ env) -> ExceptT $
        catch (fmap Right (runIO (io env))) $
          \exc -> case fromException exc of
            Just exc' -> return (Left exc')
            Nothing -> case fromException exc of
              Just (asyncExc :: SomeAsyncException) -> throwIO asyncExc
              Nothing -> do
                crash callStack [] (UncaughtExceptionFxExceptionReason exc)
                fail "Unhandled exception in runExceptionalIO. Got propagated to top."

-- |
-- Map the environment.
-- Please notice that the expected function is contravariant.
mapEnv :: (b -> a) -> Fx a err res -> Fx b err res
mapEnv fn (Fx m) =
  Fx $
    ReaderT $
      \(FxEnv runIO unmask crash live env) ->
        runReaderT m (FxEnv runIO unmask crash live (fn env))

-- * Classes

-------------------------

-- ** Fx Running

-------------------------

-- |
-- Support for running of `Fx`.
--
-- Apart from other things this is your interface to turn `Fx` into `IO`.
class RunsFx env err m | m -> env, m -> err where
  runFx :: Fx env err res -> m res

-- |
-- Executes an effect with no environment and all errors handled.
instance RunsFx () Void IO where
  runFx = runFxInIO

instance RunsFx () err (ExceptT err IO) where
  runFx fx = ExceptT (runFx (exposeErr fx))

instance RunsFx env err (ReaderT env (ExceptT err IO)) where
  runFx fx = ReaderT (\env -> ExceptT (runFx (mapEnv (const env) (exposeErr fx))))

instance RunsFx env err (Fx env err) where
  runFx = id

-- ** Error Handling

-------------------------

-- |
-- Interrupt the current computation raising an error.
--
-- Same as `throwError` of `MonadError`.
throwErr :: err -> Fx env err res
throwErr = Fx . lift . throwE

-- |
-- Handle error in another failing action.
-- Sort of like a bind operation over the error type parameter.
--
-- Similar to `handleError` of `MonadError` but allows changing the error type.
handleErr :: (a -> Fx env b res) -> Fx env a res -> Fx env b res
handleErr handler = mapTransformers $ \m -> ReaderT $ \unmask -> ExceptT $ do
  a <- runExceptT (runReaderT m unmask)
  case a of
    Right res -> return (Right res)
    Left err -> case handler err of
      Fx m -> runExceptT (runReaderT m unmask)

-- |
-- Map the error.
mapErr :: (a -> b) -> Fx env a res -> Fx env b res
mapErr mapper = handleErr (throwErr . mapper)

-- |
-- Expose the error in result,
-- producing an action, which is compatible with any error type.
--
-- Almost the same as `tryError` of `MonadError`, but allows changing the error type.
exposeErr :: Fx env a res -> Fx env b (Either a res)
exposeErr = absorbErr Left . fmap Right

-- |
-- Map from error to result, leaving the error be anything.
absorbErr :: (a -> res) -> Fx env a res -> Fx env b res
absorbErr fn = handleErr (pure . fn)
