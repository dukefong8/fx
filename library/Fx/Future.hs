-- |
-- Future abstraction for concurrent computations
module Fx.Future
  ( Future,
    async,
    await,
    cancel,
  )
where

import Fx.Fx
import Fx.Prelude
import qualified Fx.Strings as Strings
import GHC.Stack (callStack)

-- |
-- Handle to a result of an action which may still be being executed on another thread.
data Future err res
  = Future
      (Maybe ThreadId)
      (Compose STM (Either (Maybe err)) res)

instance Functor (Future err) where
  fmap f (Future tid result) = Future tid (fmap f result)

instance Applicative (Future err) where
  pure a = Future Nothing (pure a)
  Future tid1 resultF <*> Future tid2 resultA =
    Future (tid1 <|> tid2) (resultF <*> resultA)

instance Alternative (Future err) where
  empty = Future Nothing empty
  Future tid1 result1 <|> Future tid2 result2 =
    -- When one completes, cancel the other
    Future (tid1 <|> tid2) (result1 <|> result2)

instance Bifunctor Future where
  bimap lf rf = mapImpl (mapCompose (fmap (bimap (fmap lf) rf)))

mapImpl ::
  ( Compose STM (Either (Maybe err1)) res1 ->
    Compose STM (Either (Maybe err2)) res2
  ) ->
  Future err1 res1 ->
  Future err2 res2
mapImpl fn (Future tid m) = Future tid (fn m)

-- |
-- Spawn a thread and start running an effect on it,
-- returning the associated future.
--
-- Fatal errors on the spawned thread are guaranteed to get propagated to the top.
-- By fatal errors we mean calls to `error`, `fail` and uncaught exceptions.
--
-- Normal errors (the explicit @err@ parameter) will only propagate
-- if you use `await` at some point.
--
-- __Warning:__
-- It is your responsibility to ensure that the whole future executes
-- before the running `Fx` finishes.
-- Otherwise you will lose the environment in scope of which the future executes.
-- To achieve that use `await`.
async :: Fx env err res -> Fx env err' (Future err res)
async (Fx m) =
  Fx $ ReaderT $ \(FxEnv unmask crash live env) -> lift $ do
    futureVar <- newEmptyTMVarIO

    let handler = \case
          ThreadKilled -> do
            self <- myThreadId
            atomically (modifyTVar' live (filter ((/= self) . fst)))
            void $ atomically (tryPutTMVar futureVar (Left Nothing))
          exc -> throwIO exc
    tid <- forkIO $ handle handler do
      tid <- myThreadId

      let childCrash stack tids dls = crash stack (tid : tids) dls
          unregister = atomically (modifyTVar' live (filter ((/= tid) . fst)))

          -- Only `ThreadKilled` goes to the outer handler, which ends
          -- the future empty without a fatal failure. Any other
          -- asynchronous exception is posted as a fatal failure here,
          -- so an awaiter sees it instead of an uncaught exception
          -- leaving the future unfilled.
          handle exc
            | Just ThreadKilled <- fromException exc =
                throwIO exc
            | Just (asyncExc :: SomeAsyncException) <- fromException exc =
                childCrash callStack [] (AsyncFxExceptionReason asyncExc)
            | Just errorCall <- fromException exc =
                childCrash callStack [] (ErrorCallFxExceptionReason errorCall)
            | otherwise =
                childCrash callStack [] (BugFxExceptionReason (Strings.unexpectedException exc))

      -- Unregistered on the way out of either outcome. The parent registers
      -- the child before this runs, which narrows but does not close the
      -- fork-to-register window: a concurrent abandon snapshot can land
      -- between them and miss the child. Best-effort only.
      finalize <-
        catch
          ( do
              res <- runExceptT (runReaderT m (FxEnv unmask childCrash live env))
              return $ do
                unregister
                atomically (putTMVar futureVar (first Just res))
          )
          ( \exc -> return $ do
              unregister
              handle exc
              atomically (putTMVar futureVar (Left Nothing))
          )

      finalize

    -- Registered by the spawning thread while masked. The mask only orders
    -- this thread's own fork-then-register steps; a concurrent abandon
    -- snapshot from the waiter can still land between them and miss the
    -- child. Best-effort only.
    atomically (modifyTVar' live ((tid, BodyPhase) :))

    return $ Future (Just tid) $ Compose $ readTMVar futureVar

-- |
-- Block until the future completes either with a result or an error.
await :: Future err res -> Fx env err res
await (Future _ m) =
  Fx $ ReaderT $ \(FxEnv unmask crash _ _) ->
    ExceptT $
      join $
        catch
          ( do
              futureStatus <- unmask (atomically (getCompose m))
              return $ case futureStatus of
                Right res -> return (Right res)
                Left (Just err) -> return (Left err)
                Left Nothing -> fail "Waiting for a future that crashed"
          )
          ( \(exc :: SomeException) -> return $ do
              crash callStack [] (BugFxExceptionReason (Strings.failedWaitingForResult exc))
              fail "Thread crashed with uncaught exception waiting for result."
          )

cancel :: Future err res -> Fx env err' ()
cancel (Future Nothing _) = return ()
cancel (Future (Just tid) _) = runTotalIO (\_ -> killThread tid `catch` \(_ :: SomeException) -> return ())
