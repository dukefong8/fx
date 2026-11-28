module Main where

import Control.Monad (forever)
import Control.Monad.Parallel (bindM2)
import Fx
import Test.Hspec
import Prelude hiding (choose, IOError)

main :: IO ()
main = hspec do
  it "Error call on main thread" do
    res <- try $ runFx $ error "A"
    case res of
      Left (FxException _ (ErrorCallFxExceptionReason _) _) -> return ()
      Left exc -> expectationFailure (show exc)
      _ -> expectationFailure "Right"

  it "Fail" do
    testException
      (fail "A")
      ( \case
          FxException _ (ErrorCallFxExceptionReason _) _ -> True
          _ -> False
      )

  it "Racing completes with first result" do
    -- Test that racing returns the first result
    let slowAction = do
          -- Wrap in exception handler to catch ThreadKilled
          runTotalIO $ \_ ->
            catch
              (threadDelay 1000000)
              ( \case
                  ThreadKilled -> return ()
                  exc -> throwIO exc
              )
          return (1 :: Int)

    let fastAction = do
          return (2 :: Int)

    -- Race them using concurrently
    result <- runFx $ concurrently $ \lift ->
      lift slowAction <|> lift fastAction

    -- The fast action should win
    result `shouldBe` 2

  it "Racing kills the losing thread" do
    -- Test that the losing thread is actually killed
    completedRef <- newIORef False
    killedRef <- newIORef False

    let slowAction = do
          runTotalIO $ \_ -> do
            catch
              (threadDelay 5000000 >> writeIORef completedRef True)
              ( \case
                  ThreadKilled -> writeIORef killedRef True
                  _ -> pure ()
              )
          return (1 :: Int)

    let fastAction = do
          return (2 :: Int)

    -- Race them using concurrently
    result <- runFx $ concurrently $ \lift ->
      lift slowAction <|> lift fastAction

    -- The fast action should win
    result `shouldBe` 2

    -- Give a moment for the kill signal to be processed
    threadDelay 100000

    -- The slow action should NOT have completed normally
    completed <- readIORef completedRef
    completed `shouldBe` False

    -- The slow action should have been killed
    killed <- readIORef killedRef
    killed `shouldBe` True

  it "Scope releases on body typed error" do
    releasedRef <- newIORef False
    let scope :: Scope String ()
        scope = do
          _ <- acquire (pure () :: Fx () String ())
          _ <- registerRelease (runTotalIO (\_ -> writeIORef releasedRef True) :: Fx () String ())
          pure ()
        body :: Fx () String ()
        body = throwErr "boom"
    res <- runFx (exposeErr (scoping scope body) :: Fx () Void (Either String ()))
    res `shouldBe` Left "boom"
    released <- readIORef releasedRef
    released `shouldBe` True

  it "Scope releases on body exception" do
    releasedRef <- newIORef False
    let scope :: Scope Void ()
        scope = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease (runTotalIO (\_ -> writeIORef releasedRef True) :: Fx () Void ())
          pure ()
        body :: Fx () Void ()
        body = runTotalIO (\_ -> error "boom")
    res <- try (runFx (scoping scope body)) :: IO (Either FxException ())
    case res of
      Left _ -> return ()
      Right _ -> expectationFailure "No exception"
    released <- readIORef releasedRef
    released `shouldBe` True

  it "Scope releases exactly once on body kill" do
    countRef <- newIORef (0 :: Int)
    let scope :: Scope Void ()
        scope = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease (runTotalIO (\_ -> modifyIORef' countRef (+ 1)) :: Fx () Void ())
          pure ()
        body :: Fx () Void ()
        body = runTotalIO $ \_ -> do
          self <- myThreadId
          _ <- forkIO (threadDelay 50000 >> killThread self)
          threadDelay 5000000
    res <- try (runFx (scoping scope body)) :: IO (Either FxException ())
    case res of
      Left _ -> return ()
      Right _ -> expectationFailure "No exception"
    threadDelay 100000
    n <- readIORef countRef
    n `shouldBe` 1

  it "Chained Scopes release the earlier resource on later Left" do
    releasedRef <- newIORef False
    let first :: Scope String Int
        first = do
          a <- acquire (pure 1 :: Fx () String Int)
          _ <- registerRelease (runTotalIO (\_ -> writeIORef releasedRef True) :: Fx () String ())
          pure a
        failing :: Scope String Int
        failing = acquire (throwErr "boom" :: Fx () String Int)
        chained :: Scope String (Int, Int)
        chained = do
          a <- first
          b <- failing
          pure (a, b)
    res <- runFx (exposeErr (scoping chained (pure ())) :: Fx () Void (Either String ()))
    res `shouldBe` Left "boom"
    released <- readIORef releasedRef
    released `shouldBe` True

  it "Chained Scopes release the earlier resource on later throw" do
    releasedRef <- newIORef False
    let first :: Scope Void Int
        first = do
          a <- acquire (pure 1 :: Fx () Void Int)
          _ <- registerRelease (runTotalIO (\_ -> writeIORef releasedRef True) :: Fx () Void ())
          pure a
        throwing :: Scope Void Int
        throwing = acquire (runTotalIO (\_ -> error "boom") :: Fx () Void Int)
        chained :: Scope Void (Int, Int)
        chained = do
          a <- first
          b <- throwing
          pure (a, b)
    res <- try (runFx (scoping chained (pure ()) :: Fx () Void ())) :: IO (Either FxException ())
    case res of
      Left _ -> return ()
      Right _ -> expectationFailure "No exception"
    released <- readIORef releasedRef
    released `shouldBe` True

  it "Applicative Scopes release the earlier resource on partial failure" do
    releasedRef <- newIORef False
    let first :: Scope String Int
        first = do
          a <- acquire (pure 1 :: Fx () String Int)
          _ <- registerRelease (runTotalIO (\_ -> writeIORef releasedRef True) :: Fx () String ())
          pure a
        failing :: Scope String Int
        failing = acquire (throwErr "boom" :: Fx () String Int)
        chained :: Scope String (Int, Int)
        chained = (,) <$> first <*> failing
    res <- runFx (exposeErr (scoping chained (pure ())) :: Fx () Void (Either String ()))
    res `shouldBe` Left "boom"
    released <- readIORef releasedRef
    released `shouldBe` True

  it "All releases run when the first fails, keeping the first error" do
    ranARef <- newIORef False
    ranBRef <- newIORef False
    let scope :: Scope String ()
        scope = do
          _ <- acquire (pure () :: Fx () String ())
          _ <- registerRelease (do
            runTotalIO (\_ -> writeIORef ranARef True) :: Fx () String ()
            throwErr "errA")
          _ <- registerRelease (do
            runTotalIO (\_ -> writeIORef ranBRef True) :: Fx () String ()
            throwErr "errB")
          pure ()
    res <- runFx (exposeErr (scoping scope (pure ())) :: Fx () Void (Either String ()))
    res `shouldBe` Left "errB"
    ranA <- readIORef ranARef
    ranB <- readIORef ranBRef
    ranA `shouldBe` True
    ranB `shouldBe` True

  it "All releases run when the first throws" do
    ranRef <- newIORef False
    let scope :: Scope Void ()
        scope = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease (runTotalIO (\_ -> writeIORef ranRef True) :: Fx () Void ())
          _ <- registerRelease (runTotalIO (\_ -> error "boom") :: Fx () Void ())
          pure ()
    res <- try (runFx (scoping scope (pure ()) :: Fx () Void ())) :: IO (Either FxException ())
    case res of
      Left _ -> return ()
      Right _ -> expectationFailure "No exception"
    ran <- readIORef ranRef
    ran `shouldBe` True

  it "liftIO does not swallow an async kill" do
    let action :: Fx () SomeException ()
        action = liftIO $ do
          self <- myThreadId
          _ <- forkIO (threadDelay 50000 >> killThread self)
          threadDelay 5000000
    res <- try (runFx (absorbErr (const ()) action)) :: IO (Either FxException ())
    case res of
      Left (FxException _ (AsyncFxExceptionReason _) _) -> return ()
      Left exc -> expectationFailure ("Wrong reason: " ++ show exc)
      Right _ -> expectationFailure "Kill was swallowed as a value"

  it "runExceptionalIO with SomeException rethrows an async kill" do
    let action :: Fx () SomeException ()
        action = runExceptionalIO $ \() -> do
          self <- myThreadId
          _ <- forkIO (threadDelay 50000 >> killThread self)
          threadDelay 5000000
    res <- try (runFx (absorbErr (const ()) action)) :: IO (Either FxException ())
    case res of
      Left (FxException _ (AsyncFxExceptionReason _) _) -> return ()
      Left exc -> expectationFailure ("Wrong reason: " ++ show exc)
      Right _ -> expectationFailure "Kill was swallowed as a value"

  it "Abandoned run cleans up and leaves no running threads" do
    releasedRef <- newIORef False
    ticksRef <- newIORef (0 :: Int)
    startedVar <- newEmptyMVar
    let scope :: Scope Void ()
        scope = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease (runTotalIO (\_ -> writeIORef releasedRef True) :: Fx () Void ())
          pure ()
        spinningChild =
          runTotalIO $ \_ -> forever (threadDelay 10000 >> modifyIORef' ticksRef (+ 1))
        blockingBody = do
          runTotalIO $ \_ -> putMVar startedVar ()
          runTotalIO (\_ -> threadDelay 10000000)
        body :: Fx () Void ()
        body = void $ bindM2 (\_ _ -> pure ()) spinningChild blockingBody
    runner <- forkIO (runFx (scoping scope body) >> pure ())
    takeMVar startedVar
    killThread runner
    -- Cleanup runs on the abandoned run.
    releaseSeen <- pollUntil 2000000 (readIORef releasedRef)
    releaseSeen `shouldBe` True
    -- No thread keeps working after the run was abandoned.
    firstSample <- readIORef ticksRef
    threadDelay 300000
    secondSample <- readIORef ticksRef
    firstSample `shouldBe` secondSample

  it "Abandoned run lets an in-flight release finish" do
    startedRef <- newIORef False
    finishedRef <- newIORef False
    let scope :: Scope Void ()
        scope = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease ((runTotalIO $ \_ -> do
            writeIORef startedRef True
            threadDelay 300000
            writeIORef finishedRef True) :: Fx () Void ())
          pure ()
        body :: Fx () Void ()
        body = pure ()
    runner <- forkIO (runFx (scoping scope body) >> pure ())
    started <- pollUntil 2000000 (readIORef startedRef)
    started `shouldBe` True
    killThread runner
    finished <- pollUntil 2000000 (readIORef finishedRef)
    finished `shouldBe` True

  it "Abandoned run lets an acquisition-rollback release finish" do
    startedRef <- newIORef False
    finishedRef <- newIORef False
    let first :: Scope String Int
        first = do
          a <- acquire (pure 1 :: Fx () String Int)
          _ <- registerRelease ((runTotalIO $ \_ -> do
            writeIORef startedRef True
            threadDelay 300000
            writeIORef finishedRef True) :: Fx () String ())
          pure a
        failing :: Scope String Int
        failing = acquire (throwErr "boom" :: Fx () String Int)
        chained :: Scope String (Int, Int)
        chained = do
          a <- first
          b <- failing
          pure (a, b)
        body :: Fx (Int, Int) String ()
        body = pure ()
    runner <- forkIO (runFx (exposeErr (scoping chained body) :: Fx () Void (Either String ())) >> pure ())
    started <- pollUntil 2000000 (readIORef startedRef)
    started `shouldBe` True
    killThread runner
    finished <- pollUntil 2000000 (readIORef finishedRef)
    finished `shouldBe` True

  it "Abandoned run absorbs a kill pending at cleanup entry" do
    startedVar <- newEmptyMVar
    gateVar <- newEmptyMVar
    releasedRef <- newIORef False
    let scope :: Scope Void ()
        scope = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease (runTotalIO (\_ -> writeIORef releasedRef True) :: Fx () Void ())
          pure ()
        body :: Fx () Void ()
        body =
          runTotalIO $ \_ ->
            uninterruptibleMask_ do
              putMVar startedVar ()
              takeMVar gateVar
    runner <- forkIO (runFx (scoping scope body) >> pure ())
    takeMVar startedVar
    killThread runner
    -- Let the sweep claim the worker and send the kill while it is masked,
    -- so the kill is pending when the release starts.
    threadDelay 50000
    putMVar gateVar ()
    released <- pollUntil 2000000 (readIORef releasedRef)
    released `shouldBe` True

  it "Abandoned run absorbs a queued async exception ahead of the claimed kill" do
    startedVar <- newEmptyMVar
    helperGo <- newEmptyMVar
    gateVar <- newEmptyMVar
    finishedRef <- newIORef False
    workerTidRef <- newIORef Nothing
    let scope :: Scope Void ()
        scope = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease ((runTotalIO $ \_ -> do
            threadDelay 100000
            writeIORef finishedRef True) :: Fx () Void ())
          pure ()
        body :: Fx () Void ()
        body = do
          runTotalIO $ \_ -> do
            tid <- myThreadId
            writeIORef workerTidRef (Just tid)
            putMVar startedVar ()
          -- Hold the body in the masking state it inherited from 'scoping',
          -- so both queued exceptions stay pending until cleanup.
          maskedIO_ (takeMVar gateVar)
    runner <- forkIO (runFx (scoping scope body) >> pure ())
    takeMVar startedVar
    Just workerTid <- readIORef workerTidRef
    -- 'throwTo' blocks until delivery, so queue the first exception from a
    -- helper thread and let it park while the worker is masked.
    _ <- forkIO $ do
      putMVar helperGo ()
      throwTo workerTid (ReviewAsyncException "queued-ahead")
    takeMVar helperGo
    threadDelay 200000
    killThread runner
    -- Let the sweep claim the worker and queue its kill behind the first
    -- asynchronous exception.
    threadDelay 50000
    putMVar gateVar ()
    finished <- pollUntil 2000000 (readIORef finishedRef)
    finished `shouldBe` True

  it "An async exception during a release leaves the release running" do
    startedRef <- newIORef False
    finishedRef <- newIORef False
    workerTidRef <- newIORef Nothing
    spinCountRef <- newIORef (20000000 :: Int)
    helperGo <- newEmptyMVar
    let spin :: Int -> Integer
        spin n = go n 0
          where
            go 0 acc = acc
            go k acc = go (k - 1) ((acc * 31 + fromIntegral (k `mod` 7)) `mod` 1000003)
        scope :: Scope Void ()
        scope = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease ((runTotalIO $ \_ -> do
            tid <- myThreadId
            writeIORef workerTidRef (Just tid)
            writeIORef startedRef True
            n <- readIORef spinCountRef
            spin n `seq` pure ()
            writeIORef finishedRef True) :: Fx () Void ())
          pure ()
        body :: Fx () Void ()
        body = pure ()
    forkIO (runFx (scoping scope body) >> pure ()) >> pure ()
    started <- pollUntil 2000000 (readIORef startedRef)
    started `shouldBe` True
    Just workerTid <- readIORef workerTidRef
    _ <- forkIO $ do
      putMVar helperGo ()
      throwTo workerTid (ReviewAsyncException "during-release")
    takeMVar helperGo
    finished <- pollUntil 2000000 (readIORef finishedRef)
    finished `shouldBe` True

  it "Abandoned run keeps a nested release protected" do
    tailStartedVar <- newEmptyMVar
    tailGateVar <- newEmptyMVar
    finishedRef <- newIORef False
    let inner :: Scope Void ()
        inner = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease (pure () :: Fx () Void ())
          pure ()
        outer :: Scope Void ()
        outer = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease
            ( (scoping inner (pure ()))
                >> ( runTotalIO $ \_ -> do
                       putMVar tailStartedVar ()
                       takeMVar tailGateVar
                       writeIORef finishedRef True
                   )
            )
          pure ()
        body :: Fx () Void ()
        body = pure ()
    runner <- forkIO (runFx (scoping outer body) >> pure ())
    takeMVar tailStartedVar
    killThread runner
    threadDelay 50000
    putMVar tailGateVar ()
    finished <- pollUntil 2000000 (readIORef finishedRef)
    finished `shouldBe` True

  it "Custom async on the waiter abandons the run and rethrows" do
    ticksRef <- newIORef (0 :: Int)
    startedVar <- newEmptyMVar
    resVar <- newEmptyMVar
    let body :: Fx () Void ()
        body = do
          runTotalIO (\_ -> putMVar startedVar ())
          runTotalIO $ \_ -> forever (threadDelay 10000 >> modifyIORef' ticksRef (+ 1))
    runner <- forkIO (try @ReviewAsyncException (runFx body) >>= putMVar resVar)
    takeMVar startedVar
    throwTo runner (ReviewAsyncException "cancel")
    res <- takeMVar resVar
    case res of
      Left (ReviewAsyncException "cancel") -> return ()
      Right () -> expectationFailure "Expected ReviewAsyncException"
      Left exc -> expectationFailure ("Unexpected exception: " ++ show exc)
    firstSample <- readIORef ticksRef
    threadDelay 150000
    secondSample <- readIORef ticksRef
    secondSample `shouldBe` firstSample

  it "Custom async in a future child surfaces with the async reason" do
    -- A non-ThreadKilled asynchronous exception must still fill the future,
    -- or an awaiter would see an STM-blocked bug instead of the child failure.
    let action :: Fx () SomeException ()
        action = do
          tid <- liftIO myThreadId
          _ <- liftIO $ forkIO (threadDelay 50000 >> throwTo tid (ReviewAsyncException "probe"))
          liftIO (threadDelay 5000000)
    res <- try $ runFx $ do
      _ <- bindM2 (\_ _ -> pure ()) (absorbErr (const ()) action) (runTotalIO (\_ -> threadDelay 300000))
      pure ()
    case res :: Either FxException () of
      Left (FxException _ (AsyncFxExceptionReason _) _) -> return ()
      Left exc -> expectationFailure ("Wrong reason: " ++ show exc)
      Right _ -> expectationFailure "No exception"

  it "await rethrows an async kill instead of reporting a bug" do
    -- A kill delivered while blocked in await is cancellation, not a defect.
    tidVar <- newEmptyMVar
    resVar <- newEmptyMVar
    let spin :: Fx () Void ()
        spin = runTotalIO $ \_ -> threadDelay 5000000
        body :: Fx () Void ()
        body = do
          runTotalIO $ \_ -> myThreadId >>= putMVar tidVar
          void $ concurrently $ \lift -> lift spin <|> lift spin
    _ <- forkIO $ (try (runFx body) :: IO (Either FxException ())) >>= putMVar resVar
    tid <- takeMVar tidVar
    threadDelay 100000
    throwTo tid ThreadKilled
    res <- takeMVar resVar
    case res of
      Left (FxException _ (AsyncFxExceptionReason _) _) -> return ()
      Left exc -> expectationFailure ("Wrong reason: " ++ show exc)
      Right () -> expectationFailure "Async was swallowed"

  it "README Quick Start example runs" do
    user <- runFx $
      handleErr (\_ -> pure (User "fallback")) $
        scoping env $ do
          u <- fetchUser 42
          liftErr IOError $ print u
          pure u
    user `shouldBe` User "User 42"

  it "liftErr rethrows an async kill instead of mapping it" do
    let action :: Fx () String ()
        action = liftErr show $ do
          self <- myThreadId
          _ <- forkIO (threadDelay 50000 >> killThread self)
          threadDelay 5000000
    res <- try (runFx (absorbErr (const ()) action)) :: IO (Either FxException ())
    case res of
      Left (FxException _ (AsyncFxExceptionReason _) _) -> return ()
      Left exc -> expectationFailure ("Wrong reason: " ++ show exc)
      Right _ -> expectationFailure "Kill was swallowed as a value"

  it "liftErr maps a non-matching sync throw into a catchable Left" do
    caught <- runFx $ handleErr (\(_ :: String) -> pure "caught") $
      liftErr show (ioError (userError "boom")) >> pure "after"
    caught `shouldBe` "caught"

  it "liftErr carries the mapped exception as Left" do
    res <- try (runFx (exposeErr (liftErr show (ioError (userError "boom"))))) :: IO (Either FxException (Either String ()))
    case res of
      Right (Left msg) -> msg `shouldBe` "user error (boom)"
      Right (Right _) -> expectationFailure "Throw was swallowed as a value"
      Left exc -> expectationFailure ("Unexpected failure: " ++ show exc)


  it "worker-level async sweeps children and runs their releases" do
    -- A dead worker must not leave its children running: they get killed,
    -- their futures end up empty, and their scope releases run.
    tidVar <- newEmptyMVar
    ticksRef <- newIORef (0 :: Int)
    childReleasedRef <- newIORef False
    let childScope :: Scope Void ()
        childScope = do
          _ <- acquire (pure () :: Fx () Void ())
          _ <- registerRelease (runTotalIO (\_ -> writeIORef childReleasedRef True) :: Fx () Void ())
          pure ()
        spin :: Fx () Void ()
        spin = scoping childScope $ runTotalIO $ \_ -> forever (threadDelay 10000 >> modifyIORef' ticksRef (+ 1))
        body :: Fx () Void ()
        body = do
          runTotalIO $ \_ -> myThreadId >>= putMVar tidVar
          void $ bindM2 (\_ _ -> pure ()) spin (pure ())
    resVar <- newEmptyMVar
    _ <- forkIO $ (try (runFx body) :: IO (Either FxException ())) >>= putMVar resVar
    tid <- takeMVar tidVar
    threadDelay 150000
    beforeKill <- readIORef ticksRef
    beforeKill `shouldNotBe` 0
    throwTo tid (ReviewAsyncException "worker-kill")
    res <- takeMVar resVar
    case res of
      Left (FxException _ (AsyncFxExceptionReason _) _) -> return ()
      Left exc -> expectationFailure ("Wrong exception: " ++ show exc)
      Right () -> expectationFailure "Async was swallowed"
    t1 <- readIORef ticksRef
    threadDelay 300000
    t2 <- readIORef ticksRef
    t2 `shouldBe` t1
    released <- pollUntil 2000000 (readIORef childReleasedRef)
    released `shouldBe` True

data ReviewAsyncException = ReviewAsyncException String
  deriving stock (Show, Typeable)

instance Exception ReviewAsyncException where
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

-- the README example

data Connection = Connection

data User = User String
  deriving stock (Eq, Show)

data Env = Env {dbConn :: Connection, apiKey :: String}

data Err = ApiError String | DbError SomeException | IOError SomeException
  deriving stock Show

connectDb :: String -> Fx () Err Connection
connectDb _ = pure Connection

closeDb :: Fx Connection Err ()
closeDb = pure ()

readApiKey :: FilePath -> Fx () Err String
readApiKey _ = pure "key"

queryUser :: Connection -> Int -> IO User
queryUser _ n = pure (User ("User " ++ show n))

env :: Scope Err Env
env = do
  conn <-
    releasing closeDb $
      acquire (connectDb "postgres://localhost/mydb")
  key <-
    acquire (readApiKey "config.txt")
  pure $ Env conn key

fetchUser :: Int -> Fx Env Err User
fetchUser userId =
  mapErr DbError $
    runExceptionalIO $ \dbEnv ->
      queryUser (dbConn dbEnv) userId

pollUntil :: Int -> IO Bool -> IO Bool
pollUntil micros check
  | micros <= 0 = check
  | otherwise = do
      ok <- check
      if ok
        then return True
        else threadDelay 10000 >> pollUntil (micros - 10000) check

-- |
-- Run IO in the current masking state instead of restoring the ambient
-- unmasking policy. Test-only helper for holding a worker in a masked body
-- so queued asynchronous exceptions stay pending until cleanup.
{-# NOINLINE maskedIO_ #-}
maskedIO_ :: IO () -> Fx env err ()
maskedIO_ io = unsafePerformIO io `seq` pure ()

testException :: Fx () Void a -> (FxException -> Bool) -> IO ()
testException fx validateExc = do
  res <- try @FxException $ runFx $ fx
  case res of
    Left exc ->
      if validateExc exc
        then return ()
        else expectationFailure (show exc)
    Right _ -> expectationFailure "No exception"
