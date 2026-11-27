module Main where

import Control.Monad (forever)
import Control.Monad.Parallel (bindM2)
import Fx
import Test.Hspec
import Prelude hiding (choose)

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

data ReviewAsyncException = ReviewAsyncException String
  deriving stock (Show, Typeable)

instance Exception ReviewAsyncException where
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

pollUntil :: Int -> IO Bool -> IO Bool
pollUntil micros check
  | micros <= 0 = check
  | otherwise = do
      ok <- check
      if ok
        then return True
        else threadDelay 10000 >> pollUntil (micros - 10000) check

testException :: Fx () Void a -> (FxException -> Bool) -> IO ()
testException fx validateExc = do
  res <- try @FxException $ runFx $ fx
  case res of
    Left exc ->
      if validateExc exc
        then return ()
        else expectationFailure (show exc)
    Right _ -> expectationFailure "No exception"
