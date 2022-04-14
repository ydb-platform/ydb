#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_utils.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>

using namespace NThreading;
using namespace NKikimr;
using namespace NKikimr::NPersQueueTests;

namespace NPersQueue {
    Y_UNIT_TEST_SUITE(TRetryingConsumerTest) {
        static TConsumerSettings FakeSettings() {
            TConsumerSettings settings;
            settings.ReconnectOnFailure = true;
            settings.Server = TServerSetting{"localhost"};
            settings.Topics.push_back("topic");
            settings.ClientId = "client";
            return settings;
        }

        Y_UNIT_TEST(NotStartedConsumerCanBeDestructed) {
            // Test that consumer doesn't hang on till shutdown
            TPQLib lib;
            auto settings = FakeSettings();
            lib.CreateConsumer(settings);
        }

        Y_UNIT_TEST(StartDeadlineExpires) {
            TPQLibSettings libSettings;
            libSettings.ChannelCreationTimeout = TDuration::MilliSeconds(1);
            libSettings.DefaultLogger = new TCerrLogger(TLOG_DEBUG);
            TPQLib lib(libSettings);
            THolder<IConsumer> consumer;
            {
                auto settings = FakeSettings();
                settings.ReconnectionDelay = TDuration::MilliSeconds(1);
                settings.StartSessionTimeout = TDuration::MilliSeconds(10);
                if (GrpcV1EnabledByDefault()) {
                    settings.MaxAttempts = 3;
                }
                consumer = lib.CreateConsumer(settings);
            }
            const TInstant beforeStart = TInstant::Now();
            auto future = consumer->Start(TDuration::MilliSeconds(100));
            if (!GrpcV1EnabledByDefault()) {
                UNIT_ASSERT(future.GetValueSync().Response.HasError());
                UNIT_ASSERT_EQUAL_C(future.GetValueSync().Response.GetError().GetCode(), NErrorCode::CREATE_TIMEOUT,
                                    "Error: " << future.GetValueSync().Response.GetError());
                const TInstant now = TInstant::Now();
                UNIT_ASSERT_C(now - beforeStart >= TDuration::MilliSeconds(100), now);
            }
            auto isDead = consumer->IsDead();
            isDead.Wait();

            DestroyAndWait(consumer);
        }

        Y_UNIT_TEST(StartMaxAttemptsExpire) {
            TPQLibSettings libSettings;
            libSettings.ChannelCreationTimeout = TDuration::MilliSeconds(1);
            libSettings.DefaultLogger = new TCerrLogger(TLOG_DEBUG);
            TPQLib lib(libSettings);
            THolder<IConsumer> consumer;
            {
                auto settings = FakeSettings();
                settings.ReconnectionDelay = TDuration::MilliSeconds(10);
                settings.StartSessionTimeout = TDuration::MilliSeconds(10);
                settings.MaxAttempts = 3;
                consumer = lib.CreateConsumer(settings);
            }
            const TInstant beforeStart = TInstant::Now();
            auto future = consumer->Start();
            if (!GrpcV1EnabledByDefault()) {
                UNIT_ASSERT(future.GetValueSync().Response.HasError());
            }
            auto deadFuture = consumer->IsDead();
            deadFuture.Wait();
            const TInstant now = TInstant::Now();
            UNIT_ASSERT_C(now - beforeStart >= TDuration::MilliSeconds(30), now);

            DestroyAndWait(consumer);
        }

        static void AssertWriteValid(const NThreading::TFuture<TProducerCommitResponse>& respFuture) {
            const TProducerCommitResponse& resp = respFuture.GetValueSync();
            UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TWriteResponse::kAck, "Msg: " << resp.Response);
        }

        static void AssertReadValid(const NThreading::TFuture<TConsumerMessage>& respFuture) {
            const TConsumerMessage& resp = respFuture.GetValueSync();
            UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TReadResponse::kData, "Msg: " << resp.Response);
        }

        static void AssertCommited(const NThreading::TFuture<TConsumerMessage>& respFuture) {
            const TConsumerMessage& resp = respFuture.GetValueSync();
            UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TReadResponse::kCommit, "Msg: " << resp.Response);
        }

        static void AssertReadFailed(const NThreading::TFuture<TConsumerMessage>& respFuture) {
            const TConsumerMessage& resp = respFuture.GetValueSync();
            UNIT_ASSERT_EQUAL_C(resp.Response.GetResponseCase(), TReadResponse::kError, "Msg: " << resp.Response);
        }

        static TProducerSettings MakeProducerSettings(const TTestServer& testServer) {
            TProducerSettings producerSettings;
            producerSettings.ReconnectOnFailure = false;
            producerSettings.Topic = "topic1";
            producerSettings.SourceId = "123";
            producerSettings.Server = TServerSetting{"localhost", testServer.GrpcPort};
            producerSettings.Codec = ECodec::LZOP;
            return producerSettings;
        }

        static TConsumerSettings MakeConsumerSettings(const TTestServer& testServer) {
            TConsumerSettings settings;
            settings.ReconnectOnFailure = true;
            settings.Server = TServerSetting{"localhost", testServer.GrpcPort};
            settings.Topics.emplace_back("topic1");
            settings.ClientId = "user";
            return settings;
        }

        Y_UNIT_TEST(ReconnectsToServer) {
            TTestServer testServer(false);
            testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
            testServer.StartServer(false);

            testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
            const size_t partitions = 10;
            testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", partitions);

            testServer.WaitInit("topic1");

            TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
            TPQLib PQLib;

            auto producer = PQLib.CreateProducer(MakeProducerSettings(testServer), logger, false);
            UNIT_ASSERT(!producer->IsDead().HasValue());
            producer->Start().Wait();

            auto consumer = PQLib.CreateConsumer(MakeConsumerSettings(testServer), logger, false);
            TFuture<TError> isDead = consumer->IsDead();
            UNIT_ASSERT(!isDead.HasValue());
            consumer->Start().Wait();

            auto read1 = consumer->GetNextMessage();

            // write first value
            auto write1 = producer->Write(1, TString("blob1"));
            AssertWriteValid(write1);
            AssertReadValid(read1);

            // commit
            consumer->Commit({1});
            auto commitAck = consumer->GetNextMessage();
            Cerr << "wait for commit1\n";

            AssertCommited(commitAck);

            Cerr << "After shutdown\n";
            testServer.ShutdownGRpc();

            auto read2 = consumer->GetNextMessage();
            UNIT_ASSERT(!isDead.HasValue());
            testServer.EnableGRpc();

            testServer.WaitInit("topic1");

            Cerr << "Wait producer1 death\n";

            producer->IsDead().Wait();

            producer = PQLib.CreateProducer(MakeProducerSettings(testServer), logger, false);
            Cerr << "Wait producer2 start\n";
            producer->Start().Wait();
            UNIT_ASSERT(!producer->IsDead().HasValue());
            auto write2 = producer->Write(2, TString("blob2"));
            Cerr << "Wait for second write\n";
            AssertWriteValid(write2);
            Cerr << "wait for read second blob\n";
            AssertReadValid(read2);
            UNIT_ASSERT_VALUES_EQUAL(2u, read2.GetValueSync().Response.GetData().GetCookie());
            UNIT_ASSERT(!isDead.HasValue());
            consumer->Commit({2});
            Cerr << "wait for commit2\n";
            auto commitAck2 = consumer->GetNextMessage();
            AssertCommited(commitAck2);

            DestroyAndWait(producer);
            DestroyAndWait(consumer);
        }

        static void DiesOnTooManyReconnectionAttempts(bool callRead) {
            TTestServer testServer(false);
            testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
            testServer.StartServer(false);

            testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
            testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", 2);

            testServer.WaitInit("topic1");

            TConsumerSettings settings = MakeConsumerSettings(testServer);
            settings.MaxAttempts = 3;
            settings.ReconnectionDelay = TDuration::MilliSeconds(100);
            auto consumer = testServer.PQLib->CreateConsumer(settings, testServer.PQLibSettings.DefaultLogger, false);
            consumer->Start().Wait();
            TFuture<TError> isDead = consumer->IsDead();
            UNIT_ASSERT(!isDead.HasValue());

            // shutdown server
            const TInstant beforeShutdown = TInstant::Now();
            testServer.ShutdownServer();

            NThreading::TFuture<TConsumerMessage> read;
            if (callRead) {
                read = consumer->GetNextMessage();
            }

            isDead.Wait();
            const TInstant afterDead = TInstant::Now();
            // 3 attempts: 100ms, 200ms and 300ms
            UNIT_ASSERT_C(afterDead - beforeShutdown >= TDuration::MilliSeconds(GrpcV1EnabledByDefault() ? 300 : 600), "real difference: " << (afterDead - beforeShutdown));

            if (callRead) {
                AssertReadFailed(read);
            }
        }

        Y_UNIT_TEST(DiesOnTooManyReconnectionAttemptsWithoutRead) {
            // Check that we reconnect even without explicit write errors
            DiesOnTooManyReconnectionAttempts(false);
        }

        Y_UNIT_TEST(DiesOnTooManyReconnectionAttemptsWithRead) {
            DiesOnTooManyReconnectionAttempts(true);
        }

        Y_UNIT_TEST(ReadBeforeConnectToServer) {
            TTestServer testServer(false);
            testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
            testServer.StartServer(false);

            testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
            const size_t partitions = 10;
            testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", partitions);

            testServer.WaitInit("topic1");

            auto consumer = testServer.PQLib->CreateConsumer(
                    MakeConsumerSettings(testServer), testServer.PQLibSettings.DefaultLogger, false
            );
            auto consumerStarted = consumer->Start();
            auto read1 = consumer->GetNextMessage();
            consumerStarted.Wait();
            TVector<TFuture<TConsumerMessage>> messages;
            for (int i = 2; i <= 5; ++i) {
                messages.push_back(consumer->GetNextMessage());
            }

            auto producer = testServer.PQLib->CreateProducer(
                    MakeProducerSettings(testServer), testServer.PQLibSettings.DefaultLogger, false
            );
            producer->Start().Wait();

            while (!messages.back().HasValue()) {
                auto write = producer->Write(TString("data"));
                AssertWriteValid(write);
            }

            AssertReadValid(read1);
            for (auto& msg: messages) {
                AssertReadValid(msg);
            }

            DestroyAndWait(producer);
            DestroyAndWait(consumer);
        }

        Y_UNIT_TEST(CancelsStartAfterPQLibDeath) {
            TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
            TPQLibSettings pqLibSettings;
            pqLibSettings.DefaultLogger = logger;
            THolder<TPQLib> PQLib = MakeHolder<TPQLib>(pqLibSettings);

            auto consumer = PQLib->CreateConsumer(FakeSettings(), logger, false);
            auto start = consumer->Start();
            if (!GrpcV1EnabledByDefault()) {
                UNIT_ASSERT(!start.HasValue());
            }

            PQLib = nullptr;

            if (!GrpcV1EnabledByDefault()) {
                UNIT_ASSERT(start.HasValue());
                UNIT_ASSERT(start.GetValue().Response.HasError());
            }

            auto dead = consumer->IsDead();
            if (GrpcV1EnabledByDefault()) {
                dead.Wait();
            } else {
                UNIT_ASSERT(dead.HasValue());
            }

            auto read = consumer->GetNextMessage();
            UNIT_ASSERT(read.HasValue());
            UNIT_ASSERT(read.GetValueSync().Response.HasError());
        }

        Y_UNIT_TEST(LockSession) {
            SDKTestSetup setup("LockSession", false);
            setup.GetGrpcServerOptions().SetGRpcShutdownDeadline(TDuration::MilliSeconds(100));
            setup.Start();
            auto consumerSettings = setup.GetConsumerSettings();
            consumerSettings.UseLockSession = true;
            consumerSettings.ReconnectOnFailure = true;
            auto consumer = setup.StartConsumer(consumerSettings);

            setup.WriteToTopic({"msg1", "msg2"});

            size_t releases = 0;
            for (size_t times = 0; times < 2; ++times) {
                while (true) {
                    auto msgFuture = consumer->GetNextMessage();
                    auto& msg = msgFuture.GetValueSync();
                    setup.GetLog() << TLOG_INFO << "Got response from consumer: " << msg.Response;
                    if (times) {
                        UNIT_ASSERT_C(msg.Type == EMT_LOCK || msg.Type == EMT_DATA || msg.Type == EMT_RELEASE, msg.Response);
                    } else {
                        UNIT_ASSERT_C(msg.Type == EMT_LOCK || msg.Type == EMT_DATA, msg.Response);
                    }
                    if (msg.Type == EMT_LOCK) {
                        TLockInfo lockInfo;
                        lockInfo.ReadOffset = 1;
                        msg.ReadyToRead.SetValue(lockInfo);
                    } else if (msg.Type == EMT_DATA) {
                        UNIT_ASSERT_VALUES_EQUAL_C(msg.Response.GetData().MessageBatchSize(), 1, msg.Response);
                        UNIT_ASSERT_VALUES_EQUAL_C(msg.Response.GetData().GetMessageBatch(0).MessageSize(), 1, msg.Response);
                        UNIT_ASSERT_VALUES_EQUAL_C(msg.Response.GetData().GetMessageBatch(0).GetMessage(0).GetData(), "msg2", msg.Response);
                        break;
                    } else if (msg.Type == EMT_RELEASE) {
                        ++releases;
                    }
                }
                if (!times) { // force reconnect
                    setup.ShutdownGRpc();
                    setup.EnableGRpc();
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(releases, 1);
        }

        Y_UNIT_TEST(ReadAhead) {
            SDKTestSetup setup("ReadAhead");

            auto consumerSettings = setup.GetConsumerSettings();
            consumerSettings.ReconnectOnFailure = true;
            auto consumer = setup.StartConsumer(consumerSettings);

            setup.WriteToTopic({"msg"});

            // Wait until read ahead occurs.
            Sleep(TDuration::MilliSeconds(100));

            setup.ReadFromTopic({{"msg"}}, true, consumer.Get());
        }

        Y_UNIT_TEST(ReadBeforeWrite) {
            SDKTestSetup setup("ReadBeforeWrite");

            auto consumerSettings = setup.GetConsumerSettings();
            consumerSettings.ReconnectOnFailure = true;
            auto consumer = setup.StartConsumer(consumerSettings);

            auto msgFuture = consumer->GetNextMessage();

            Sleep(TDuration::MilliSeconds(100));

            setup.WriteToTopic({"msg"});

            const auto& msg = msgFuture.GetValueSync();
            UNIT_ASSERT_EQUAL(msg.Type, EMT_DATA);
            UNIT_ASSERT_STRINGS_EQUAL(msg.Response.GetData().GetMessageBatch(0).GetMessage(0).GetData(), "msg");
        }

        Y_UNIT_TEST(CommitDisabled) {
            SDKTestSetup setup("CommitDisabled");

            auto consumerSettings = setup.GetConsumerSettings();
            consumerSettings.ReconnectOnFailure = true;
            consumerSettings.CommitsDisabled = true;
            auto consumer = setup.StartConsumer(consumerSettings);
            setup.WriteToTopic({"msg", "msg2"});
            setup.ReadFromTopic({{"msg", "msg2"}}, false, consumer.Get());
            // check that commit fails with error when commits disabled
            setup.WriteToTopic({"msg3"});
            auto msgFuture = consumer->GetNextMessage();
            const auto& msg = msgFuture.GetValueSync();
            UNIT_ASSERT_C(!msg.Response.HasError(), msg.Response);
            UNIT_ASSERT_EQUAL(msg.Type, EMT_DATA);
            UNIT_ASSERT_STRINGS_EQUAL(msg.Response.GetData().GetMessageBatch(0).GetMessage(0).GetData(), "msg3");
            consumer->Commit({msg.Response.GetData().GetCookie()});
            auto isDead = consumer->IsDead();
            auto& err = isDead.GetValueSync();
            UNIT_ASSERT_STRINGS_EQUAL("Commits are disabled", err.GetDescription());
        }

        void LockReleaseTest(bool multiclusterConsumer, bool killPqrb) {
            SDKTestSetup setup("LockRelease");
            const TString topicName = "lock_release";
            const size_t partitionsCount = 10;
            setup.CreateTopic(topicName, setup.GetLocalCluster(), partitionsCount);

            auto consumerSettings = setup.GetConsumerSettings();
            consumerSettings.Topics[0] = topicName;
            consumerSettings.ReconnectOnFailure = true;
            consumerSettings.UseLockSession = true;
            if (multiclusterConsumer) { // The same test for multicluster wrapper.
                consumerSettings.ReadFromAllClusterSources = true;
                consumerSettings.ReadMirroredPartitions = false;
            }
            auto consumer = setup.StartConsumer(consumerSettings);

            std::vector<int> partitionsLocked(partitionsCount * 2); // For both consumers.
            std::vector<ui64> partitionsGens(partitionsCount * 2);
            for (size_t i = 0; i < partitionsCount; ++i) {
                auto lock = consumer->GetNextMessage().GetValueSync();
                UNIT_ASSERT_EQUAL_C(lock.Type, EMT_LOCK, lock.Response);
                UNIT_ASSERT_C(!partitionsLocked[lock.Response.GetLock().GetPartition()], lock.Response);
                partitionsLocked[lock.Response.GetLock().GetPartition()] = 1;
                partitionsGens[lock.Response.GetLock().GetPartition()] = lock.Response.GetLock().GetGeneration();
                lock.ReadyToRead.SetValue({});
            }

            const size_t messagesCount = 100;
            auto generateMessages = [&]() {
                for (size_t i = 0; i < messagesCount; ++i) { // Write to several random partitions.
                    const TString uniqStr = CreateGuidAsString();
                    auto settings = setup.GetProducerSettings();
                    settings.Topic = topicName;
                    settings.SourceId = uniqStr;
                    auto producer = setup.StartProducer(settings);
                    setup.WriteToTopic({uniqStr}, producer.Get());
                }
            };

            generateMessages();

            THolder<IConsumer> otherConsumer = nullptr;
            bool rebalanced = false;

            NThreading::TFuture<TConsumerMessage> msg1 = consumer->GetNextMessage();
            NThreading::TFuture<TConsumerMessage> msg2;
            size_t hardReleasesCount = 0;

            THashSet<TString> msgsReceived;
            THashSet<ui64> cookiesToCommit;
            auto locksCount = [&]() -> int {
                return std::accumulate(partitionsLocked.begin(), partitionsLocked.end(), 0);
            };
            auto allEventsReceived = [&]() -> bool {
                return msgsReceived.size() >= messagesCount * 2
                    && locksCount() == partitionsCount
                    && (!killPqrb || hardReleasesCount > 0)
                    && cookiesToCommit.empty();
            };

            int iterationNum = 0;
            while (!allEventsReceived()) {
                ++iterationNum;
                Cerr << "\nIterationNum: " << iterationNum << Endl;
                Cerr << "msgsReceived.size(): " << msgsReceived.size() << Endl;
                Cerr << "partitionsCount: " << partitionsCount << Endl;
                Cerr << "locksGot: " << locksCount() << Endl;
                Cerr << "hardReleasesCount: " << hardReleasesCount << Endl;
                Cerr << "cookiesToCommit.size(): " << cookiesToCommit.size() << Endl;
                auto waiter = msg2.Initialized() ? NThreading::WaitAny(msg1.IgnoreResult(), msg2.IgnoreResult()) : msg1.IgnoreResult();
                waiter.Wait();
                auto processValue = [&](auto& msg, auto& consumer) {
                    const bool isOtherConsumer = consumer == otherConsumer;
                    const ui64 consumerMix = isOtherConsumer ? 0x8000000000000000 : 0;

                    switch (msg.GetValue().Type) {
                    case EMT_LOCK:
                        {
                            UNIT_ASSERT_LT(msg.GetValue().Response.GetLock().GetPartition(), partitionsCount);
                            const size_t idx = msg.GetValue().Response.GetLock().GetPartition() + (isOtherConsumer ? partitionsCount : 0);
                            if (msg.GetValue().Response.GetLock().GetGeneration() >= partitionsGens[idx]) {
                                UNIT_ASSERT_C(!partitionsLocked[idx], msg.GetValue().Response << ". Other: " << isOtherConsumer);
                                partitionsLocked[idx] = 1;
                                msg.GetValue().ReadyToRead.SetValue({});
                                partitionsGens[idx] = msg.GetValue().Response.GetLock().GetGeneration();
                            }
                            break;
                        }
                    case EMT_RELEASE:
                        {
                            UNIT_ASSERT_LT(msg.GetValue().Response.GetRelease().GetPartition(), partitionsCount);
                            const size_t idx = msg.GetValue().Response.GetRelease().GetPartition() + (isOtherConsumer ? partitionsCount : 0);
                            partitionsLocked[idx] = 0;
                            if (!msg.GetValue().Response.GetRelease().GetCanCommit()) {
                                ++hardReleasesCount;
                            }
                            if (!killPqrb) {
                                UNIT_ASSERT(msg.GetValue().Response.GetRelease().GetCanCommit()); // No restarts => soft release.
                            }
                            break;
                        }
                    case EMT_DATA:
                        {
                            const auto& resp = msg.GetValue().Response.GetData();
                            UNIT_ASSERT(cookiesToCommit.insert(resp.GetCookie() | consumerMix).second);
                            for (const auto& batch : resp.GetMessageBatch()) {
                                for (const auto& message : batch.GetMessage()) {
                                    msgsReceived.insert(message.GetData());
                                }
                            }
                            consumer->Commit({resp.GetCookie()});
                            break;
                        }
                    case EMT_ERROR:
                    case EMT_STATUS:
                    case EMT_COMMIT:
                        for (ui64 cookie : msg.GetValue().Response.GetCommit().GetCookie()) {
                            UNIT_ASSERT(cookiesToCommit.find(cookie | consumerMix) != cookiesToCommit.end());
                            cookiesToCommit.erase(cookie | consumerMix);
                        }
                        break;
                    }
                    msg = consumer->GetNextMessage();
                };

                if (msg1.HasValue()) {
                    processValue(msg1, consumer);
                }
                if (msg2.Initialized() && msg2.HasValue()) {
                    processValue(msg2, otherConsumer);
                }

                if (!otherConsumer && msgsReceived.size() >= messagesCount / 4) {
                    otherConsumer = setup.StartConsumer(consumerSettings);
                    msg2 = otherConsumer->GetNextMessage();
                }

                if (!rebalanced && msgsReceived.size() >= messagesCount / 2) {
                    rebalanced = true;

                    if (killPqrb) {
                        Sleep(TDuration::MilliSeconds(100));
                        setup.KillPqrb(topicName, setup.GetLocalCluster());
                    }

                    generateMessages();
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(locksCount(), partitionsCount);
            if (killPqrb) {
                UNIT_ASSERT(hardReleasesCount > 0);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(hardReleasesCount, 0);
            }
        }

        Y_UNIT_TEST(LockRelease) {
            LockReleaseTest(false, false);
        }

        Y_UNIT_TEST(LockReleaseHardRelease) {
            LockReleaseTest(false, true);
        }

        Y_UNIT_TEST(LockReleaseMulticluster) {
            LockReleaseTest(true, false);
        }

        Y_UNIT_TEST(LockReleaseMulticlusterHardRelease) {
            LockReleaseTest(true, true);
        }

        Y_UNIT_TEST(NonRetryableErrorOnStart) {
            SDKTestSetup setup("NonRetryableErrorOnStart");

            auto consumerSettings = setup.GetConsumerSettings();
            //consumerSettings.
            consumerSettings.ReconnectOnFailure = true;
            consumerSettings.Topics = { "unknown/topic" };
            THolder<IConsumer> consumer = setup.GetPQLib()->CreateConsumer(consumerSettings);
            auto startFuture = consumer->Start();
            if (!GrpcV1EnabledByDefault()) {
                Cerr << "===Got response: " << startFuture.GetValueSync().Response << Endl;
                UNIT_ASSERT_C(startFuture.GetValueSync().Response.HasError(), startFuture.GetValueSync().Response);
                UNIT_ASSERT_EQUAL(startFuture.GetValueSync().Response.GetError().GetCode(), NPersQueue::NErrorCode::UNKNOWN_TOPIC);
            }
            auto deadFuture = consumer->IsDead();
            UNIT_ASSERT_EQUAL_C(deadFuture.GetValueSync().GetCode(), NPersQueue::NErrorCode::UNKNOWN_TOPIC, deadFuture.GetValueSync());
        }
    }
} // namespace NPersQueue
