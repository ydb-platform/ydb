#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/managed_executor.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/ut_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <future>

namespace NYdb::NTopic::NTests {

Y_UNIT_TEST_SUITE(BasicUsage) {
    Y_UNIT_TEST(ConnectToYDB) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);

        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "invalid:" << setup->GetGrpcPort());
        cfg.SetDatabase("/Invalid");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        auto driver = NYdb::TDriver(cfg);

        {
            TTopicClient client(driver);

            auto writeSettings = TWriteSessionSettings()
                .Path(setup->GetTestTopic())
                .MessageGroupId("group_id")
                // TODO why retries? see LOGBROKER-8490
                .RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
            auto writeSession = client.CreateWriteSession(writeSettings);

            auto event = writeSession->GetEvent(true);
            UNIT_ASSERT(event.Defined() && std::holds_alternative<TSessionClosedEvent>(event.GetRef()));
        }

        {
            auto settings = TTopicClientSettings()
                .Database({"/Root"})
                .DiscoveryEndpoint({TStringBuilder() << "localhost:" << setup->GetGrpcPort()});

            TTopicClient client(driver, settings);

            auto writeSettings = TWriteSessionSettings()
                .Path(setup->GetTestTopic())
                .MessageGroupId("group_id")
                .RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
            auto writeSession = client.CreateWriteSession(writeSettings);

            auto event = writeSession->GetEvent(true);
            UNIT_ASSERT(event.Defined() && !std::holds_alternative<TSessionClosedEvent>(event.GetRef()));
        }
    }


    Y_UNIT_TEST(WriteRead) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        TTopicClient client(setup->GetDriver());
        
        {
            auto writeSettings = TWriteSessionSettings()
                        .Path(setup->GetTestTopic())
                        .ProducerId(setup->GetTestMessageGroupId())
                        .MessageGroupId(setup->GetTestMessageGroupId());
            auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
            UNIT_ASSERT(writeSession->Write("message_using_MessageGroupId"));
            writeSession->Close();
        }
        {
            auto writeSettings = TWriteSessionSettings()
                        .Path(setup->GetTestTopic())
                        .ProducerId(setup->GetTestMessageGroupId())
                        .PartitionId(0);
            auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
            UNIT_ASSERT(writeSession->Write("message_using_PartitionId"));
            writeSession->Close();
        }

        {
            auto readSettings = TReadSessionSettings()
                .ConsumerName(setup->GetTestConsumer())
                .AppendTopics(setup->GetTestTopic());
            auto readSession = client.CreateReadSession(readSettings);

            auto event = readSession->GetEvent(true);
            UNIT_ASSERT(event.Defined());

            auto& startPartitionSession = std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event);
            startPartitionSession.Confirm();

            event = readSession->GetEvent(true);
            UNIT_ASSERT(event.Defined());

            auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            dataReceived.Commit();

            auto& messages = dataReceived.GetMessages();
            UNIT_ASSERT(messages.size() == 2);
            UNIT_ASSERT(messages[0].GetData() == "message_using_MessageGroupId");
            UNIT_ASSERT(messages[1].GetData() == "message_using_PartitionId");
        }
    }

    Y_UNIT_TEST(MaxByteSizeEqualZero) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        TTopicClient client(setup->GetDriver());

        auto writeSettings = TWriteSessionSettings()
            .Path(setup->GetTestTopic())
            .MessageGroupId("group_id");
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
        UNIT_ASSERT(writeSession->Write("message"));
        writeSession->Close();

        auto readSettings = TReadSessionSettings()
            .ConsumerName(setup->GetTestConsumer())
            .AppendTopics(setup->GetTestTopic());
        auto readSession = client.CreateReadSession(readSettings);

        auto event = readSession->GetEvent(true);
        UNIT_ASSERT(event.Defined());

        auto& startPartitionSession = std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event);
        startPartitionSession.Confirm();

        UNIT_CHECK_GENERATED_EXCEPTION(readSession->GetEvent(true, 0), TContractViolation);
        UNIT_CHECK_GENERATED_EXCEPTION(readSession->GetEvents(true, Nothing(), 0), TContractViolation);

        event = readSession->GetEvent(true, 1);
        UNIT_ASSERT(event.Defined());

        auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
        dataReceived.Commit();
    }

    Y_UNIT_TEST(WriteAndReadSomeMessagesWithSyncCompression) {

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);

        NPersQueue::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        writeSettings.Codec(NPersQueue::ECodec::RAW);
        NPersQueue::IExecutor::TPtr executor = new NPersQueue::TSyncExecutor();
        writeSettings.CompressionExecutor(executor);

        ui64 count = 100u;
        TMaybe<bool> shouldCaptureData = {true};

        auto& client = setup->GetPersQueueClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);
        TString messageBase = "message----";
        TVector<TString> sentMessages;

        for (auto i = 0u; i < count; i++) {
            // sentMessages.emplace_back(messageBase * (i+1) + ToString(i));
            sentMessages.emplace_back(messageBase * (200 * 1024));
            auto res = session->Write(sentMessages.back());
            UNIT_ASSERT(res);
        }
        {
            auto sessionAdapter = NPersQueue::NTests::TSimpleWriteSessionTestAdapter(
                    dynamic_cast<NPersQueue::TSimpleBlockingWriteSession *>(session.get()));
            if (shouldCaptureData.Defined()) {
                TStringBuilder msg;
                msg << "Session has captured " << sessionAdapter.GetAcquiredMessagesCount()
                    << " messages, capturing was expected: " << *shouldCaptureData << Endl;
                UNIT_ASSERT_VALUES_EQUAL_C(sessionAdapter.GetAcquiredMessagesCount() > 0, *shouldCaptureData, msg.c_str());
            }
        }
        session->Close();

        std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;

        // Create topic client.
        NYdb::NTopic::TTopicClient topicClient(setup->GetDriver());

        // Create read session.
        NYdb::NTopic::TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(setup->GetTestConsumer())
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        Cerr << "Session was created" << Endl;

        NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
        auto totalReceived = 0u;

        auto f = checkedPromise.GetFuture();
        TAtomic check = 1;
        readSettings.EventHandlers_.SimpleDataHandlers(
            // [checkedPromise = std::move(checkedPromise), &check, &sentMessages, &totalReceived]
            [&]
            (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& ev) mutable {
            Y_VERIFY_S(AtomicGet(check) != 0, "check is false");
            auto& messages = ev.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];
                UNIT_ASSERT_VALUES_EQUAL(message.GetData(), sentMessages[totalReceived]);
                totalReceived++;
            }
            if (totalReceived == sentMessages.size())
                checkedPromise.SetValue();
        });

        ReadSession = topicClient.CreateReadSession(readSettings);

        f.GetValueSync();
        ReadSession->Close(TDuration::MilliSeconds(10));
        AtomicSet(check, 0);

        auto status = topicClient.CommitOffset(setup->GetTestTopic(), 0, setup->GetTestConsumer(), 50);
        UNIT_ASSERT(status.GetValueSync().IsSuccess());

        auto describeConsumerSettings = TDescribeConsumerSettings().IncludeStats(true);
        auto result = topicClient.DescribeConsumer("/Root/PQ/rt3.dc1--" + setup->GetTestTopic(), setup->GetTestConsumer(), describeConsumerSettings).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto description = result.GetConsumerDescription();
        UNIT_ASSERT(description.GetPartitions().size() == 1);
        auto stats = description.GetPartitions().front().GetPartitionConsumerStats();
        UNIT_ASSERT(stats.Defined());
        UNIT_ASSERT(stats->GetCommittedOffset() == 50);
    }


    Y_UNIT_TEST(ReadWithRestarts) {

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);

        NPersQueue::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        writeSettings.Codec(NPersQueue::ECodec::RAW);
        NPersQueue::IExecutor::TPtr executor = new NPersQueue::TSyncExecutor();
        writeSettings.CompressionExecutor(executor);

        auto& client = setup->GetPersQueueClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

        ui32 count = 700;
        std::string message(2'000, 'x');
        for (ui32 i = 1; i <= count; ++i) {
            bool res = session->Write(message);
            UNIT_ASSERT(res);
        }
        bool res = session->Close(TDuration::Seconds(10));
        UNIT_ASSERT(res);

        std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;

        // Create topic client.
        NYdb::NTopic::TTopicClient topicClient(setup->GetDriver());

        // Create read session.
        NYdb::NTopic::TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(setup->GetTestConsumer())
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        //
        // controlled decompressor
        //
        auto decompressor = CreateThreadPoolManagedExecutor(1);
        readSettings.DecompressionExecutor(decompressor);


        //
        // auxiliary functions for decompressor and handler control
        //
        auto WaitTasks = [&](auto f, size_t c) {
            while (f() < c) {
                Sleep(TDuration::MilliSeconds(100));
            };
        };
        auto WaitPlannedTasks = [&](auto e, size_t count) {
            WaitTasks([&]() { return e->GetPlannedCount(); }, count);
        };
        auto WaitExecutedTasks = [&](auto e, size_t count) {
            WaitTasks([&]() { return e->GetExecutedCount(); }, count);
        };

        auto RunTasks = [&](auto e, const std::vector<size_t>& tasks) {
            size_t n = tasks.size();
            WaitPlannedTasks(e, n);
            size_t completed = e->GetExecutedCount();
            e->StartFuncs(tasks);
            WaitExecutedTasks(e, completed + n);
        };
        Y_UNUSED(RunTasks);

        auto PlanTasksAndRestart = [&](auto e, const std::vector<size_t>& tasks) {
            size_t n = tasks.size();
            WaitPlannedTasks(e, n);
            size_t completed = e->GetExecutedCount();

            setup->GetServer().KillTopicPqrbTablet(setup->GetTestTopicPath());
            Sleep(TDuration::MilliSeconds(100));

            e->StartFuncs(tasks);
            WaitExecutedTasks(e, completed + n);
        };
        Y_UNUSED(PlanTasksAndRestart);


        NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
        TAtomic lastOffset = 0u;

        auto f = checkedPromise.GetFuture();
        readSettings.EventHandlers_.SimpleDataHandlers(
            [&]
            (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& ev) mutable {
            AtomicSet(lastOffset, ev.GetMessages().back().GetOffset());
            ev.Commit();
            Cerr << ">>> TEST: last offset = " << lastOffset << Endl;
        });

        Cerr << ">>> TEST: Create session" << Endl;

        ReadSession = topicClient.CreateReadSession(readSettings);

        ui32 i = 0;
        while (AtomicGet(lastOffset) + 1 < count) {
            Cerr << ">>> TEST: last offset = " << AtomicGet(lastOffset) << Endl;
            // TODO (ildar-khisam@): restarts with progress and check sdk budget
            // PlanTasksAndRestart(decompressor, {i++});
            RunTasks(decompressor, {i++});
        }

        ReadSession->Close(TDuration::MilliSeconds(10));
        Cerr << ">>> TEST: Session gracefully closed" << Endl;
    }

    Y_UNIT_TEST(SessionNotDestroyedWhileCompressionInFlight) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);

        // controlled executor
        auto stepByStepExecutor = CreateThreadPoolManagedExecutor(1);

        // Create topic client.
        NYdb::NTopic::TTopicClient topicClient(setup->GetDriver());

        NThreading::TPromise<void> promiseToWrite = NThreading::NewPromise<void>();
        auto futureWrite = promiseToWrite.GetFuture();

        NThreading::TPromise<void> promiseToRead = NThreading::NewPromise<void>();
        auto futureRead = promiseToRead.GetFuture();

        NYdb::NTopic::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic())
                     .MessageGroupId("src_id")
                     .ProducerId("src_id")
                     .CompressionExecutor(stepByStepExecutor);

        // Create read session.
        NYdb::NTopic::TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(setup->GetTestConsumer())
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic())
            .DecompressionExecutor(stepByStepExecutor);

        auto f = std::async(std::launch::async,
                            [readSettings, writeSettings, &topicClient,
                             promiseToWrite = std::move(promiseToWrite),
                             promiseToRead = std::move(promiseToRead)]() mutable {
            {
                auto writeSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);
                std::string message(2'000, 'x');
                bool res = writeSession->Write(message);
                UNIT_ASSERT(res);
                writeSession->Close(TDuration::Seconds(10));
            }
            promiseToWrite.SetValue();
            Cerr << ">>>TEST: write promise set " << Endl;

            {
                NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
                auto future = promise.GetFuture();

                readSettings.EventHandlers_.SimpleDataHandlers(
                    [promise = std::move(promise)](NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& ev) mutable {
                    ev.Commit();
                    promise.SetValue();
                    Cerr << ">>>TEST: get read event " << Endl;
                });

                auto readSession = topicClient.CreateReadSession(readSettings);
                future.Wait();
                readSession->Close(TDuration::Seconds(10));
            }
            promiseToRead.SetValue();
            Cerr << ">>>TEST: read promise set " << Endl;
        });


        //
        // auxiliary functions for decompressor and handler control
        //
        auto WaitTasks = [&](auto f, size_t c) {
            while (f() < c) {
                Sleep(TDuration::MilliSeconds(100));
            };
        };
        auto WaitPlannedTasks = [&](auto e, size_t count) {
            WaitTasks([&]() { return e->GetPlannedCount(); }, count);
        };
        auto WaitExecutedTasks = [&](auto e, size_t count) {
            WaitTasks([&]() { return e->GetExecutedCount(); }, count);
        };

        auto RunTasks = [&](auto e, const std::vector<size_t>& tasks) {
            size_t n = tasks.size();
            Cerr << ">>>TEST in RunTasks: before WaitPlannedTasks" << Endl;
            WaitPlannedTasks(e, n);
            Cerr << ">>>TEST in RunTasks: before WaitExecutedTasks" << Endl;
            size_t completed = e->GetExecutedCount();
            e->StartFuncs(tasks);
            WaitExecutedTasks(e, completed + n);
        };

        UNIT_ASSERT(!futureWrite.HasValue());
        Cerr << ">>>TEST: future write has no value " << Endl;
        RunTasks(stepByStepExecutor, {0});
        futureWrite.GetValueSync();
        UNIT_ASSERT(futureWrite.HasValue());
        Cerr << ">>>TEST: future write has value " << Endl;

        UNIT_ASSERT(!futureRead.HasValue());
        Cerr << ">>>TEST: future read has no value " << Endl;
        RunTasks(stepByStepExecutor, {1});
        futureRead.GetValueSync();
        UNIT_ASSERT(futureRead.HasValue());
        Cerr << ">>>TEST: future read has value " << Endl;

        f.get();

        Cerr << ">>> TEST: gracefully closed" << Endl;
    }

    Y_UNIT_TEST(SessionNotDestroyedWhileUserEventHandlingInFlight) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);

        // controlled executor
        auto stepByStepExecutor = CreateThreadPoolManagedExecutor(1);

        // Create topic client.
        NYdb::NTopic::TTopicClient topicClient(setup->GetDriver());

        // NThreading::TPromise<void> promiseToWrite = NThreading::NewPromise<void>();
        // auto futureWrite = promiseToWrite.GetFuture();

        NThreading::TPromise<void> promiseToRead = NThreading::NewPromise<void>();
        auto futureRead = promiseToRead.GetFuture();

        auto writeSettings = TWriteSessionSettings()
            .Path(setup->GetTestTopic())
            .MessageGroupId("src_id")
            .ProducerId("src_id");

        auto writeSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);
        std::string message(2'000, 'x');
        bool res = writeSession->Write(message);
        UNIT_ASSERT(res);
        writeSession->Close(TDuration::Seconds(10));

        // writeSettings.EventHandlers_
        //     .HandlersExecutor(stepByStepExecutor);

        // Create read session.
        auto readSettings = TReadSessionSettings()
            .ConsumerName(setup->GetTestConsumer())
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        readSettings.EventHandlers_
            .HandlersExecutor(stepByStepExecutor);

        auto f = std::async(std::launch::async,
                            [readSettings, /*writeSettings,*/ &topicClient,
                            //  promiseToWrite = std::move(promiseToWrite),
                             promiseToRead = std::move(promiseToRead)]() mutable {
            // {
            //     std::shared_ptr<TContinuationToken> token;
            //     writeSettings.EventHandlers_.CommonHandler([token](TWriteSessionEvent::TEvent& event){
            //         Cerr << ">>>TEST: in CommonHandler " << Endl;

            //         if (std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event)) {
            //             *token = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
            //         }
            //     });

            //     auto writeSession = topicClient.CreateWriteSession(writeSettings);
            //     std::string message(2'000, 'x');
            //     writeSession->WaitEvent().Wait();
            //     writeSession->Write(std::move(*token), message);
            //     writeSession->WaitEvent().Wait();
            //     writeSession->Close(TDuration::Seconds(10));
            // }
            // promiseToWrite.SetValue();
            // Cerr << ">>>TEST: write promise set " << Endl;

            {
                NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
                auto future = promise.GetFuture();

                readSettings.EventHandlers_.SimpleDataHandlers(
                    [promise = std::move(promise)](NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& ev) mutable {
                    Cerr << ">>>TEST: in SimpleDataHandlers " << Endl;
                    ev.Commit();
                    promise.SetValue();
                });

                auto readSession = topicClient.CreateReadSession(readSettings);
                future.Wait();
                readSession->Close(TDuration::Seconds(10));
            }
            promiseToRead.SetValue();
            Cerr << ">>>TEST: read promise set " << Endl;
        });


        //
        // auxiliary functions for decompressor and handler control
        //
        auto WaitTasks = [&](auto f, size_t c) {
            while (f() < c) {
                Sleep(TDuration::MilliSeconds(100));
            };
        };
        auto WaitPlannedTasks = [&](auto e, size_t count) {
            WaitTasks([&]() { return e->GetPlannedCount(); }, count);
        };
        auto WaitExecutedTasks = [&](auto e, size_t count) {
            WaitTasks([&]() { return e->GetExecutedCount(); }, count);
        };

        auto RunTasks = [&](auto e, const std::vector<size_t>& tasks) {
            size_t n = tasks.size();
            Cerr << ">>>TEST in RunTasks: before WaitPlannedTasks" << Endl;
            WaitPlannedTasks(e, n);
            Cerr << ">>>TEST in RunTasks: before WaitExecutedTasks" << Endl;
            size_t completed = e->GetExecutedCount();
            e->StartFuncs(tasks);
            WaitExecutedTasks(e, completed + n);
        };

        // RunTasks(stepByStepExecutor, {0});
        // UNIT_ASSERT(!futureWrite.HasValue());
        // Cerr << ">>>TEST: future write has no value " << Endl;
        // RunTasks(stepByStepExecutor, {1});
        // futureWrite.GetValueSync();
        // UNIT_ASSERT(futureWrite.HasValue());
        // Cerr << ">>>TEST: future write has value " << Endl;

        UNIT_ASSERT(!futureRead.HasValue());
        Cerr << ">>>TEST: future read has no value " << Endl;
        // 0: TStartPartitionSessionEvent
        RunTasks(stepByStepExecutor, {0});
        // 1: TDataReceivedEvent
        RunTasks(stepByStepExecutor, {1});
        futureRead.GetValueSync();
        UNIT_ASSERT(futureRead.HasValue());
        Cerr << ">>>TEST: future read has value " << Endl;

        f.get();

        Cerr << ">>> TEST: gracefully closed" << Endl;
    }

}

}
