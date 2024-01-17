#include "ut_utils/managed_executor.h"
#include "ut_utils/topic_sdk_test_setup.h"
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/write_session.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <future>

namespace NYdb::NTopic::NTests {

void WriteAndReadToEndWithRestarts(TReadSessionSettings readSettings, TWriteSessionSettings writeSettings, const std::string& message, ui32 count, TTopicSdkTestSetup& setup, TIntrusivePtr<TManagedExecutor> decompressor) {
    auto client = setup.MakeClient();
    auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

    for (ui32 i = 1; i <= count; ++i) {
        bool res = session->Write(message);
        UNIT_ASSERT(res);
    }
    bool res = session->Close(TDuration::Seconds(10));
    UNIT_ASSERT(res);

    std::shared_ptr<IReadSession> ReadSession;

    TTopicClient topicClient = setup.MakeClient();


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

        setup.GetServer().KillTopicPqrbTablet(setup.GetTopicPath());
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
        (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
        AtomicSet(lastOffset, ev.GetMessages().back().GetOffset());
        Cerr << ">>> TEST: last offset = " << lastOffset << Endl;
    });

    ReadSession = topicClient.CreateReadSession(readSettings);

    ui32 i = 0;
    while (AtomicGet(lastOffset) + 1 < count) {
        RunTasks(decompressor, {i++});
    }

    ReadSession->Close(TDuration::MilliSeconds(10));
}

Y_UNIT_TEST_SUITE(BasicUsage) {
    Y_UNIT_TEST(ConnectToYDB) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);

        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "invalid:" << setup.GetServer().GrpcPort);
        cfg.SetDatabase("/Invalid");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        auto driver = NYdb::TDriver(cfg);

        {
            TTopicClient client(driver);

            auto writeSettings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                // TODO why retries? see LOGBROKER-8490
                .RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
            auto writeSession = client.CreateWriteSession(writeSettings);

            auto event = writeSession->GetEvent(true);
            UNIT_ASSERT(event.Defined() && std::holds_alternative<TSessionClosedEvent>(event.GetRef()));
        }

        {
            auto settings = TTopicClientSettings()
                .Database({"/Root"})
                .DiscoveryEndpoint({TStringBuilder() << "localhost:" << setup.GetServer().GrpcPort});

            TTopicClient client(driver, settings);

            auto writeSettings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
            auto writeSession = client.CreateWriteSession(writeSettings);

            auto event = writeSession->GetEvent(true);
            UNIT_ASSERT(event.Defined() && !std::holds_alternative<TSessionClosedEvent>(event.GetRef()));
        }
    }


    Y_UNIT_TEST(WriteRead) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TTopicClient client = setup.MakeClient();

        for (size_t i = 0; i < 100; ++i) {
            auto writeSettings = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .ProducerId(TEST_MESSAGE_GROUP_ID)
                        .MessageGroupId(TEST_MESSAGE_GROUP_ID);
            Cerr << ">>> open write session " << i << Endl;
            auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
            UNIT_ASSERT(writeSession->Write("message_using_MessageGroupId"));
            Cerr << ">>> write session " << i << " message written" << Endl;
            writeSession->Close();
            Cerr << ">>> write session " << i << " closed" << Endl;
        }
        {
            auto writeSettings = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .ProducerId(TEST_MESSAGE_GROUP_ID)
                        .PartitionId(0);
            Cerr << ">>> open write session 100" << Endl;
            auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
            UNIT_ASSERT(writeSession->Write("message_using_PartitionId"));
            Cerr << ">>> write session 100 message written" << Endl;
            writeSession->Close();
            Cerr << ">>> write session 100 closed" << Endl;
        }

        {
            auto readSettings = TReadSessionSettings()
                .ConsumerName(TEST_CONSUMER)
                .AppendTopics(TEST_TOPIC);
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
            UNIT_ASSERT(messages.size() == 101);
            UNIT_ASSERT(messages[0].GetData() == "message_using_MessageGroupId");
            UNIT_ASSERT(messages[100].GetData() == "message_using_PartitionId");
        }
    }

    Y_UNIT_TEST(ReadWithoutConsumerWithRestarts) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto compressor = new TSyncExecutor();
        auto decompressor = CreateThreadPoolManagedExecutor(1);

        TReadSessionSettings readSettings;
        TTopicReadSettings topic = TEST_TOPIC;
        topic.AppendPartitionIds(0);
        readSettings
            .WithoutConsumer()
            .MaxMemoryUsageBytes(1_MB)
            .DecompressionExecutor(decompressor)
            .AppendTopics(topic);

        TWriteSessionSettings writeSettings;
        writeSettings
            .Path(TEST_TOPIC)
            .MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .Codec(NTopic::ECodec::RAW)
            .CompressionExecutor(compressor);


        ui32 count = 700;
        std::string message(2'000, 'x');

        WriteAndReadToEndWithRestarts(readSettings, writeSettings, message, count, setup, decompressor);
    }

    Y_UNIT_TEST(MaxByteSizeEqualZero) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TTopicClient client = setup.MakeClient();

        auto writeSettings = TWriteSessionSettings()
            .Path(TEST_TOPIC)
            .MessageGroupId(TEST_MESSAGE_GROUP_ID);
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
        UNIT_ASSERT(writeSession->Write("message"));
        writeSession->Close();

        auto readSettings = TReadSessionSettings()
            .ConsumerName(TEST_CONSUMER)
            .AppendTopics(TEST_TOPIC);
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
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId(TEST_MESSAGE_GROUP_ID);
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

        std::shared_ptr<IReadSession> ReadSession;

        // Create topic client.
        NYdb::NTopic::TTopicClient topicClient(setup->GetDriver());

        // Create read session.
        TReadSessionSettings readSettings;
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
            (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
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
        auto result = topicClient.DescribeConsumer(setup->GetTestTopicPath(), setup->GetTestConsumer(), describeConsumerSettings).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto description = result.GetConsumerDescription();
        UNIT_ASSERT(description.GetPartitions().size() == 1);
        auto stats = description.GetPartitions().front().GetPartitionConsumerStats();
        UNIT_ASSERT(stats.Defined());
        UNIT_ASSERT(stats->GetCommittedOffset() == 50);
    }


    Y_UNIT_TEST(ReadWithRestarts) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        auto compressor = new TSyncExecutor();
        auto decompressor = CreateThreadPoolManagedExecutor(1);

        TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(TEST_CONSUMER)
            .MaxMemoryUsageBytes(1_MB)
            .DecompressionExecutor(decompressor)
            .AppendTopics(TEST_TOPIC);

        TWriteSessionSettings writeSettings;
        writeSettings
            .Path(TEST_TOPIC).MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .Codec(NTopic::ECodec::RAW)
            .CompressionExecutor(compressor);


        ui32 count = 700;
        std::string message(2'000, 'x');

        WriteAndReadToEndWithRestarts(readSettings, writeSettings, message, count, setup, decompressor);
    }

    Y_UNIT_TEST(SessionNotDestroyedWhileCompressionInFlight) {
        TTopicSdkTestSetup setup(TEST_CASE_NAME);

        // controlled executor
        auto stepByStepExecutor = CreateThreadPoolManagedExecutor(1);

        // Create topic client.
        TTopicClient topicClient = setup.MakeClient();

        NThreading::TPromise<void> promiseToWrite = NThreading::NewPromise<void>();
        auto futureWrite = promiseToWrite.GetFuture();

        NThreading::TPromise<void> promiseToRead = NThreading::NewPromise<void>();
        auto futureRead = promiseToRead.GetFuture();

        TWriteSessionSettings writeSettings;
        writeSettings.Path(TEST_TOPIC)
                     .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                     .ProducerId(TEST_MESSAGE_GROUP_ID)
                     .CompressionExecutor(stepByStepExecutor);

        // Create read session.
        TReadSessionSettings readSettings;
        readSettings
            .ConsumerName(TEST_CONSUMER)
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(TEST_TOPIC)
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
                    [promise = std::move(promise)](TReadSessionEvent::TDataReceivedEvent& ev) mutable {
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
        TTopicSdkTestSetup setup(TEST_CASE_NAME);

        // controlled executor
        auto stepByStepExecutor = CreateThreadPoolManagedExecutor(1);

        // Create topic client.
        TTopicClient topicClient = setup.MakeClient();

        // NThreading::TPromise<void> promiseToWrite = NThreading::NewPromise<void>();
        // auto futureWrite = promiseToWrite.GetFuture();

        NThreading::TPromise<void> promiseToRead = NThreading::NewPromise<void>();
        auto futureRead = promiseToRead.GetFuture();

        auto writeSettings = TWriteSessionSettings()
            .Path(TEST_TOPIC)
            .MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .ProducerId(TEST_MESSAGE_GROUP_ID);

        auto writeSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);
        std::string message(2'000, 'x');
        bool res = writeSession->Write(message);
        UNIT_ASSERT(res);
        writeSession->Close(TDuration::Seconds(10));

        // writeSettings.EventHandlers_
        //     .HandlersExecutor(stepByStepExecutor);

        // Create read session.
        auto readSettings = TReadSessionSettings()
            .ConsumerName(TEST_CONSUMER)
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(TEST_TOPIC);

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
                    [promise = std::move(promise)](TReadSessionEvent::TDataReceivedEvent& ev) mutable {
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

    Y_UNIT_TEST(ReadSessionCorrectClose) {

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);

        NPersQueue::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        writeSettings.Codec(NPersQueue::ECodec::RAW);
        NPersQueue::IExecutor::TPtr executor = new NPersQueue::TSyncExecutor();
        writeSettings.CompressionExecutor(executor);

        auto& client = setup->GetPersQueueClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

        ui32 count = 7000;
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
            .Decompress(false)
            .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy())
            .AppendTopics(setup->GetTestTopic());

        readSettings.EventHandlers_.SimpleDataHandlers(
            []
            (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& ev) mutable {
                Cerr << ">>> Got TDataReceivedEvent" << Endl;
                ev.Commit();
        });

        Cerr << ">>> TEST: Create session" << Endl;

        ReadSession = topicClient.CreateReadSession(readSettings);

        Sleep(TDuration::MilliSeconds(50));

        ReadSession->Close();
        ReadSession = nullptr;
        Cerr << ">>> TEST: Session gracefully closed" << Endl;

        Sleep(TDuration::Seconds(5));

        // UNIT_ASSERT(false);
    }

    Y_UNIT_TEST(ConflictingWrites) {

        TTopicSdkTestSetup setup(TEST_CASE_NAME);

        NTopic::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup.GetTopicPath()).MessageGroupId(TEST_MESSAGE_GROUP_ID);
        writeSettings.Path(setup.GetTopicPath()).ProducerId(TEST_MESSAGE_GROUP_ID);
        writeSettings.Codec(NTopic::ECodec::RAW);
        NTopic::IExecutor::TPtr executor = new NTopic::TSyncExecutor();
        writeSettings.CompressionExecutor(executor);

        ui64 count = 100u;

        auto client = setup.MakeClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

        TString messageBase = "message----";

        for (auto i = 0u; i < count; i++) {
            auto res = session->Write(messageBase);
            UNIT_ASSERT(res);
            if (i % 10 == 0) {
                setup.GetServer().KillTopicPqTablets(setup.GetTopicPath());
            }
        }
        session->Close();

        auto describeTopicSettings = TDescribeTopicSettings().IncludeStats(true);
        auto result = client.DescribeTopic(setup.GetTopicPath(), describeTopicSettings).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto description = result.GetTopicDescription();
        UNIT_ASSERT(description.GetPartitions().size() == 1);
        auto stats = description.GetPartitions().front().GetPartitionStats();
        UNIT_ASSERT(stats.Defined());
        UNIT_ASSERT_VALUES_EQUAL(stats->GetEndOffset(), count);

    }



}

}
