#include "setup/fixture.h"

#include "utils/managed_executor.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/impl/write_session.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/executor_impl.h>

#include <util/generic/overloaded.h>
#include <util/stream/zlib.h>

#include <gtest/gtest.h>

#include <future>


namespace NYdb::inline Dev::NPersQueue::NTests {

class TSimpleWriteSessionTestAdapter {
public:
    TSimpleWriteSessionTestAdapter(NPersQueue::TSimpleBlockingWriteSession* session);
    std::uint64_t GetAcquiredMessagesCount() const;

private:
    NPersQueue::TSimpleBlockingWriteSession* Session;
};

TSimpleWriteSessionTestAdapter::TSimpleWriteSessionTestAdapter(NPersQueue::TSimpleBlockingWriteSession* session)
    : Session(session)
{}

std::uint64_t TSimpleWriteSessionTestAdapter::GetAcquiredMessagesCount() const {
    if (Session->Writer)
        return Session->Writer->TryGetImpl()->MessagesAcquired;
    else
        return 0;
}

}

namespace NYdb::inline Dev::NTopic::NTests {

class BasicUsage : public TTopicTestFixture {};

TEST_F(BasicUsage, TEST_NAME(ConnectToYDB)) {
    auto cfg = NYdb::TDriverConfig()
        .SetEndpoint("invalid:2136")
        .SetDatabase("/Invalid")
        .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG).Release()));
    auto driver = NYdb::TDriver(cfg);

    {
        TTopicClient client(driver);

        auto writeSettings = TWriteSessionSettings()
            .Path(GetTopicPath())
            .MessageGroupId(TEST_MESSAGE_GROUP_ID)
            // TODO why retries? see LOGBROKER-8490
            .RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
        auto writeSession = client.CreateWriteSession(writeSettings);

        auto event = writeSession->GetEvent(true);
        ASSERT_TRUE(event && std::holds_alternative<TSessionClosedEvent>(event.value()));
    }

    {
        auto settings = TTopicClientSettings()
            .Database(std::getenv("YDB_DATABASE"))
            .DiscoveryEndpoint(std::getenv("YDB_ENDPOINT"));

        TTopicClient client(driver, settings);

        auto writeSettings = TWriteSessionSettings()
            .Path(GetTopicPath())
            .MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
        auto writeSession = client.CreateWriteSession(writeSettings);

        auto event = writeSession->GetEvent(true);
        ASSERT_TRUE(event && !std::holds_alternative<TSessionClosedEvent>(event.value()));
    }
}

TEST_F(BasicUsage, TEST_NAME(WriteRead)) {
    auto driver = MakeDriver();

    TTopicClient client(driver);

    for (size_t i = 0; i < 100; ++i) {
        auto writeSettings = TWriteSessionSettings()
                    .Path(GetTopicPath())
                    .ProducerId(TEST_MESSAGE_GROUP_ID)
                    .MessageGroupId(TEST_MESSAGE_GROUP_ID);
        std::cerr << ">>> open write session " << i << std::endl;
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
        ASSERT_TRUE(writeSession->Write("message_using_MessageGroupId"));
        std::cerr << ">>> write session " << i << " message written" << std::endl;
        writeSession->Close();
        std::cerr << ">>> write session " << i << " closed" << std::endl;
    }
    {
        auto writeSettings = TWriteSessionSettings()
                    .Path(GetTopicPath())
                    .ProducerId(TEST_MESSAGE_GROUP_ID)
                    .PartitionId(0);
        std::cerr << ">>> open write session 100" << std::endl;
        auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
        ASSERT_TRUE(writeSession->Write("message_using_PartitionId"));
        std::cerr << ">>> write session 100 message written" << std::endl;
        writeSession->Close();
        std::cerr << ">>> write session 100 closed" << std::endl;
    }

    {
        auto readSettings = TReadSessionSettings()
            .ConsumerName(GetConsumerName())
            .AppendTopics(GetTopicPath())
            // .DirectRead(EnableDirectRead)
            ;
        auto readSession = client.CreateReadSession(readSettings);

        auto event = readSession->GetEvent(true);
        ASSERT_TRUE(event.has_value());

        auto& startPartitionSession = std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event);
        startPartitionSession.Confirm();

        event = readSession->GetEvent(true);
        ASSERT_TRUE(event.has_value());

        auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
        dataReceived.Commit();

        auto& messages = dataReceived.GetMessages();
        ASSERT_EQ(messages.size(), 101u);
        ASSERT_EQ(messages[0].GetData(), "message_using_MessageGroupId");
        ASSERT_EQ(messages[100].GetData(), "message_using_PartitionId");
    }
}

TEST_F(BasicUsage, TEST_NAME(MaxByteSizeEqualZero)) {
    auto driver = MakeDriver();

    TTopicClient client(driver);

    auto writeSettings = TWriteSessionSettings()
        .Path(GetTopicPath())
        .MessageGroupId(TEST_MESSAGE_GROUP_ID);
    auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
    ASSERT_TRUE(writeSession->Write("message"));
    writeSession->Close();

    auto readSettings = TReadSessionSettings()
        .ConsumerName(GetConsumerName())
        .AppendTopics(GetTopicPath())
        // .DirectRead(EnableDirectRead)
        ;
    auto readSession = client.CreateReadSession(readSettings);

    auto event = readSession->GetEvent(true);
    ASSERT_TRUE(event.has_value());

    auto& startPartitionSession = std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event);
    startPartitionSession.Confirm();

    ASSERT_THROW(readSession->GetEvent(true, 0), TContractViolation);
    ASSERT_THROW(readSession->GetEvents(true, std::nullopt, 0), TContractViolation);

    event = readSession->GetEvent(true, 1);
    ASSERT_TRUE(event.has_value());

    auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
    dataReceived.Commit();
}

TEST_F(BasicUsage, TEST_NAME(WriteAndReadSomeMessagesWithSyncCompression)) {
    auto driver = MakeDriver();

    IExecutor::TPtr executor = new TSyncExecutor();
    auto writeSettings = NPersQueue::TWriteSessionSettings()
        .Path(GetTopicPath())
        .MessageGroupId(TEST_MESSAGE_GROUP_ID)
        .Codec(NPersQueue::ECodec::RAW)
        .CompressionExecutor(executor);

    std::uint64_t count = 100u;
    std::optional<bool> shouldCaptureData = {true};

    NPersQueue::TPersQueueClient client(driver);
    auto session = client.CreateSimpleBlockingWriteSession(writeSettings);
    std::string messageBase = "message----";
    std::vector<std::string> sentMessages;

    for (auto i = 0u; i < count; i++) {
        // sentMessages.emplace_back(messageBase * (i+1) + ToString(i));
        std::ostringstream os;
        for(int i = 0; i < 200 * 1024; i++) {
            os << messageBase;
        }
        sentMessages.emplace_back(os.str());
        auto res = session->Write(sentMessages.back());
        ASSERT_TRUE(res);
    }
    {
        auto sessionAdapter = NPersQueue::NTests::TSimpleWriteSessionTestAdapter(
                dynamic_cast<NPersQueue::TSimpleBlockingWriteSession *>(session.get()));
        if (shouldCaptureData.has_value()) {
            std::ostringstream msg;
            msg << "Session has captured " << sessionAdapter.GetAcquiredMessagesCount()
                << " messages, capturing was expected: " << *shouldCaptureData << std::endl;
            ASSERT_TRUE(sessionAdapter.GetAcquiredMessagesCount() > 0) << msg.str();
        }
    }
    session->Close();

    std::shared_ptr<IReadSession> ReadSession;

    // Create topic client.
    TTopicClient topicClient(driver);

    // Create read session.
    TReadSessionSettings readSettings;
    readSettings
        .ConsumerName(GetConsumerName())
        .MaxMemoryUsageBytes(1_MB)
        .AppendTopics(GetTopicPath())
        // .DirectRead(EnableDirectRead)
        ;

    std::cerr << "Session was created" << std::endl;

    NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
    auto totalReceived = 0u;

    auto f = checkedPromise.GetFuture();
    std::atomic<int> check = 1;
    readSettings.EventHandlers_.SimpleDataHandlers(
        // [checkedPromise = std::move(checkedPromise), &check, &sentMessages, &totalReceived]
        [&]
        (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
        Y_ABORT_UNLESS(check.load() != 0, "check is false");
        auto& messages = ev.GetMessages();
        for (size_t i = 0u; i < messages.size(); ++i) {
            auto& message = messages[i];
            ASSERT_EQ(message.GetData(), sentMessages[totalReceived]);
            totalReceived++;
        }
        if (totalReceived == sentMessages.size())
            checkedPromise.SetValue();
    });

    ReadSession = topicClient.CreateReadSession(readSettings);

    f.GetValueSync();
    ReadSession->Close(TDuration::MilliSeconds(10));
    check.store(0);

    auto status = topicClient.CommitOffset(GetTopicPath(), 0, GetConsumerName(), 50);
    ASSERT_TRUE(status.GetValueSync().IsSuccess());

    auto describeConsumerSettings = TDescribeConsumerSettings().IncludeStats(true);
    auto result = topicClient.DescribeConsumer(GetTopicPath(), GetConsumerName(), describeConsumerSettings).GetValueSync();
    ASSERT_TRUE(result.IsSuccess());

    auto description = result.GetConsumerDescription();
    ASSERT_EQ(description.GetPartitions().size(), 1u);
    auto stats = description.GetPartitions().front().GetPartitionConsumerStats();
    ASSERT_TRUE(stats.has_value());
    ASSERT_EQ(stats->GetCommittedOffset(), 50u);
}

TEST_F(BasicUsage, TEST_NAME(SessionNotDestroyedWhileCompressionInFlight)) {
    auto driver = MakeDriver();

    // controlled executor
    auto stepByStepExecutor = CreateThreadPoolManagedExecutor(1);

    // Create topic client.
    TTopicClient topicClient(driver);

    NThreading::TPromise<void> promiseToWrite = NThreading::NewPromise<void>();
    auto futureWrite = promiseToWrite.GetFuture();

    NThreading::TPromise<void> promiseToRead = NThreading::NewPromise<void>();
    auto futureRead = promiseToRead.GetFuture();

    TWriteSessionSettings writeSettings;
    writeSettings.Path(GetTopicPath())
                    .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                    .ProducerId(TEST_MESSAGE_GROUP_ID)
                    .CompressionExecutor(stepByStepExecutor);

    // Create read session.
    TReadSessionSettings readSettings;
    readSettings
        .ConsumerName(GetConsumerName())
        .MaxMemoryUsageBytes(1_MB)
        .AppendTopics(GetTopicPath())
        .DecompressionExecutor(stepByStepExecutor)
        // .DirectRead(EnableDirectRead)
        ;

    auto f = std::async(std::launch::async,
                        [readSettings, writeSettings, &topicClient,
                            promiseToWrite = std::move(promiseToWrite),
                            promiseToRead = std::move(promiseToRead)]() mutable {
        {
            auto writeSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);
            std::string message(2'000, 'x');
            bool res = writeSession->Write(message);
            ASSERT_TRUE(res);
            writeSession->Close(TDuration::Seconds(10));
        }
        promiseToWrite.SetValue();
        std::cerr << ">>>TEST: write promise set " << std::endl;

        {
            NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
            auto future = promise.GetFuture();

            readSettings.EventHandlers_.SimpleDataHandlers(
                [promise = std::move(promise)](TReadSessionEvent::TDataReceivedEvent& ev) mutable {
                ev.Commit();
                promise.SetValue();
                std::cerr << ">>>TEST: get read event " << std::endl;
            });

            auto readSession = topicClient.CreateReadSession(readSettings);
            future.Wait();
            readSession->Close(TDuration::Seconds(10));
        }
        promiseToRead.SetValue();
        std::cerr << ">>>TEST: read promise set " << std::endl;
    });


    //
    // auxiliary functions for decompressor and handler control
    //
    auto WaitTasks = [&](auto f, size_t c) {
        while (f() < c) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
        std::cerr << ">>>TEST in RunTasks: before WaitPlannedTasks" << std::endl;
        WaitPlannedTasks(e, n);
        std::cerr << ">>>TEST in RunTasks: before WaitExecutedTasks" << std::endl;
        size_t completed = e->GetExecutedCount();
        e->StartFuncs(tasks);
        WaitExecutedTasks(e, completed + n);
    };

    ASSERT_FALSE(futureWrite.HasValue());
    std::cerr << ">>>TEST: future write has no value " << std::endl;
    RunTasks(stepByStepExecutor, {0});  // Run compression task.
    RunTasks(stepByStepExecutor, {1});  // Run send task.
    futureWrite.GetValueSync();
    ASSERT_TRUE(futureWrite.HasValue());
    std::cerr << ">>>TEST: future write has value " << std::endl;

    ASSERT_FALSE(futureRead.HasValue());
    std::cerr << ">>>TEST: future read has no value " << std::endl;
    RunTasks(stepByStepExecutor, {2}); // Run decompression task.
    futureRead.GetValueSync();
    ASSERT_TRUE(futureRead.HasValue());
    std::cerr << ">>>TEST: future read has value " << std::endl;

    f.get();

    std::cerr << ">>> TEST: gracefully closed" << std::endl;
}

TEST_F(BasicUsage, TEST_NAME(SessionNotDestroyedWhileUserEventHandlingInFlight)) {
    auto driver = MakeDriver();

    // controlled executor
    auto stepByStepExecutor = CreateThreadPoolManagedExecutor(1);

    // Create topic client.
    TTopicClient topicClient(driver);

    // NThreading::TPromise<void> promiseToWrite = NThreading::NewPromise<void>();
    // auto futureWrite = promiseToWrite.GetFuture();

    NThreading::TPromise<void> promiseToRead = NThreading::NewPromise<void>();
    auto futureRead = promiseToRead.GetFuture();

    auto writeSettings = TWriteSessionSettings()
        .Path(GetTopicPath())
        .MessageGroupId(TEST_MESSAGE_GROUP_ID)
        .ProducerId(TEST_MESSAGE_GROUP_ID);

    auto writeSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);
    std::string message(2'000, 'x');
    bool res = writeSession->Write(message);
    ASSERT_TRUE(res);
    writeSession->Close(TDuration::Seconds(10));

    // writeSettings.EventHandlers_
    //     .HandlersExecutor(stepByStepExecutor);

    // Create read session.
    auto readSettings = TReadSessionSettings()
        .ConsumerName(GetConsumerName())
        .MaxMemoryUsageBytes(1_MB)
        .AppendTopics(GetTopicPath())
        // .DirectRead(EnableDirectRead)
        ;

    readSettings.EventHandlers_
        .HandlersExecutor(stepByStepExecutor);

    auto f = std::async(std::launch::async,
                        [readSettings, /*writeSettings,*/ &topicClient,
                        //  promiseToWrite = std::move(promiseToWrite),
                            promiseToRead = std::move(promiseToRead)]() mutable {
        // {
        //     std::shared_ptr<TContinuationToken> token;
        //     writeSettings.EventHandlers_.CommonHandler([token](TWriteSessionEvent::TEvent& event){
        //         std::cerr << ">>>TEST: in CommonHandler " << std::endl;

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
        // std::cerr << ">>>TEST: write promise set " << std::endl;

        {
            NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
            auto future = promise.GetFuture();

            readSettings.EventHandlers_.SimpleDataHandlers(
                [promise = std::move(promise)](TReadSessionEvent::TDataReceivedEvent& ev) mutable {
                std::cerr << ">>>TEST: in SimpleDataHandlers " << std::endl;
                ev.Commit();
                promise.SetValue();
            });

            auto readSession = topicClient.CreateReadSession(readSettings);
            future.Wait();
            readSession->Close(TDuration::Seconds(10));
        }
        promiseToRead.SetValue();
        std::cerr << ">>>TEST: read promise set " << std::endl;
    });


    //
    // auxiliary functions for decompressor and handler control
    //
    auto WaitTasks = [&](auto f, size_t c) {
        while (f() < c) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
        std::cerr << ">>>TEST in RunTasks: before WaitPlannedTasks" << std::endl;
        WaitPlannedTasks(e, n);
        std::cerr << ">>>TEST in RunTasks: before WaitExecutedTasks" << std::endl;
        size_t completed = e->GetExecutedCount();
        e->StartFuncs(tasks);
        WaitExecutedTasks(e, completed + n);
    };

    // RunTasks(stepByStepExecutor, {0});
    // UNIT_ASSERT(!futureWrite.HasValue());
    // std::cerr << ">>>TEST: future write has no value " << std::endl;
    // RunTasks(stepByStepExecutor, {1});
    // futureWrite.GetValueSync();
    // UNIT_ASSERT(futureWrite.HasValue());
    // std::cerr << ">>>TEST: future write has value " << std::endl;

    ASSERT_FALSE(futureRead.HasValue());
    std::cerr << ">>>TEST: future read has no value " << std::endl;
    // 0: TStartPartitionSessionEvent
    RunTasks(stepByStepExecutor, {0});
    // 1: TDataReceivedEvent
    RunTasks(stepByStepExecutor, {1});
    futureRead.GetValueSync();
    ASSERT_TRUE(futureRead.HasValue());
    std::cerr << ">>>TEST: future read has value " << std::endl;

    f.get();

    std::cerr << ">>> TEST: gracefully closed" << std::endl;
}

TEST_F(BasicUsage, TEST_NAME(ReadSessionCorrectClose)) {
    auto driver = MakeDriver();

    NPersQueue::TWriteSessionSettings writeSettings;
    writeSettings.Path(GetTopicPath()).MessageGroupId(TEST_MESSAGE_GROUP_ID);
    writeSettings.Codec(NPersQueue::ECodec::RAW);
    IExecutor::TPtr executor = new TSyncExecutor();
    writeSettings.CompressionExecutor(executor);

    NPersQueue::TPersQueueClient client(driver);
    auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

    std::uint32_t count = 7000;
    std::string message(2'000, 'x');
    for (std::uint32_t i = 1; i <= count; ++i) {
        bool res = session->Write(message);
        ASSERT_TRUE(res);
    }
    bool res = session->Close(TDuration::Seconds(30));
    ASSERT_TRUE(res);

    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;

    // Create topic client.
    TTopicClient topicClient(driver);

    // Create read session.
    NYdb::NTopic::TReadSessionSettings readSettings;
    readSettings
        .ConsumerName(GetConsumerName())
        .MaxMemoryUsageBytes(1_MB)
        .Decompress(false)
        .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy())
        .AppendTopics(GetTopicPath())
        // .DirectRead(EnableDirectRead)
        ;

    readSettings.EventHandlers_.SimpleDataHandlers(
        []
        (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& ev) mutable {
            std::cerr << ">>> Got TDataReceivedEvent" << std::endl;
            ev.Commit();
    });

    std::cerr << ">>> TEST: Create session" << std::endl;

    ReadSession = topicClient.CreateReadSession(readSettings);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    ReadSession->Close();
    ReadSession = nullptr;
    std::cerr << ">>> TEST: Session gracefully closed" << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(5));
}

TEST_F(BasicUsage, TEST_NAME(ConfirmPartitionSessionWithCommitOffset)) {
    // TStartPartitionSessionEvent::Confirm(readOffset, commitOffset) should work,
    // if commitOffset passed to Confirm is greater than the offset committed previously by the consumer.
    // https://st.yandex-team.ru/KIKIMR-23015

    auto driver = MakeDriver();

    {
        // Write 2 messages:
        auto settings = NTopic::TWriteSessionSettings()
            .Path(GetTopicPath())
            .MessageGroupId(TEST_MESSAGE_GROUP_ID)
            .ProducerId(TEST_MESSAGE_GROUP_ID);
        TTopicClient client(driver);
        auto writer = client.CreateSimpleBlockingWriteSession(settings);
        writer->Write("message");
        writer->Write("message");
        writer->Close();
    }

    {
        // Read messages:
        auto settings = NTopic::TReadSessionSettings()
            .ConsumerName(GetConsumerName())
            .AppendTopics(GetTopicPath())
            // .DirectRead(EnableDirectRead)
            ;

        TTopicClient client(driver);
        auto reader = client.CreateReadSession(settings);

        {
            // Start partition session and request to read from offset 1 and commit offset 1:
            auto event = reader->GetEvent(true);
            ASSERT_TRUE(event.has_value());
            ASSERT_TRUE(std::holds_alternative<TReadSessionEvent::TStartPartitionSessionEvent>(*event));
            auto& startPartitionSession = std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event);
            startPartitionSession.Confirm(/*readOffset=*/ 1, /*commitOffset=*/ 1);
        }

        {
            // Receive a message with offset 1 and commit it:
            auto event = reader->GetEvent(true);
            ASSERT_TRUE(event.has_value());
            ASSERT_TRUE(std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(*event));
            auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);

            // Here we should commit range [1, 2), not [0, 2):
            dataReceived.Commit();
        }

        {
            // And then get a TCommitOffsetAcknowledgementEvent:
            auto event = reader->GetEvent(true);
            ASSERT_TRUE(event.has_value());
            ASSERT_TRUE(std::holds_alternative<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(*event));
        }
    }
}

TEST_F(BasicUsage, TEST_NAME(TWriteSession_WriteEncoded)) {
    // This test was adapted from ydb_persqueue tests.
    // It writes 4 messages: 2 with default codec, 1 with explicitly set GZIP codec, 1 with RAW codec.
    // The last message MUST be sent in a separate WriteRequest, as it has a codec field applied for all messages in the request.
    // This separation currently happens in TWriteSessionImpl::SendImpl method.

    auto driver = MakeDriver();

    TTopicClient client(driver);

    auto settings = TWriteSessionSettings()
        .Path(GetTopicPath())
        .MessageGroupId(TEST_MESSAGE_GROUP_ID);

    size_t batchSize = 100000000;
    settings.BatchFlushInterval(TDuration::Seconds(1000)); // Batch on size, not on time.
    settings.BatchFlushSizeBytes(batchSize);
    auto writer = client.CreateWriteSession(settings);
    std::string message = "message";
    TString packed;
    {
        TStringOutput so(packed);
        TZLibCompress oss(&so, ZLib::GZip, 6);
        oss << message;
    }

    std::cerr << message << " " << packed << "\n";

    {
        auto event = *writer->GetEvent(true);
        ASSERT_FALSE(writer->WaitEvent().Wait(TDuration::Seconds(1)));
        auto ev = writer->WaitEvent();
        ASSERT_TRUE(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
        auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
        writer->Write(std::move(continueToken), message);
        ASSERT_TRUE(ev.Wait(TDuration::Seconds(1)));
    }
    {
        auto event = *writer->GetEvent(true);
        ASSERT_TRUE(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
        auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
        writer->Write(std::move(continueToken), "");
    }
    {
        auto event = *writer->GetEvent(true);
        ASSERT_TRUE(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
        auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
        writer->WriteEncoded(std::move(continueToken), packed, ECodec::GZIP, message.size());
    }

    std::uint32_t acks = 0, tokens = 0;
    while(acks < 4 || tokens < 2)  {
        auto event = *writer->GetEvent(true);
        if (std::holds_alternative<TWriteSessionEvent::TAcksEvent>(event)) {
            acks += std::get<TWriteSessionEvent::TAcksEvent>(event).Acks.size();
        }
        if (std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event)) {
            if (tokens == 0) {
                auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
                writer->WriteEncoded(std::move(continueToken), "", ECodec::RAW, 0);
            }
            ++tokens;
        }
        std::cerr << "GOT EVENT " << acks << " " << tokens << "\n";
    }
    ASSERT_FALSE(writer->WaitEvent().Wait(TDuration::Seconds(5)));

    ASSERT_EQ(acks, 4u);
    ASSERT_EQ(tokens, 2u);

    auto readSettings = TReadSessionSettings()
        .ConsumerName(GetConsumerName())
        .AppendTopics(GetTopicPath())
        // .DirectRead(EnableDirectRead)
        ;
    std::shared_ptr<IReadSession> readSession = client.CreateReadSession(readSettings);
    std::uint32_t readMessageCount = 0;
    while (readMessageCount < 4) {
        std::cerr << "Get event on client\n";
        auto event = *readSession->GetEvent(true);
        std::visit(TOverloaded {
            [&](TReadSessionEvent::TDataReceivedEvent& event) {
                for (auto& message: event.GetMessages()) {
                    std::string sourceId = message.GetMessageGroupId();
                    std::uint32_t seqNo = message.GetSeqNo();
                    ASSERT_EQ(readMessageCount + 1, seqNo);
                    ++readMessageCount;
                    ASSERT_EQ(message.GetData(), (seqNo % 2) == 1 ? "message" : "");
                }
            },
            [&](TReadSessionEvent::TCommitOffsetAcknowledgementEvent&) {
                FAIL();
            },
            [&](TReadSessionEvent::TStartPartitionSessionEvent& event) {
                event.Confirm();
            },
            [&](TReadSessionEvent::TStopPartitionSessionEvent& event) {
                event.Confirm();
            },
            [&](TReadSessionEvent::TEndPartitionSessionEvent& event) {
                event.Confirm();
            },
            [&](TReadSessionEvent::TPartitionSessionStatusEvent&) {
                FAIL() << "Test does not support lock sessions yet";
            },
            [&](TReadSessionEvent::TPartitionSessionClosedEvent&) {
                FAIL() << "Test does not support lock sessions yet";
            },
            [&](TSessionClosedEvent&) {
                FAIL() << "Session closed";
            }

        }, event);
    }
}

namespace {
    enum class EExpectedTestResult {
        SUCCESS,
        FAIL_ON_SDK,
        FAIL_ON_RPC
    };
}

class TSettingsValidation : public TTopicTestFixture {};

TEST_F(TSettingsValidation, TEST_NAME(TestDifferentDedupParams)) {
    char* ydbVersion = std::getenv("YDB_VERSION");
    if (ydbVersion != nullptr && std::string(ydbVersion) != "trunk" && std::string(ydbVersion) < "24.3") {
        GTEST_SKIP() << "Skipping test for YDB version " << ydbVersion;
    }

    auto driver = MakeDriver();

    TTopicClient client(driver);

    std::uint64_t producerIndex = 0u;
    auto runTest = [&](std::string producer, std::string msgGroup, const std::optional<bool>& useDedup, bool useSeqNo, EExpectedTestResult result) {
        TWriteSessionSettings writeSettings;
        writeSettings.Path(GetTopicPath()).Codec(NTopic::ECodec::RAW);
        std::string useDedupStr = useDedup.has_value() ? ToString(*useDedup) : "<unset>";
        if (!producer.empty()) {
            producer += ToString(producerIndex);
        }
        if (!msgGroup.empty()) {
            msgGroup += ToString(producerIndex);
        }
        writeSettings.ProducerId(producer).MessageGroupId(msgGroup);
        producerIndex++;
        std::cerr.flush();
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        std::cerr << "=== === START TEST. Producer = '" << producer << "', MsgGroup = '" << msgGroup << "', useDedup: "
                  << useDedupStr << ", manual SeqNo: " << useSeqNo << std::endl;

        try {
            if (useDedup.has_value()) {
                writeSettings.DeduplicationEnabled(useDedup);
            }
            auto session = client.CreateWriteSession(writeSettings);
            std::optional<TContinuationToken> token;
            std::uint64_t seqNo = 1u;
            std::uint64_t written = 0;
            while (written < 10) {
                auto event = session->GetEvent(true);
                if (std::holds_alternative<TSessionClosedEvent>(event.value())) {
                    auto closed = std::get<TSessionClosedEvent>(*event);
                    std::cerr << "Session failed with error: " << closed.DebugString() << std::endl;
                    ASSERT_EQ(result, EExpectedTestResult::FAIL_ON_RPC);
                    return;
                } else if (std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event.value())) {
                    token = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(*event).ContinuationToken);
                    if (useSeqNo) {
                        session->Write(std::move(*token), "data", seqNo++);
                    } else {
                        session->Write(std::move(*token), "data");
                    }
                    continue;
                } else {
                    ASSERT_TRUE(std::holds_alternative<TWriteSessionEvent::TAcksEvent>(*event));
                    const auto& acks = std::get<TWriteSessionEvent::TAcksEvent>(*event);
                    for (const auto& ack : acks.Acks) {
                        ASSERT_EQ(ack.State, TWriteSessionEvent::TWriteAck::EES_WRITTEN);
                    }
                    written += acks.Acks.size();
                }
            }
        } catch(const NYdb::TContractViolation& ex) {
            std::cerr << "Test fails on contract validation: " << ex.what() << std::endl;
            ASSERT_EQ(result, EExpectedTestResult::FAIL_ON_SDK);
            return;
        }
        std::cerr << "=== === END TEST (supposed ok)=== ===\n\n";
        ASSERT_EQ(result, EExpectedTestResult::SUCCESS);
    };
    // Normal scenarios:
    // Most common:
    std::vector<std::string> producers = {"producer", ""};
    std::vector<std::optional<std::string>> messageGroup = {std::nullopt, "producer", "messageGroup", ""};
    std::vector<std::optional<bool>> useDedupVariants = {std::nullopt, true, false};
    std::vector<bool> manSeqNoVariants = {true, false};
    runTest("producer", {}, {}, false, EExpectedTestResult::SUCCESS);
    runTest("producer", {}, {}, true, EExpectedTestResult::SUCCESS);
    // Enable dedup (doesnt take affect anything as it is enabled anyway)
    runTest("producer", {}, true, true, EExpectedTestResult::SUCCESS);
    runTest("producer", {}, true, false, EExpectedTestResult::SUCCESS);

    //No producer, do dedup
    runTest({}, {}, {}, false, EExpectedTestResult::SUCCESS);
    // manual seqNo with no-dedup - error
    runTest({}, {}, {}, true, EExpectedTestResult::FAIL_ON_SDK);
    // No producer but do enable dedup
    runTest({}, {}, true, true, EExpectedTestResult::SUCCESS);
    runTest({}, {}, true, false, EExpectedTestResult::SUCCESS);

    // MsgGroup = producer with explicit dedup enabling or not
    runTest("producer", "producer", {}, false, EExpectedTestResult::SUCCESS);
    runTest("producer", "producer", {}, true, EExpectedTestResult::SUCCESS);
    runTest("producer", "producer", true, true, EExpectedTestResult::SUCCESS);
    runTest("producer", "producer", true, false, EExpectedTestResult::SUCCESS);

    //Bad scenarios
        // MsgGroup != producer, triggers error
    runTest("producer", "msgGroup", {}, false, EExpectedTestResult::FAIL_ON_SDK);
    runTest("producer", "msgGroup", {}, true, EExpectedTestResult::FAIL_ON_SDK);
    runTest("producer", "msgGroup", true, true, EExpectedTestResult::FAIL_ON_SDK);
    runTest("producer", "msgGroup", true, false, EExpectedTestResult::FAIL_ON_SDK);

    //Set producer or msgGroupId but disnable dedup:
    runTest("producer", {}, false, true, EExpectedTestResult::FAIL_ON_SDK);
    runTest("producer", {}, false, false, EExpectedTestResult::FAIL_ON_SDK);
    runTest({}, "msgGroup", false, true, EExpectedTestResult::FAIL_ON_SDK);
    runTest({}, "msgGroup", false, false, EExpectedTestResult::FAIL_ON_SDK);

    //Use msgGroupId as producerId, enable dedup
    runTest({}, "msgGroup", true, true, EExpectedTestResult::SUCCESS);
    runTest({}, "msgGroup", true, false, EExpectedTestResult::SUCCESS);


    //Specify msg groupId and don't specify deduplication. Should work with dedup enable
    runTest({}, "msgGroup", {}, true, EExpectedTestResult::SUCCESS);
    runTest({}, "msgGroup", {}, false, EExpectedTestResult::SUCCESS);
}

TEST_F(TSettingsValidation, TEST_NAME(ValidateSettingsFailOnStart)) {
    auto driver = MakeDriver();

    TTopicClient client(driver);

    auto readSettings = TReadSessionSettings()
        .ConsumerName(GetConsumerName())
        .MaxMemoryUsageBytes(0)
        .AppendTopics(GetTopicPath())
        // .DirectRead(EnableDirectRead)
        ;

    auto readSession = client.CreateReadSession(readSettings);
    auto event = readSession->GetEvent(true);
    ASSERT_TRUE(event.has_value());

    auto& closeEvent = std::get<NYdb::NTopic::TSessionClosedEvent>(*event);
    ASSERT_NE(closeEvent.DebugString().find("Too small max memory usage"), std::string::npos);
}

} // namespace
