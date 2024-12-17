#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/string/join.h>
#include <util/system/event.h>
#include <util/stream/zlib.h>
#include <util/stream/str.h>

using namespace NThreading;
using namespace NKikimr;
using namespace NKikimr::NPersQueueTests;
using namespace NPersQueue;

namespace NYdb::NPersQueue::NTests {
// This suite tests basic Logbroker usage scenario: write and read a bunch of messages
Y_UNIT_TEST_SUITE(BasicUsage) {
    template<typename TResponse>
    void AssertStreamingMessageCase(const typename TResponse::ResponseCase& expectedCase, const TResponse& actualResponse) {
        UNIT_ASSERT_EQUAL_C(expectedCase, actualResponse.GetResponseCase(), "Got unexpected streaming message or error:\n" + actualResponse.DebugString());
    }

    using TWriteCallable = std::function<TFuture<TWriteResult>(TString& data, ui64 sequenceNumber, TInstant createdAt)>;

    void WriteAndReadAndCommitRandomMessages(TPersQueueYdbSdkTestSetup* setup, TWriteCallable write, bool disableClusterDiscovery = false) {
        auto log = setup->GetLog();
        const TInstant start = TInstant::Now();
        TVector<TString> messages;
        const ui32 messageCount = 5;
        ui32 totalSize = 0;
        std::queue<NThreading::TFuture<TWriteResult>> writeFutures;
        for (ui32 i = 1; i <= messageCount; ++i) {
            const size_t size = (1 + (i%100))*1_KB;

            const auto timestamp = TInstant::MilliSeconds(i);
            const auto message = NUnitTest::RandomString(size, std::rand());

            messages.emplace_back(std::move(message));

            auto writeFuture = write(messages.back(), i, timestamp);
            writeFutures.push(writeFuture);
            totalSize += size;
        }

        while (!writeFutures.empty()) {
            const auto& oldestWriteFuture = writeFutures.front();
            const auto& result = oldestWriteFuture.GetValueSync();
            UNIT_ASSERT_C(result.Ok || result.NoWait, result.ResponseDebugString);
            writeFutures.pop();
        }

        log.Write(TLOG_INFO, "All messages are written");

        std::shared_ptr<IReadSession> readSession = setup->GetPersQueueClient().CreateReadSession(
                setup->GetReadSessionSettings().DisableClusterDiscovery(disableClusterDiscovery)
        );
//            auto isStarted = consumer->Start().ExtractValueSync();
//            AssertStreamingMessageCase(TReadResponse::kInit, isStarted.Response);

        ui32 readMessageCount = 0;
        TMaybe<ui32> committedOffset;
        ui32 previousOffset = 0;
        bool closed = false;
        while ((readMessageCount < messageCount || committedOffset <= previousOffset) && !closed) {
            Cerr << "Get event on client\n";
            auto event = *readSession->GetEvent(true);
            std::visit(TOverloaded {
                [&](TReadSessionEvent::TDataReceivedEvent& event) {
                    for (auto& message: event.GetMessages()) {
                        TString sourceId = message.GetMessageGroupId();
                        ui32 seqNo = message.GetSeqNo();
                        UNIT_ASSERT_VALUES_EQUAL(readMessageCount + 1, seqNo);
                        ++readMessageCount;
                        auto offset = message.GetOffset();
                        if (readMessageCount == 1) {
                            UNIT_ASSERT_VALUES_EQUAL(offset, 0);
                        } else {
                            UNIT_ASSERT_VALUES_EQUAL(offset, previousOffset + 1);
                        }
                        previousOffset = offset;
                        message.Commit();
                    }
                },
                [&](TReadSessionEvent::TCommitAcknowledgementEvent& event) {
                    auto offset = event.GetCommittedOffset();
                    log << TLOG_INFO << "Got commit ack with offset " << offset;
                    if (committedOffset.Defined()) {
                        UNIT_ASSERT_GT(offset, *committedOffset);
                    }
                    committedOffset = offset;
                },
                [&](TReadSessionEvent::TCreatePartitionStreamEvent& event) {
                    //UNIT_FAIL("Test does not support lock sessions yet");
                    event.Confirm();
                },
                [&](TReadSessionEvent::TDestroyPartitionStreamEvent& event) {
//                        UNIT_FAIL("Test does not support lock sessions yet");
                    event.Confirm();
                },
                [&](TReadSessionEvent::TPartitionStreamStatusEvent&) {
                    UNIT_FAIL("Test does not support lock sessions yet");
                },
                [&](TReadSessionEvent::TPartitionStreamClosedEvent&) {
                    UNIT_FAIL("Test does not support lock sessions yet");
                },
                [&](TSessionClosedEvent& event) {
                    log << TLOG_INFO << "Got close event: " << event.DebugString();
                    //UNIT_FAIL("Session closed");
                    closed = true;
                }

            }, event);
            log << TLOG_INFO << "Read message count is " << readMessageCount << " (from " << messageCount
                             << " in total), committed offset count is "
                             << (committedOffset.Defined() ? * committedOffset : 0) << " (maximum offset: "
                             << previousOffset << ")";
        }

        UNIT_ASSERT_VALUES_EQUAL(previousOffset + 1, committedOffset);
        UNIT_ASSERT_VALUES_EQUAL(readMessageCount, messageCount);
        log.Write(TLOG_INFO, Sprintf("Time took to write and read %u messages, %lu [MiB] in total is %lu [s]",
                                     messageCount, (totalSize / 1_MB), (TInstant::Now() - start).Seconds()));
    }


    void SimpleWriteAndValidateData(
            TPersQueueYdbSdkTestSetup* setup, TWriteSessionSettings& writeSettings, ui64 count,
            TMaybe<bool> shouldCaptureData = Nothing()
    ) {
        std::shared_ptr<NYdb::NPersQueue::IReadSession> readSession;

        auto& client = setup->GetPersQueueClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);
        TString messageBase = "message-";
        TVector<TString> sentMessages;

        for (auto i = 0u; i < count; i++) {
            sentMessages.emplace_back(messageBase * (i+1) + ToString(i));
            auto res = session->Write(sentMessages.back());
            UNIT_ASSERT(res);
        }
        {
            auto sessionAdapter = TSimpleWriteSessionTestAdapter(
                    dynamic_cast<TSimpleBlockingWriteSession *>(session.get()));
            if (shouldCaptureData.Defined()) {
                TStringBuilder msg;
                msg << "Session has captured " << sessionAdapter.GetAcquiredMessagesCount()
                    << " messages, capturing was expected: " << *shouldCaptureData << Endl;
                UNIT_ASSERT_VALUES_EQUAL_C(sessionAdapter.GetAcquiredMessagesCount() > 0, *shouldCaptureData, msg.c_str());
            }
        }
        session->Close();

        auto readSettings = setup->GetReadSessionSettings();
        NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
        auto totalReceived = 0u;
        readSettings.EventHandlers_.SimpleDataHandlers([&](NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& ev) {
            auto& messages = ev.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];
                UNIT_ASSERT_VALUES_EQUAL(message.GetData(), sentMessages[totalReceived]);
                totalReceived++;
            }
            if (totalReceived == sentMessages.size())
                checkedPromise.SetValue();
        });
        readSession = client.CreateReadSession(readSettings);
        checkedPromise.GetFuture().GetValueSync();
        readSession->Close(TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(MaxByteSizeEqualZero) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        TPersQueueClient client(setup->GetDriver());

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

        auto& createPartitionStream = std::get<TReadSessionEvent::TCreatePartitionStreamEvent>(*event);
        createPartitionStream.Confirm();

        UNIT_CHECK_GENERATED_EXCEPTION(readSession->GetEvent(true, 0), TContractViolation);
        UNIT_CHECK_GENERATED_EXCEPTION(readSession->GetEvents(true, Nothing(), 0), TContractViolation);

        event = readSession->GetEvent(true, 1);
        UNIT_ASSERT(event.Defined());

        auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
        dataReceived.Commit();
    }

    Y_UNIT_TEST(WriteAndReadSomeMessagesWithAsyncCompression) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        SimpleWriteAndValidateData(setup.get(), writeSettings, 100u, true);
    }

    Y_UNIT_TEST(WriteAndReadSomeMessagesWithSyncCompression) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        IExecutor::TPtr executor = CreateSyncExecutor();
        writeSettings.CompressionExecutor(executor);
        // LOGBROKER-7189
        //SimpleWriteAndValidateData(setup.get(), writeSettings, 100u, false);
        SimpleWriteAndValidateData(setup.get(), writeSettings, 100u, true);
    }

    Y_UNIT_TEST(WriteAndReadSomeMessagesWithNoCompression) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        writeSettings.Codec(ECodec::RAW);
        // LOGBROKER-7189
        //SimpleWriteAndValidateData(setup.get(), writeSettings, 100u, false);
        SimpleWriteAndValidateData(setup.get(), writeSettings, 100u, true);
    }

    Y_UNIT_TEST(TWriteSession_WriteAndReadAndCommitRandomMessages) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto log = setup->GetLog();
        TYDBClientEventLoop clientEventLoop{setup};

        TAutoEvent messagesWrittenToBuffer;
        auto clientWrite = [&](TString& message, ui64 sequenceNumber, TInstant createdAt) {
            auto promise = NewPromise<TWriteResult>();
            //log << TLOG_INFO << "Enqueue message with sequence number " << sequenceNumber;
            clientEventLoop.MessageBuffer.Enqueue(TAcknowledgableMessage{message, sequenceNumber, createdAt, promise});
            messagesWrittenToBuffer.Signal();
            return promise.GetFuture();
        };

        WriteAndReadAndCommitRandomMessages(setup.get(), std::move(clientWrite));
    }

    Y_UNIT_TEST(TWriteSession_WriteAndReadAndCommitRandomMessagesNoClusterDiscovery) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto log = setup->GetLog();
        TYDBClientEventLoop clientEventLoop{setup};

        TAutoEvent messagesWrittenToBuffer;
        auto clientWrite = [&](TString& message, ui64 sequenceNumber, TInstant createdAt) {
            auto promise = NewPromise<TWriteResult>();
            //log << TLOG_INFO << "Enqueue message with sequence number " << sequenceNumber;
            clientEventLoop.MessageBuffer.Enqueue(TAcknowledgableMessage{message, sequenceNumber, createdAt, promise});
            messagesWrittenToBuffer.Signal();
            return promise.GetFuture();
        };

        WriteAndReadAndCommitRandomMessages(setup.get(), std::move(clientWrite), true);
    }

    Y_UNIT_TEST(TSimpleWriteSession_AutoSeqNo_BasicUsage) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto& client = setup->GetPersQueueClient();
        auto settings = setup->GetWriteSessionSettings();
        auto simpleSession = client.CreateSimpleBlockingWriteSession(settings);

        TString msg = "message";
        auto clientWrite = [&](TString& message, ui64 seqNo, TInstant createdAt) {
            auto promise = NewPromise<TWriteResult>();
            //log << TLOG_INFO << "Enqueue message with sequence number " << sequenceNumber;
            simpleSession->Write(message, seqNo, createdAt);
            promise.SetValue(TWriteResult{true});
            return promise.GetFuture();
        };
        WriteAndReadAndCommitRandomMessages(setup.get(), std::move(clientWrite));
        auto res = simpleSession->Close();
        UNIT_ASSERT(res);
        UNIT_ASSERT(res);
    }



    Y_UNIT_TEST(TWriteSession_AutoBatching) {
        // ToDo: Re-enable once batching takes more than 1 message at once
        return;
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto& client = setup->GetPersQueueClient();
        auto settings = setup->GetWriteSessionSettings();
        size_t batchSize = 100;
        settings.BatchFlushInterval(TDuration::Seconds(1000)); // Batch on size, not on time.
        settings.BatchFlushSizeBytes(batchSize);
        auto writer = client.CreateWriteSession(settings);
        TString message = "message";
        size_t totalSize = 0, seqNo = 0;
        while (totalSize < batchSize) {
            auto event = *writer->GetEvent(true);
            UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
            auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
            writer->Write(std::move(continueToken), message, ++seqNo);
            totalSize += message.size();
        }
        WaitMessagesAcked(writer, 1, seqNo);
    }

    Y_UNIT_TEST(TWriteSession_WriteEncoded) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto& client = setup->GetPersQueueClient();
        auto settings = setup->GetWriteSessionSettings();
        size_t batchSize = 100000000;
        settings.BatchFlushInterval(TDuration::Seconds(1000)); // Batch on size, not on time.
        settings.BatchFlushSizeBytes(batchSize);
        auto writer = client.CreateWriteSession(settings);
        TString message = "message";
        TString packed;
        {
            TStringOutput so(packed);
            TZLibCompress oss(&so, ZLib::GZip, 6);
            oss << message;
        }

        Cerr << message << " " << packed << "\n";

        {
            auto event = *writer->GetEvent(true);
            UNIT_ASSERT(!writer->WaitEvent().Wait(TDuration::Seconds(1)));
            auto ev = writer->WaitEvent();
            UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
            auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
            writer->Write(std::move(continueToken), message);
            UNIT_ASSERT(ev.Wait(TDuration::Seconds(1)));
        }
        {
            auto event = *writer->GetEvent(true);
            UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
            auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
            writer->Write(std::move(continueToken), "");
        }
        {
            auto event = *writer->GetEvent(true);
            UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
            auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
            writer->WriteEncoded(std::move(continueToken), packed, ECodec::GZIP, message.size());
        }

        ui32 acks = 0, tokens = 0;
        while(acks < 4 || tokens < 2)  {
            auto event = *writer->GetEvent(true);
            if (std::holds_alternative<TWriteSessionEvent::TAcksEvent>(event)) acks += std::get<TWriteSessionEvent::TAcksEvent>(event).Acks.size();
            if (std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event)) {
                if (tokens == 0) {
                    auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
                    writer->WriteEncoded(std::move(continueToken), "", ECodec::RAW, 0);
                }
                ++tokens;
            }
            Cerr << "GOT EVENT " << acks << " " << tokens << "\n";

        }
        UNIT_ASSERT(!writer->WaitEvent().Wait(TDuration::Seconds(5)));

        UNIT_ASSERT_VALUES_EQUAL(acks, 4);
        UNIT_ASSERT_VALUES_EQUAL(tokens, 2);

        std::shared_ptr<IReadSession> readSession = setup->GetPersQueueClient().CreateReadSession(
                setup->GetReadSessionSettings().DisableClusterDiscovery(true)
        );
        ui32 readMessageCount = 0;
        while (readMessageCount < 4) {
            Cerr << "Get event on client\n";
            auto event = *readSession->GetEvent(true);
            std::visit(TOverloaded {
                [&](TReadSessionEvent::TDataReceivedEvent& event) {
                    for (auto& message: event.GetMessages()) {
                        TString sourceId = message.GetMessageGroupId();
                        ui32 seqNo = message.GetSeqNo();
                        UNIT_ASSERT_VALUES_EQUAL(readMessageCount + 1, seqNo);
                        ++readMessageCount;
                        UNIT_ASSERT_VALUES_EQUAL(message.GetData(), (seqNo % 2) == 1 ? "message" : "");
                    }
                },
                [&](TReadSessionEvent::TCommitAcknowledgementEvent&) {
                    UNIT_FAIL("no commits in test");
                },
                [&](TReadSessionEvent::TCreatePartitionStreamEvent& event) {
                    event.Confirm();
                },
                [&](TReadSessionEvent::TDestroyPartitionStreamEvent& event) {
                    event.Confirm();
                },
                [&](TReadSessionEvent::TPartitionStreamStatusEvent&) {
                    UNIT_FAIL("Test does not support lock sessions yet");
                },
                [&](TReadSessionEvent::TPartitionStreamClosedEvent&) {
                    UNIT_FAIL("Test does not support lock sessions yet");
                },
                [&](TSessionClosedEvent&) {
                    UNIT_FAIL("Session closed");
                }

            }, event);
        }
    }


    Y_UNIT_TEST(TWriteSession_BatchingProducesContinueTokens) {
        // ToDo: Re-enable once batching takes more than 1 message at once
        return;
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto& client = setup->GetPersQueueClient();
        auto settings = setup->GetWriteSessionSettings();
        auto batchInterval = TDuration::Seconds(10);
        settings.BatchFlushInterval(batchInterval); // Batch on size, not on time.
        settings.BatchFlushSizeBytes(9999999999);
        settings.MaxMemoryUsage(99);
        auto writer = client.CreateWriteSession(settings);
        TString message = "0123456789";
        auto seqNo = 10u;

        auto event = *writer->GetEvent(true);
        UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
        auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
        writer->Write(std::move(continueToken), message * 5, ++seqNo);
        auto eventF = writer->WaitEvent();
        eventF.Wait(batchInterval / 2);
        UNIT_ASSERT(!eventF.HasValue());
        while (true) {
            event = *writer->GetEvent(true);
            if (std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event)) {
                break;
            } else {
                UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TAcksEvent>(event));
            }
        }
        writer->Close();
    }

    class TBrokenCredentialsProvider : public ICredentialsProvider {
    public:
        TBrokenCredentialsProvider() {}
        virtual ~TBrokenCredentialsProvider() {}
        TStringType GetAuthInfo() const {
            ythrow yexception() << "exception during creation";
            return "";
        }
        bool IsValid() const { return true; }
    };

    class TBrokenCredentialsProviderFactory : public ICredentialsProviderFactory {
    public:
        TBrokenCredentialsProviderFactory() {}

        virtual ~TBrokenCredentialsProviderFactory() {}
        virtual TCredentialsProviderPtr CreateProvider() const {
            return std::make_shared<TBrokenCredentialsProvider>();
        }

        virtual TStringType GetClientIdentity() const {
            return "abacaba";
        }
    };

    Y_UNIT_TEST(BrokenCredentialsProvider) {

        std::shared_ptr<ICredentialsProviderFactory> brokenCredentialsProviderFactory;
        brokenCredentialsProviderFactory.reset(new TBrokenCredentialsProviderFactory{});
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto client = TPersQueueClient(setup->GetDriver(), TPersQueueClientSettings().CredentialsProviderFactory(brokenCredentialsProviderFactory));
        auto settings = setup->GetReadSessionSettings();
        settings.DisableClusterDiscovery(true)
                .RetryPolicy(IRetryPolicy::GetNoRetryPolicy());
        std::shared_ptr<IReadSession> readSession = client.CreateReadSession(settings);

        Cerr << "Get event on client\n";
        auto event = *readSession->GetEvent(true);
        std::visit(TOverloaded {
                [&](TReadSessionEvent::TDataReceivedEvent&) {
                    UNIT_ASSERT(false);
                },
                [&](TReadSessionEvent::TCommitAcknowledgementEvent&) {
                    UNIT_ASSERT(false);
                },
                [&](TReadSessionEvent::TCreatePartitionStreamEvent& event) {
                    UNIT_ASSERT(false);
                    event.Confirm();
                },
                [&](TReadSessionEvent::TDestroyPartitionStreamEvent& event) {
                    UNIT_ASSERT(false);
                    event.Confirm();
                },
                [&](TReadSessionEvent::TPartitionStreamStatusEvent&) {
                    UNIT_FAIL("Test does not support lock sessions yet");
                },
                [&](TReadSessionEvent::TPartitionStreamClosedEvent&) {
                    UNIT_FAIL("Test does not support lock sessions yet");
                },
                [&](TSessionClosedEvent& event) {
                    Cerr << "Got close event: " << event.DebugString();
                }

            }, event);

    }
}
} // namespace NYdb::NPersQueue::NTests
