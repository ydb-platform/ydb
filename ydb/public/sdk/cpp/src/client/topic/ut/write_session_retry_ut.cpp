#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <grpcpp/grpcpp.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace NYdb::inline Dev::NTopic::NTests {
    namespace {

        using TStreamWrite = Ydb::Topic::StreamWriteMessage;

        template <class T>
        T Await(NThreading::TFuture<T> future) {
            UNIT_ASSERT_C(future.Wait(TDuration::Seconds(30)), "Operation timed out");
            return future.ExtractValueSync();
        }

        // Forward the sequential init/write exchanges used by this test to the real server.
        // Lose the first write ACK only after the server has successfully stored the message.
        class TLostWriteAckProxy final: public Ydb::Topic::V1::TopicService::Service {
        public:
            struct TAttempt {
                TStreamWrite::InitRequest Init;
                TStreamWrite::WriteRequest Write;
                TStreamWrite::WriteResponse Ack;
            };

            explicit TLostWriteAckProxy(const std::string& upstreamEndpoint)
                : Stub_(Ydb::Topic::V1::TopicService::NewStub(
                      grpc::CreateChannel(TString(upstreamEndpoint), grpc::InsecureChannelCredentials())))
            {
                grpc::ServerBuilder builder;
                int port = 0;
                builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
                builder.RegisterService(this);
                Server_ = builder.BuildAndStart();
                UNIT_ASSERT(Server_);
                UNIT_ASSERT(port > 0);
                Endpoint_ = "127.0.0.1:" + std::to_string(port);
            }

            ~TLostWriteAckProxy() override {
                Server_->Shutdown(std::chrono::system_clock::now());
                Server_->Wait();
            }

            const std::string& GetEndpoint() const {
                return Endpoint_;
            }

            std::vector<TAttempt> GetAttempts() const {
                std::lock_guard guard(Lock_);
                return Attempts_;
            }

            grpc::Status StreamWrite(grpc::ServerContext* context,
                                     grpc::ServerReaderWriter<TStreamWrite::FromServer, TStreamWrite::FromClient>* downstream) override {
                auto upstreamContext = grpc::ClientContext::FromServerContext(*context);
                upstreamContext->set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
                for (const auto& [key, value] : context->client_metadata()) {
                    const TString name(key.data(), key.size());
                    if (name == "x-ydb-database" || name == "x-ydb-auth-ticket") {
                        upstreamContext->AddMetadata(name, TString(value.data(), value.size()));
                    }
                }
                auto upstream = Stub_->StreamWrite(upstreamContext.get());
                TStreamWrite::InitRequest init;
                TStreamWrite::FromClient request;
                while (downstream->Read(&request)) {
                    if (request.has_init_request()) {
                        init = request.init_request();
                    }
                    if (!upstream->Write(request)) {
                        upstreamContext->TryCancel();
                        return upstream->Finish();
                    }
                    TStreamWrite::FromServer response;
                    if (!upstream->Read(&response)) {
                        return upstream->Finish();
                    }
                    if (request.has_write_request() && response.has_write_response() && response.status() == Ydb::StatusIds::SUCCESS) {
                        bool dropAck;
                        {
                            std::lock_guard guard(Lock_);
                            Attempts_.push_back({init, request.write_request(), response.write_response()});
                            dropAck = Attempts_.size() == 1;
                        }
                        if (dropAck) {
                            upstreamContext->TryCancel();
                            upstream->Finish();
                            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Injected disconnect after successful write, before ACK");
                        }
                    }
                    if (!downstream->Write(response)) {
                        break;
                    }
                }
                upstreamContext->TryCancel();
                upstream->Finish();
                return grpc::Status::OK;
            }

        private:
            std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> Stub_;
            std::unique_ptr<grpc::Server> Server_;
            std::string Endpoint_;
            mutable std::mutex Lock_;
            std::vector<TAttempt> Attempts_;
        };

        TContinuationToken WaitForWriteToken(IWriteSession& writer) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            while (true) {
                UNIT_ASSERT_C(writer.WaitEvent().Wait(deadline), "Timed out waiting for a write token");
                for (auto& event : writer.GetEvents()) {
                    if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                        return std::move(ready->ContinuationToken);
                    }
                    if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                        UNIT_FAIL("Writer closed unexpectedly: " << closed->GetIssues().ToString());
                    }
                }
            }
        }

        void TestLostTransactionalWriteAck(const std::string& testName, bool deduplicationEnabled) {
            TTopicSdkTestSetup setup(testName);
            auto driver = setup.MakeDriver();
            NQuery::TQueryClient queryClient(driver);
            auto sessionResult = Await(queryClient.GetSession());
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
            auto session = sessionResult.GetSession();
            auto beginResult = Await(session.BeginTransaction(NQuery::TTxSettings()));
            UNIT_ASSERT_C(beginResult.IsSuccess(), beginResult.GetIssues().ToString());
            auto tx = beginResult.GetTransaction();

            TLostWriteAckProxy proxy(setup.GetEndpoint());
            auto writerConfig = setup.MakeDriverConfig();
            writerConfig.SetEndpoint(proxy.GetEndpoint()).SetDiscoveryMode(EDiscoveryMode::Off);
            TDriver writerDriver(writerConfig);
            TTopicClient writerClient(writerDriver);
            auto writer = writerClient.CreateWriteSession(
                TWriteSessionSettings()
                    .Path(setup.GetTopicPath())
                    .PartitionId(0)
                    .DirectWriteToPartition(false)
                    .DeduplicationEnabled(deduplicationEnabled)
                    .Codec(ECodec::RAW)
                    .BatchFlushInterval(TDuration::Zero()));

            const std::string payload = "one application write, retried after a lost ACK";
            TWriteMessage message(payload);
            message.Tx(tx);
            writer->Write(WaitForWriteToken(*writer), std::move(message)); // The only application Write call.
            UNIT_ASSERT_C(Await(writer->Flush()), "The SDK did not acknowledge the retried message");

            const auto attempts = proxy.GetAttempts();
            UNIT_ASSERT_VALUES_EQUAL(attempts.size(), 2);
            for (const auto& attempt : attempts) {
                UNIT_ASSERT_VALUES_EQUAL(attempt.Init.producer_id().empty(), !deduplicationEnabled);
                UNIT_ASSERT_VALUES_EQUAL(attempt.Init.message_group_id().empty(), !deduplicationEnabled);
                UNIT_ASSERT_VALUES_EQUAL(attempt.Init.producer_id(), attempts.front().Init.producer_id());
                UNIT_ASSERT_VALUES_EQUAL(attempt.Ack.acks_size(), 1);
                UNIT_ASSERT(attempt.Write.has_tx());
                UNIT_ASSERT_VALUES_EQUAL(attempt.Write.messages_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(attempt.Write.messages(0).data(), payload);
                UNIT_ASSERT_VALUES_EQUAL(attempt.Write.messages(0).seq_no(), attempts.front().Write.messages(0).seq_no());
                UNIT_ASSERT_VALUES_EQUAL(attempt.Write.tx().id(), tx.GetId());
                UNIT_ASSERT_VALUES_EQUAL(attempt.Write.tx().session(), tx.GetSessionId());
            }

            UNIT_ASSERT(attempts.front().Ack.acks(0).has_written_in_tx());

            auto commitResult = Await(tx.Commit()); // Commit the same transaction once, after the retry.
            UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());
            UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));

            TTopicClient topicClient(driver);
            const size_t expectedCopies = deduplicationEnabled ? 1 : 2;
            auto describe = Await(topicClient.DescribePartition(setup.GetTopicPath(), 0,
                                                                TDescribePartitionSettings().IncludeStats(true)));
            UNIT_ASSERT_C(describe.IsSuccess(), describe.GetIssues().ToString());
            const auto& stats = describe.GetPartitionDescription().GetPartition().GetPartitionStats();
            UNIT_ASSERT(stats);
            UNIT_ASSERT_VALUES_EQUAL(stats->GetStartOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats->GetEndOffset(), expectedCopies);

            auto reader = topicClient.CreateReadSession(TReadSessionSettings()
                                                            .ConsumerName(setup.GetConsumerName())
                                                            .AppendTopics(TTopicReadSettings(setup.GetTopicPath()).AppendPartitionIds(0)));
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            std::vector<uint64_t> offsets;
            while (offsets.size() < expectedCopies) {
                UNIT_ASSERT_C(reader->WaitEvent().Wait(deadline), "Timed out reading committed messages");
                for (auto& event : reader->GetEvents()) {
                    if (auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                        start->Confirm();
                    } else if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
                        for (const auto& received : data->GetMessages()) {
                            UNIT_ASSERT_VALUES_EQUAL(received.GetData(), payload);
                            UNIT_ASSERT_VALUES_EQUAL(received.GetPartitionSession()->GetPartitionId(), 0);
                            offsets.push_back(received.GetOffset());
                        }
                    } else if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                        UNIT_FAIL("Reader closed unexpectedly: " << closed->GetIssues().ToString());
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(offsets.size(), expectedCopies);
            for (size_t i = 0; i < offsets.size(); ++i) {
                // With deduplication off, the identical payload is stored twice, at distinct offsets 0 and 1.
                UNIT_ASSERT_VALUES_EQUAL(offsets[i], i);
            }
            UNIT_ASSERT(reader->Close(TDuration::Seconds(30)));
        }

    } // anonymous namespace

    Y_UNIT_TEST_SUITE(WriteSessionRetry) {
        Y_UNIT_TEST(LostAckDuplicatesMessageInTransactionWithoutDeduplication) {
            TestLostTransactionalWriteAck(TEST_CASE_NAME, false);
        }

        Y_UNIT_TEST(LostAckDoesNotDuplicateMessageInTransactionWithDeduplication) {
            TestLostTransactionalWriteAck(TEST_CASE_NAME, true);
        }
    } // Y_UNIT_TEST_SUITE(WriteSessionRetry)

} // namespace NYdb::inline Dev::NTopic::NTests
