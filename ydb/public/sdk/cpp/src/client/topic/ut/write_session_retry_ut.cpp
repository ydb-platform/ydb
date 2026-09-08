#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <grpcpp/grpcpp.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

namespace NYdb::inline Dev::NTopic::NTests {
    namespace {

        using TStreamWrite = Ydb::Topic::StreamWriteMessage;

        template <class T>
        T Await(NThreading::TFuture<T> future) {
            UNIT_ASSERT_C(future.Wait(TDuration::Seconds(30)), "Operation timed out");
            auto result = future.ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        }

        // Forward the sequential init/write exchanges used by this test to the real server.
        // Lose the first write ACK only after the server has successfully stored the message.
        class TLostWriteAckProxy final: public Ydb::Topic::V1::TopicService::Service {
        public:
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
                TStreamWrite::FromClient request;
                while (downstream->Read(&request)) {
                    if (!upstream->Write(request)) {
                        upstreamContext->TryCancel();
                        return upstream->Finish();
                    }
                    TStreamWrite::FromServer response;
                    if (!upstream->Read(&response)) {
                        return upstream->Finish();
                    }
                    if (response.has_write_response() && response.status() == Ydb::StatusIds::SUCCESS && DropAck_.exchange(false)) {
                        upstreamContext->TryCancel();
                        upstream->Finish();
                        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Lost write ACK");
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
            std::atomic_bool DropAck_{true};
        };

    } // anonymous namespace

    Y_UNIT_TEST_SUITE(WriteSessionRetry) {
        Y_UNIT_TEST(LostAckDoesNotDuplicateMessageInTransactionWithoutDeduplication) {
            TTopicSdkTestSetup setup(TEST_CASE_NAME);
            auto driver = setup.MakeDriver();
            NQuery::TQueryClient queryClient(driver);
            auto session = Await(queryClient.GetSession()).GetSession();
            auto tx = Await(session.BeginTransaction(NQuery::TTxSettings())).GetTransaction();

            TLostWriteAckProxy proxy(setup.GetEndpoint());
            auto writerConfig = setup.MakeDriverConfig();
            writerConfig.SetEndpoint(proxy.GetEndpoint()).SetDiscoveryMode(EDiscoveryMode::Off);
            TDriver writerDriver(writerConfig);
            TTopicClient writerClient(writerDriver);
            auto writer = writerClient.CreateSimpleBlockingWriteSession(
                TWriteSessionSettings()
                    .Path(setup.GetTopicPath())
                    .PartitionId(0)
                    .DirectWriteToPartition(false)
                    .DeduplicationEnabled(false)
                    .Codec(ECodec::RAW));

            UNIT_ASSERT(writer->Write(TWriteMessage("message"), &tx, TDuration::Seconds(30)));
            UNIT_ASSERT(writer->Close(TDuration::Seconds(30))); // Wait for the SDK to retry and receive the ACK.
            Await(tx.Commit());

            TTopicClient topicClient(driver);
            auto describe = Await(topicClient.DescribePartition(setup.GetTopicPath(), 0,
                                                                TDescribePartitionSettings().IncludeStats(true)));
            const auto& stats = describe.GetPartitionDescription().GetPartition().GetPartitionStats();
            UNIT_ASSERT(stats);
            UNIT_ASSERT_VALUES_EQUAL(stats->GetStartOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(stats->GetEndOffset(), 1); // One Write must commit exactly one message.
        }
    } // Y_UNIT_TEST_SUITE(WriteSessionRetry)

} // namespace NYdb::inline Dev::NTopic::NTests
