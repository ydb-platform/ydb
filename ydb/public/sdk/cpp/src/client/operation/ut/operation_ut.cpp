#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>

namespace NYdb::inline Dev::NOperation {
    namespace {

        class TOperationService: public Ydb::Operation::V1::OperationService::Service {
        public:
            grpc::Status GetOperation(grpc::ServerContext* context, const Ydb::Operations::GetOperationRequest* request,
                                      Ydb::Operations::GetOperationResponse* response) override {
                Deadline.store(context->deadline());
                auto& operation = *response->mutable_operation();
                operation.set_id(request->id());
                operation.set_ready(true);
                operation.set_status(Ydb::StatusIds::SUCCESS);
                return grpc::Status::OK;
            }

            std::atomic<std::chrono::system_clock::time_point> Deadline;
        };

        class TOperationClientTest: public testing::Test {
        protected:
            void SetUp() override {
                int port = 0;
                grpc::ServerBuilder builder;
                builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
                builder.RegisterService(&Service_);
                Server_ = builder.BuildAndStart();
                ASSERT_TRUE(Server_);
                TDriverConfig config;
                config.SetEndpoint("127.0.0.1:" + std::to_string(port)).SetDatabase("/local");
                config.SetDiscoveryMode(EDiscoveryMode::Off);
                Driver_ = std::make_unique<TDriver>(config);
                Client_ = std::make_unique<TOperationClient>(*Driver_);
            }

            void TearDown() override {
                Client_.reset();
                Driver_.reset();
                if (Server_) {
                    Server_->Shutdown();
                    Server_->Wait();
                }
            }

            void CheckGet(const TGetOperationSettings* settings = nullptr,
                          std::optional<std::chrono::minutes> timeout = std::nullopt,
                          std::chrono::system_clock::time_point before = std::chrono::system_clock::now()) {
                TOperation::TOperationId id;
                id.SetKind(TOperation::TOperationId::EXPORT);
                id.AddOptionalValue("id", "42");
                id.AddOptionalValue("kind", "yt");
                auto result = settings ? Client_->Get<NExport::TExportToYtResponse>(id, *settings)
                                       : Client_->Get<NExport::TExportToYtResponse>(id);
                ASSERT_TRUE(result.Wait(TDuration::Seconds(10)));
                const auto after = std::chrono::system_clock::now();
                EXPECT_TRUE(result.GetValueSync().Status().IsSuccess());
                EXPECT_EQ(result.GetValueSync().Id().ToString(), id.ToString());
                const auto deadline = Service_.Deadline.load();
                if (timeout) {
                    EXPECT_GE(deadline, before + *timeout - std::chrono::seconds(1));
                    EXPECT_LE(deadline, after + *timeout + std::chrono::seconds(1));
                } else {
                    EXPECT_GT(deadline, after + std::chrono::hours(24));
                }
            }

            TOperationService Service_;
            std::unique_ptr<grpc::Server> Server_;
            std::unique_ptr<TDriver> Driver_;
            std::unique_ptr<TOperationClient> Client_;
        };

        TEST_F(TOperationClientTest, LegacyGetHasNoDeadline) {
            CheckGet();
        }

        TEST_F(TOperationClientTest, DefaultSettingsHaveNoDeadline) {
            const TGetOperationSettings settings;
            CheckGet(&settings);
        }

        TEST_F(TOperationClientTest, ClientTimeoutIsPropagated) {
            TGetOperationSettings settings;
            settings.ClientTimeout(TDuration::Minutes(1));
            CheckGet(&settings, std::chrono::minutes(1));
        }

        TEST_F(TOperationClientTest, EarlierDeadlineWins) {
            const auto before = std::chrono::system_clock::now();
            TGetOperationSettings settings;
            settings.Deadline(TDeadline::AfterDuration(std::chrono::minutes(1)));
            settings.ClientTimeout(TDuration::Minutes(2));
            CheckGet(&settings, std::chrono::minutes(1), before);
        }

        TEST_F(TOperationClientTest, EarlierClientTimeoutWins) {
            const auto before = std::chrono::system_clock::now();
            TGetOperationSettings settings;
            settings.ClientTimeout(TDuration::Minutes(1));
            settings.Deadline(TDeadline::AfterDuration(std::chrono::minutes(2)));
            CheckGet(&settings, std::chrono::minutes(1), before);
        }

    } // namespace
} // namespace NYdb::inline Dev::NOperation
