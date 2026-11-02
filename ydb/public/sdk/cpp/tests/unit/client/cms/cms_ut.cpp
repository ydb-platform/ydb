#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/cms/cms.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>

#include <atomic>
#include <memory>
#include <optional>
#include <string>

namespace {

    struct TReply {
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        bool HasOperationId = true;
        bool Pending = false;
        bool TransportFailure = false;
    };

    class TCmsService: public Ydb::Cms::V1::CmsService::Service {
    public:
        const TReply Reply;
        std::atomic<bool> HasFreeze = false;
        std::atomic<bool> Freeze = false;

        explicit TCmsService(TReply reply)
            : Reply(reply)
        {
        }

        grpc::Status AlterDatabase(grpc::ServerContext*, const Ydb::Cms::AlterDatabaseRequest* request,
                                   Ydb::Cms::AlterDatabaseResponse* response) override {
            HasFreeze = request->has_freeze();
            Freeze = request->freeze();
            if (Reply.TransportFailure) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Injected transport failure");
            }
            auto* operation = response->mutable_operation();
            operation->set_ready(!Reply.Pending);
            operation->set_status(Reply.Status);
            if (Reply.HasOperationId) {
                operation->set_id("freeze-operation");
            }
            if (Reply.Status != Ydb::StatusIds::SUCCESS) {
                operation->add_issues()->set_message("Injected server error");
            }
            return grpc::Status::OK;
        }
    };

    class TOperationService: public Ydb::Operation::V1::OperationService::Service {
    public:
        std::atomic<ui32> Polls = 0;

        grpc::Status GetOperation(grpc::ServerContext*, const Ydb::Operations::GetOperationRequest* request,
                                  Ydb::Operations::GetOperationResponse* response) override {
            ++Polls;
            auto* operation = response->mutable_operation();
            operation->set_id(request->id());
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            return grpc::Status::OK;
        }
    };

    class TServer {
    public:
        TCmsService Cms;
        TOperationService Operations;
        std::string Endpoint;
        std::unique_ptr<grpc::Server> Server;

        explicit TServer(TReply reply = {})
            : Cms(reply)
        {
            grpc::ServerBuilder builder;
            int port = 0;
            builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
            builder.RegisterService(&Cms);
            builder.RegisterService(&Operations);
            Server = builder.BuildAndStart();
            UNIT_ASSERT(Server);
            UNIT_ASSERT(port > 0);
            Endpoint = "127.0.0.1:" + std::to_string(port);
        }

        ~TServer() {
            Server->Shutdown();
        }

        NYdb::TStatus Alter(NYdb::NCms::TAlterDatabaseSettings settings) {
            NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(Endpoint).SetDatabase("/Root"));
            NYdb::NCms::TCmsClient client(driver, NYdb::TCommonClientSettings().DiscoveryMode(NYdb::EDiscoveryMode::Off));
            auto result = client.AlterDatabase("/Root/Database", settings.ClientTimeout(TDuration::Seconds(10)));
            UNIT_ASSERT_C(result.Wait(TDuration::Seconds(15)), "AlterDatabase future did not complete");
            return result.GetValueSync();
        }
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(CmsFreezeClient) {
    Y_UNIT_TEST(SerializationPreservesOptionalFalseCopyAndReset) {
        Ydb::Cms::AlterDatabaseRequest request;
        NYdb::NCms::TAlterDatabaseSettings settings;
        settings.SerializeTo(request);
        UNIT_ASSERT(!request.has_freeze());
        for (const bool freeze : {false, true}) {
            settings.Freeze(freeze);
            const auto copy = settings;
            request.Clear();
            copy.SerializeTo(request);
            UNIT_ASSERT(request.has_freeze());
            UNIT_ASSERT_VALUES_EQUAL(request.freeze(), freeze);
        }
        settings.Freeze(std::nullopt);
        request.Clear();
        settings.SerializeTo(request);
        UNIT_ASSERT(!request.has_freeze());
    }

    Y_UNIT_TEST(LegacyServerCannotAcknowledgeFreezeOrThaw) {
        for (const bool freeze : {false, true}) {
            TServer server({.HasOperationId = false});
            const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().Freeze(freeze));
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::UNSUPPORTED);
            UNIT_ASSERT(server.Cms.HasFreeze);
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.Freeze.load(), freeze);
            UNIT_ASSERT(result.GetIssues().ToString().find("freeze") != std::string::npos);
        }
    }

    Y_UNIT_TEST(LegacyAlterWithoutFreezeStillSucceeds) {
        TServer server({.HasOperationId = false});
        const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().AlterAttributes({{"test", "value"}}));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(!server.Cms.HasFreeze);
    }

    Y_UNIT_TEST(FreezeAndThawWithOperationIdSucceed) {
        for (const bool freeze : {false, true}) {
            TServer server;
            const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().Freeze(freeze));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(server.Cms.HasFreeze);
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.Freeze.load(), freeze);
        }
    }

    Y_UNIT_TEST(AsyncOperationIsPolledUntilCompletion) {
        TServer server({.Pending = true});
        const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().Freeze(true));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(server.Operations.Polls > 0);
    }

    Y_UNIT_TEST(ServerErrorWithoutOperationIdIsPreserved) {
        TServer server({.Status = Ydb::StatusIds::UNAUTHORIZED, .HasOperationId = false});
        const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().Freeze(true));
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::UNAUTHORIZED);
        UNIT_ASSERT(result.GetIssues().ToString().find("Injected server error") != std::string::npos);
    }

    Y_UNIT_TEST(TransportFailureCompletesFuture) {
        TServer server({.TransportFailure = true});
        const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().Freeze(true));
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT(result.IsTransportError());
    }
} // Y_UNIT_TEST_SUITE(CmsFreezeClient)
