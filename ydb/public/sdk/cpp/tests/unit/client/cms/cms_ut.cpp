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
        std::atomic<ui32> Requests = 0;
        std::atomic<bool> HasFreeze = false;
        std::atomic<bool> Freeze = false;
        std::atomic<bool> HasData = false;
        std::atomic<bool> Data = false;
        std::atomic<int> OperationMode = Ydb::Operations::OperationParams::OPERATION_MODE_UNSPECIFIED;
        std::atomic<i64> OperationTimeoutSeconds = 0;

        explicit TCmsService(TReply reply)
            : Reply(reply)
        {
        }

        grpc::Status AlterDatabase(grpc::ServerContext*, const Ydb::Cms::AlterDatabaseRequest* request,
                                   Ydb::Cms::AlterDatabaseResponse* response) override {
            ++Requests;
            HasFreeze = request->has_freeze_schema();
            Freeze = request->freeze_schema();
            HasData = request->has_freeze_datashards();
            Data = request->freeze_datashards();
            OperationMode = request->operation_params().operation_mode();
            OperationTimeoutSeconds = request->operation_params().operation_timeout().seconds();
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
        std::atomic<ui32> Cancellations = 0;
        std::atomic<bool> Ready = true;

        grpc::Status GetOperation(grpc::ServerContext*, const Ydb::Operations::GetOperationRequest* request,
                                  Ydb::Operations::GetOperationResponse* response) override {
            ++Polls;
            auto* operation = response->mutable_operation();
            operation->set_id(request->id());
            operation->set_ready(Ready.load());
            if (operation->ready()) {
                operation->set_status(Ydb::StatusIds::SUCCESS);
            }
            return grpc::Status::OK;
        }

        grpc::Status CancelOperation(grpc::ServerContext*, const Ydb::Operations::CancelOperationRequest*,
                                     Ydb::Operations::CancelOperationResponse* response) override {
            ++Cancellations;
            response->set_status(Ydb::StatusIds::SUCCESS);
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
        UNIT_ASSERT(!request.has_freeze_schema());
        for (const bool freeze : {false, true}) {
            settings.FreezeSchema(freeze);
            const auto copy = settings;
            request.Clear();
            copy.SerializeTo(request);
            UNIT_ASSERT(request.has_freeze_schema());
            UNIT_ASSERT_VALUES_EQUAL(request.freeze_schema(), freeze);
        }
        settings.FreezeSchema(std::nullopt);
        request.Clear();
        settings.SerializeTo(request);
        UNIT_ASSERT(!request.has_freeze_schema());
    }

    Y_UNIT_TEST(LegacyServerCannotAcknowledgeFreezeOrThaw) {
        for (const bool freeze : {false, true}) {
            TServer server({.HasOperationId = false});
            const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(freeze));
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
        UNIT_ASSERT_VALUES_EQUAL(server.Cms.Requests.load(), 1);
        UNIT_ASSERT(!server.Cms.HasFreeze);
        UNIT_ASSERT_VALUES_EQUAL(server.Cms.OperationMode.load(), static_cast<int>(Ydb::Operations::OperationParams::OPERATION_MODE_UNSPECIFIED));
    }

    Y_UNIT_TEST(FreezeAndThawWithOperationIdSucceed) {
        for (const bool freeze : {false, true}) {
            TServer server;
            const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(freeze));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(server.Cms.HasFreeze);
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.Freeze.load(), freeze);
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.OperationMode.load(), static_cast<int>(Ydb::Operations::OperationParams::SYNC));
        }
    }

    Y_UNIT_TEST(OperationTimeoutIsForwardedInSyncMode) {
        for (const bool freeze : {false, true}) {
            TServer server;
            const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(freeze).OperationTimeout(TDuration::Seconds(1)));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.OperationMode.load(), static_cast<int>(Ydb::Operations::OperationParams::SYNC));
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.OperationTimeoutSeconds.load(), 1);
        }
    }

    Y_UNIT_TEST(AsyncOperationIsPolledUntilCompletion) {
        TServer server({.Pending = true});
        const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(true));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(server.Operations.Polls > 0);
    }

    Y_UNIT_TEST(ServerErrorWithoutOperationIdIsPreserved) {
        TServer server({.Status = Ydb::StatusIds::UNAUTHORIZED, .HasOperationId = false});
        const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(true));
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::UNAUTHORIZED);
        UNIT_ASSERT(result.GetIssues().ToString().find("Injected server error") != std::string::npos);
    }

    Y_UNIT_TEST(TransportFailureCompletesFuture) {
        TServer server({.TransportFailure = true});
        const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(true));
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT(result.IsTransportError());
    }

    Y_UNIT_TEST(FreezeWithAttributesIsRejectedBeforeRpc) {
        for (const bool freeze : {false, true}) {
            // An empty attribute value requests removal, so it also conflicts.
            for (const auto* value : {"value", ""}) {
                TServer server({.HasOperationId = false});
                const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings()
                    .FreezeSchema(freeze).AlterAttributes({{"test", value}}));
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
                UNIT_ASSERT(result.GetIssues().ToString().find("freeze cannot be combined") != std::string::npos);
                UNIT_ASSERT_VALUES_EQUAL(server.Cms.Requests.load(), 0);
                UNIT_ASSERT_VALUES_EQUAL(server.Operations.Polls.load(), 0);
            }
        }
    }

    Y_UNIT_TEST(UnsupportedFreezeOperationParametersAreRejectedBeforeRpc) {
        for (const bool freeze : {false, true}) {
            for (const bool cancel : {false, true}) {
                TServer server;
                NYdb::NCms::TAlterDatabaseSettings settings;
                settings.FreezeSchema(freeze);
                if (cancel) {
                    settings.CancelAfter(TDuration::Seconds(1));
                } else {
                    settings.ForgetAfter(TDuration::Seconds(1));
                }
                const auto result = server.Alter(settings);
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::UNSUPPORTED);
                UNIT_ASSERT(result.GetIssues().ToString().find("cancel_after and forget_after") != std::string::npos);
                UNIT_ASSERT_VALUES_EQUAL(server.Cms.Requests.load(), 0);
                UNIT_ASSERT_VALUES_EQUAL(server.Operations.Polls.load(), 0);
            }
        }
    }

    Y_UNIT_TEST(EmptyAttributesDoNotConflictWithFreeze) {
        for (const bool freeze : {false, true}) {
            TServer server;
            const auto result = server.Alter(NYdb::NCms::TAlterDatabaseSettings()
                .FreezeSchema(freeze).AlterAttributes({}));
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.Requests.load(), 1);
            UNIT_ASSERT(server.Cms.HasFreeze);
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.Freeze.load(), freeze);
        }
    }

    Y_UNIT_TEST(OperationTimeoutBoundsPollingWithoutClientTimeout) {
        for (const bool freeze : {false, true}) {
            TServer server({.Pending = true});
            server.Operations.Ready = false;
            NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(server.Endpoint).SetDatabase("/Root"));
            NYdb::NCms::TCmsClient client(driver, NYdb::TCommonClientSettings().DiscoveryMode(NYdb::EDiscoveryMode::Off));
            // Do not use TServer::Alter: its ClientTimeout would mask the bug.
            const auto result = client.AlterDatabase("/Root/Database",
                NYdb::NCms::TAlterDatabaseSettings().FreezeSchema(freeze).OperationTimeout(TDuration::Seconds(1)));
            const bool completed = result.Wait(TDuration::Seconds(5));
            // Stop pending polls before asserting so a regression cannot leave
            // an unbounded request running during test teardown.
            driver.Stop(true);
            UNIT_ASSERT_C(completed, "OperationTimeout did not bound polling of the pending freeze operation");
            const auto status = result.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED,
                status.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.Requests.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.OperationMode.load(), static_cast<int>(Ydb::Operations::OperationParams::SYNC));
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.OperationTimeoutSeconds.load(), 1);
            UNIT_ASSERT(server.Operations.Polls.load() > 0);
            UNIT_ASSERT(!server.Operations.Ready.load());
            UNIT_ASSERT_VALUES_EQUAL(server.Operations.Cancellations.load(), 0);
        }
    }

    Y_UNIT_TEST(IndependentFieldsPreservePresenceCopyAndReset) {
        for (ui32 mask = 1; mask <= 3; ++mask) {
            for (const bool schema : {false, true}) {
                for (const bool data : {false, true}) {
                    NYdb::NCms::TAlterDatabaseSettings settings;
                    if (mask & 1) {
                        settings.FreezeSchema(schema);
                    }
                    if (mask & 2) {
                        settings.FreezeDataShards(data);
                    }
                    const auto copy = settings;
                    Ydb::Cms::AlterDatabaseRequest request;
                    copy.SerializeTo(request);
                    UNIT_ASSERT_VALUES_EQUAL(request.has_freeze_schema(), bool(mask & 1));
                    UNIT_ASSERT_VALUES_EQUAL(request.has_freeze_datashards(), bool(mask & 2));
                    if (mask & 1) {
                        UNIT_ASSERT_VALUES_EQUAL(request.freeze_schema(), schema);
                    }
                    if (mask & 2) {
                        UNIT_ASSERT_VALUES_EQUAL(request.freeze_datashards(), data);
                    }
                    settings.FreezeSchema(std::nullopt).FreezeDataShards(std::nullopt);
                    request.Clear();
                    settings.SerializeTo(request);
                    UNIT_ASSERT(!request.has_freeze_schema() && !request.has_freeze_datashards());
                }
            }
        }
    }

    Y_UNIT_TEST(DataOnlyUsesExistingOperationWaitAndValidation) {
        for (const bool data : {false, true}) {
            TServer server({.Pending = true});
            const auto status = server.Alter(NYdb::NCms::TAlterDatabaseSettings().FreezeDataShards(data));
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
            UNIT_ASSERT(!server.Cms.HasFreeze.load());
            UNIT_ASSERT(server.Cms.HasData.load());
            UNIT_ASSERT_VALUES_EQUAL(server.Cms.Data.load(), data);
            UNIT_ASSERT(server.Operations.Polls.load() > 0);
            TServer rejected;
            const auto bad = rejected.Alter(NYdb::NCms::TAlterDatabaseSettings()
                .FreezeDataShards(data).AlterAttributes({{"key", "value"}}));
            UNIT_ASSERT_VALUES_EQUAL(bad.GetStatus(), NYdb::EStatus::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(rejected.Cms.Requests.load(), 0);
        }
    }

    Y_UNIT_TEST(FreezeStatusDistinguishesTargetFromApplicationAndOldServer) {
        Ydb::Cms::GetDatabaseStatusResult proto;
        auto* freeze = proto.mutable_freeze_status();
        freeze->set_desired_schema(false);
        freeze->set_desired_datashards(true);
        freeze->set_operation_id("pending");
        freeze->set_ready(false);
        NYdb::NCms::TGetDatabaseStatusResult pending(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), proto);
        UNIT_ASSERT(pending.GetFreezeStatus().has_value());
        UNIT_ASSERT(!pending.GetFreezeStatus()->DesiredSchema);
        UNIT_ASSERT(pending.GetFreezeStatus()->DesiredDataShards);
        UNIT_ASSERT(!pending.GetFreezeStatus()->Ready);
        UNIT_ASSERT(!pending.GetFreezeStatus()->Status.has_value());
        freeze->set_ready(true);
        freeze->set_status(Ydb::StatusIds::PRECONDITION_FAILED);
        NYdb::NCms::TGetDatabaseStatusResult failed(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), proto);
        UNIT_ASSERT_VALUES_EQUAL(*failed.GetFreezeStatus()->Status, NYdb::EStatus::PRECONDITION_FAILED);
        proto.clear_freeze_status();
        NYdb::NCms::TGetDatabaseStatusResult legacy(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), proto);
        UNIT_ASSERT(!legacy.GetFreezeStatus().has_value());
    }
} // Y_UNIT_TEST_SUITE(CmsFreezeClient)
