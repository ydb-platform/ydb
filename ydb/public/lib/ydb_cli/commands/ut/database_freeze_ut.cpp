#include <ydb/public/lib/ydb_cli/commands/ydb_admin.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>

namespace NYdb::NConsoleClient {
    namespace {

        struct TReply {
            Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
            bool Pending = false;
            bool HasOperationId = true;
        };

        class TCmsService: public Ydb::Cms::V1::CmsService::Service {
        public:
            std::atomic<ui32> Requests = 0;
            TReply Reply;
            NThreading::TPromise<Ydb::Cms::AlterDatabaseRequest> Request =
                NThreading::NewPromise<Ydb::Cms::AlterDatabaseRequest>();

            grpc::Status AlterDatabase(
                grpc::ServerContext*,
                const Ydb::Cms::AlterDatabaseRequest* request,
                Ydb::Cms::AlterDatabaseResponse* response) override {
                ++Requests;
                Request.SetValue(*request);
                auto* operation = response->mutable_operation();
                if (Reply.HasOperationId) {
                    operation->set_id("cli-freeze-operation");
                }
                operation->set_ready(!Reply.Pending);
                if (!Reply.Pending) {
                    operation->set_status(Reply.Status);
                    if (Reply.Status != Ydb::StatusIds::SUCCESS) {
                        operation->add_issues()->set_message("Injected database freeze failure");
                    }
                }
                return grpc::Status::OK;
            }
        };

        class TOperationService: public Ydb::Operation::V1::OperationService::Service {
        public:
            Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
            std::atomic<ui32> Polls = 0;
            NThreading::TPromise<Ydb::Operations::GetOperationRequest> FirstPoll =
                NThreading::NewPromise<Ydb::Operations::GetOperationRequest>();
            NThreading::TPromise<void> Complete = NThreading::NewPromise();

            grpc::Status GetOperation(
                grpc::ServerContext*,
                const Ydb::Operations::GetOperationRequest* request,
                Ydb::Operations::GetOperationResponse* response) override {
                if (Polls.fetch_add(1) == 0) {
                    FirstPoll.SetValue(*request);
                }
                // Bound the test service itself, including failure/teardown paths.
                if (!Complete.GetFuture().Wait(TDuration::Seconds(10))) {
                    return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Test completion gate timed out");
                }
                auto* operation = response->mutable_operation();
                operation->set_id(request->id());
                operation->set_ready(true);
                operation->set_status(Status);
                if (Status != Ydb::StatusIds::SUCCESS) {
                    operation->add_issues()->set_message("Injected database freeze failure");
                }
                return grpc::Status::OK;
            }
        };

        class TServer {
        public:
            TCmsService Cms;
            TOperationService Operations;
            TString Endpoint;
            std::unique_ptr<grpc::Server> Server;

            explicit TServer(TReply reply = {}) {
                Cms.Reply = reply;
                Operations.Status = reply.Status;
                grpc::ServerBuilder builder;
                int port = 0;
                builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
                builder.RegisterService(&Cms);
                builder.RegisterService(&Operations);
                Server = builder.BuildAndStart();
                UNIT_ASSERT(Server);
                UNIT_ASSERT(port > 0);
                Endpoint = TStringBuilder() << "127.0.0.1:" << port;
            }

            ~TServer() {
                Operations.Complete.TrySetValue();
                Server->Shutdown();
                Server->Wait();
            }

            int Run(bool freeze, const TVector<TString>& extraArgs = {}) {
                TVector<TString> args = {"admin", "database", freeze ? "freeze" : "unfreeze", "--timeout", "5s"};
                args.insert(args.end(), extraArgs.begin(), extraArgs.end());
                TVector<char*> argv;
                for (auto& arg : args) {
                    argv.push_back(arg.Detach());
                }
                TClientCommand::TConfig config(argv.size(), argv.data());
                config.Address = Endpoint;
                config.Database = "/Root/Database";
                config.SkipDiscovery = true;
                config.EnableSsl = false;
                config.AssumeYes = true;
                // Exercise command registration and argument parsing as well as Run().
                TCommandAdmin command;
                command.Prepare(config);
                return command.ValidateAndRun(config);
            }

            void CheckRequest(bool freeze, bool schema = true, bool data = true) {
                const auto received = Cms.Request.GetFuture();
                UNIT_ASSERT_C(received.Wait(TDuration::Seconds(5)), "CLI did not call AlterDatabase");
                const auto& request = received.GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(Cms.Requests.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(request.path(), "/Root/Database");
                UNIT_ASSERT_VALUES_EQUAL(request.has_freeze_schema(), schema);
                UNIT_ASSERT_VALUES_EQUAL(request.has_freeze_datashards(), data);
                if (schema) {
                    UNIT_ASSERT_VALUES_EQUAL(request.freeze_schema(), freeze);
                }
                if (data) {
                    UNIT_ASSERT_VALUES_EQUAL(request.freeze_datashards(), freeze);
                }
                UNIT_ASSERT(request.operation_params().operation_mode() == Ydb::Operations::OperationParams::SYNC);
            }
        };

    } // namespace

    Y_UNIT_TEST_SUITE(DatabaseFreezeCommand) {
        Y_UNIT_TEST(FreezeAndUnfreezeSendExplicitFlag) {
            for (const bool freeze : {true, false}) {
                TServer server;
                UNIT_ASSERT_VALUES_EQUAL(server.Run(freeze), EXIT_SUCCESS);
                server.CheckRequest(freeze);
                UNIT_ASSERT_VALUES_EQUAL(server.Operations.Polls.load(), 0);
            }
        }

        Y_UNIT_TEST(FreezeAndUnfreezeWaitForOperationCompletion) {
            for (const bool freeze : {true, false}) {
                // Exercise the SDK fallback when an endpoint returns a pending
                // operation despite SYNC. Acceptance must not finish the command.
                TServer server({.Pending = true});
                auto result = std::async(std::launch::async, [&] { return server.Run(freeze); });
                Y_DEFER {
                    // Release the RPC before std::future joins its worker on failure.
                    server.Operations.Complete.TrySetValue();
                };
                const auto poll = server.Operations.FirstPoll.GetFuture();
                UNIT_ASSERT_C(poll.Wait(TDuration::Seconds(5)), "CLI did not poll the pending operation");
                UNIT_ASSERT_VALUES_EQUAL(poll.GetValueSync().id(), "cli-freeze-operation");
                UNIT_ASSERT_C(result.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout,
                              "CLI finished before the operation completed");
                server.Operations.Complete.SetValue();
                UNIT_ASSERT_C(result.wait_for(std::chrono::seconds(10)) == std::future_status::ready,
                              "CLI did not finish after the operation completed");
                UNIT_ASSERT_VALUES_EQUAL(result.get(), EXIT_SUCCESS);
                server.CheckRequest(freeze);
            }
        }

        Y_UNIT_TEST(RejectedAlterIsReported) {
            for (const bool freeze : {true, false}) {
                TServer server({.Status = Ydb::StatusIds::PRECONDITION_FAILED});
                UNIT_ASSERT_EXCEPTION_CONTAINS(server.Run(freeze), yexception, "Injected database freeze failure");
                server.CheckRequest(freeze);
            }
        }

        Y_UNIT_TEST(DeferredOperationFailureIsReported) {
            for (const bool freeze : {true, false}) {
                TServer server({.Status = Ydb::StatusIds::PRECONDITION_FAILED, .Pending = true});
                server.Operations.Complete.SetValue();
                UNIT_ASSERT_EXCEPTION_CONTAINS(server.Run(freeze), yexception, "Injected database freeze failure");
                server.CheckRequest(freeze);
                UNIT_ASSERT(server.Operations.Polls.load() > 0);
            }
        }

        Y_UNIT_TEST(LegacyServerCannotAcknowledgeFreezeOrUnfreeze) {
            for (const bool freeze : {true, false}) {
                TServer server({.HasOperationId = false});
                UNIT_ASSERT_EXCEPTION_CONTAINS(server.Run(freeze), yexception, "Server does not support database freeze operations");
                server.CheckRequest(freeze);
            }
        }

        Y_UNIT_TEST(UnexpectedPositionalArgumentsAreRejected) {
            for (const bool freeze : {true, false}) {
                TServer server;
                UNIT_ASSERT_EXCEPTION_CONTAINS(server.Run(freeze, {"unexpected"}), TMisuseException,
                                               "requires exactly 0 free arg(s)");
                UNIT_ASSERT(!server.Cms.Request.GetFuture().HasValue());
            }
        }

        Y_UNIT_TEST(ExplicitScopesAreIndependentAndUseOneRpc) {
            for (const bool freeze : {false, true}) {
                TServer schema;
                UNIT_ASSERT_VALUES_EQUAL(schema.Run(freeze, {"--schema"}), EXIT_SUCCESS);
                schema.CheckRequest(freeze, true, false);
                TServer data;
                UNIT_ASSERT_VALUES_EQUAL(data.Run(freeze, {"--datashards"}), EXIT_SUCCESS);
                data.CheckRequest(freeze, false, true);
                TServer both;
                UNIT_ASSERT_VALUES_EQUAL(both.Run(freeze, {"--schema", "--datashards"}), EXIT_SUCCESS);
                both.CheckRequest(freeze);
            }
        }
    } // Y_UNIT_TEST_SUITE(DatabaseFreezeCommand)

} // namespace NYdb::NConsoleClient
