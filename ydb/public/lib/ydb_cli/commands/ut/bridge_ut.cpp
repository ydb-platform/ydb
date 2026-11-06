#include <ydb/public/lib/ydb_cli/commands/ydb_bridge.h>
#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>

namespace NYdb::NConsoleClient {
namespace {

class TBridgeService : public Ydb::Bridge::V1::BridgeService::Service {
public:
    NThreading::TPromise<Ydb::Bridge::UpdateClusterStateRequest> Request =
        NThreading::NewPromise<Ydb::Bridge::UpdateClusterStateRequest>();
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;

    grpc::Status UpdateClusterState(
        grpc::ServerContext*,
        const Ydb::Bridge::UpdateClusterStateRequest* request,
        Ydb::Bridge::UpdateClusterStateResponse* response) override
    {
        Request.SetValue(*request);
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Status);
        return grpc::Status::OK;
    }
};

Ydb::Bridge::UpdateClusterStateRequest RunUpdate(
    std::optional<TString> generation,
    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS)
{
    TBridgeService service;
    service.Status = status;
    int port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    UNIT_ASSERT(server);
    Y_DEFER {
        server->Shutdown();
    };

    TVector<TString> args = {"update", "--set", "pile-a:DISCONNECTED"};
    if (generation) {
        args.push_back("--expected-generation");
        args.push_back(*generation);
    }
    TVector<char*> argv;
    for (auto& arg : args) {
        argv.push_back(arg.Detach());
    }
    TClientCommand::TConfig config(argv.size(), argv.data());
    config.Address = TStringBuilder() << "127.0.0.1:" << port;
    config.SkipDiscovery = true;
    config.EnableSsl = false;
    TCommandBridgeUpdate command(true);
    command.Prepare(config);
    if (status == Ydb::StatusIds::SUCCESS) {
        UNIT_ASSERT_VALUES_EQUAL(command.Run(config), EXIT_SUCCESS);
    } else {
        UNIT_ASSERT_EXCEPTION_CONTAINS(command.Run(config), yexception, "ABORTED");
    }
    auto request = service.Request.GetFuture().GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL(request.updates_size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(request.updates(0).pile_name(), "pile-a");
    UNIT_ASSERT(request.updates(0).state() == Ydb::Bridge::PileState::DISCONNECTED);
    UNIT_ASSERT_VALUES_EQUAL(request.quorum_piles_size(), 0);
    return request;
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeUpdate) {
    Y_UNIT_TEST(WithoutExpectedGeneration) {
        UNIT_ASSERT(!RunUpdate(std::nullopt).has_expected_generation());
    }

    Y_UNIT_TEST(WithExpectedGeneration) {
        auto request = RunUpdate("42");
        UNIT_ASSERT(request.has_expected_generation());
        UNIT_ASSERT_VALUES_EQUAL(request.expected_generation(), 42);
    }

    Y_UNIT_TEST(WithZeroExpectedGeneration) {
        auto request = RunUpdate("0");
        UNIT_ASSERT(request.has_expected_generation());
        UNIT_ASSERT_VALUES_EQUAL(request.expected_generation(), 0);
    }

    Y_UNIT_TEST(WithMaxExpectedGeneration) {
        auto request = RunUpdate("18446744073709551615");
        UNIT_ASSERT(request.has_expected_generation());
        UNIT_ASSERT_VALUES_EQUAL(request.expected_generation(), Max<ui64>());
    }

    Y_UNIT_TEST(AbortedUpdateIsReported) {
        auto request = RunUpdate("42", Ydb::StatusIds::ABORTED);
        UNIT_ASSERT(request.has_expected_generation());
        UNIT_ASSERT_VALUES_EQUAL(request.expected_generation(), 42);
    }

    Y_UNIT_TEST(InvalidExpectedGeneration) {
        for (const TString value : {"invalid", "18446744073709551616"}) {
            TVector<TString> args = {"update", "--set", "pile-a:DISCONNECTED", "--expected-generation", value};
            TVector<char*> argv;
            for (auto& arg : args) {
                argv.push_back(arg.Detach());
            }
            TClientCommand::TConfig config(argv.size(), argv.data());
            TCommandBridgeUpdate command(true);
            UNIT_ASSERT_EXCEPTION(command.Prepare(config), yexception);
        }
    }
}

} // namespace NYdb::NConsoleClient
