#include <ydb/public/api/grpc/draft/ydb_distributed_storage_v1.grpc.pb.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_admin.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>

namespace NYdb::NConsoleClient {
namespace {

using TRequest = Ydb::DistributedStorage::ChangeDataCenterStateRequest;
using TResponse = Ydb::DistributedStorage::ChangeDataCenterStateResponse;

class TStorageService : public Ydb::DistributedStorage::V1::DistributedStorageService::Service {
public:
    NThreading::TPromise<TRequest> Request = NThreading::NewPromise<TRequest>();
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;

    using TPoolRequest = Ydb::DistributedStorage::SetPoolRedundancyRequest;
    NThreading::TPromise<TPoolRequest> PoolRequest = NThreading::NewPromise<TPoolRequest>();

    grpc::Status SetPoolRedundancy(grpc::ServerContext*, const TPoolRequest* request,
            Ydb::DistributedStorage::SetPoolRedundancyResponse* response) override {
        PoolRequest.SetValue(*request);
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Status);
        return grpc::Status::OK;
    }

    grpc::Status ChangeDataCenterState(grpc::ServerContext*, const TRequest* request, TResponse* response) override {
        Request.SetValue(*request);
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Status);
        return grpc::Status::OK;
    }
};

void RunCommand(TVector<TString> args, int port = 0, TString subtree = "datacenter") {
    TVector<TString> fullArgs = {"admin", "storage", subtree};
    fullArgs.insert(fullArgs.end(), args.begin(), args.end());
    TVector<char*> argv;
    for (auto& arg : fullArgs) {
        argv.push_back(arg.Detach());
    }
    TClientCommand::TConfig config(argv.size(), argv.data());
    config.Address = TStringBuilder() << "127.0.0.1:" << port;
    config.SkipDiscovery = true;
    config.EnableSsl = false;
    config.AssumeYes = true;
    TCommandAdmin admin;
    TClientCommand& command = admin;
    command.PropagateFlags({});
    command.Prepare(config);
    UNIT_ASSERT(config.AllowEmptyDatabase);
    UNIT_ASSERT(config.OnlyExplicitProfile);
    if (port) {
        UNIT_ASSERT_VALUES_EQUAL(command.ValidateAndRun(config), EXIT_SUCCESS);
    }
}

TRequest RunWithService(TVector<TString> args, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
    TStorageService service;
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
    if (status == Ydb::StatusIds::SUCCESS) {
        RunCommand(std::move(args), port);
    } else {
        UNIT_ASSERT_EXCEPTION_CONTAINS(RunCommand(std::move(args), port), yexception, "PRECONDITION_FAILED");
    }
    return service.Request.GetFuture().GetValueSync();
}

} // namespace

Y_UNIT_TEST_SUITE(StoragePoolRedundancy) {
    Y_UNIT_TEST(SendsPoolTargetAndGeneration) {
        TStorageService service;
        int port = 0;
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service);
        auto server = builder.BuildAndStart();
        UNIT_ASSERT(server);
        Y_DEFER { server->Shutdown(); };
        RunCommand({"set-redundancy", "--storage-pool-name", "/Root/tpcc:nvme", "--storage-pool-generation", "0",
            "--asynchronous-redundancy", "Redundancy2plus2"}, port, "pool");
        const auto request = service.PoolRequest.GetFuture().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(request.storage_pool_name(), "/Root/tpcc:nvme");
        UNIT_ASSERT(request.has_storage_pool_generation());
        UNIT_ASSERT_VALUES_EQUAL(request.storage_pool_generation(), 0u);
        UNIT_ASSERT(request.asynchronous_redundancy() == TStorageService::TPoolRequest::REDUNDANCY_2_PLUS_2);
    }

    Y_UNIT_TEST(RejectsUnsupportedLayoutAndMissingGeneration) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(RunCommand({"set-redundancy", "--storage-pool-name", "pool"}, 0, "pool"),
            yexception, "storage-pool-generation");
        UNIT_ASSERT_EXCEPTION_CONTAINS(RunCommand({"set-redandancy", "--storage-pool-name", "pool",
            "--allow-any-storage-pool-generation", "--asynchronous-redundancy", "Redundancy1plus2"}, 0, "pool"),
            yexception, "asynchronous-redundancy");
    }
}

Y_UNIT_TEST_SUITE(StorageDataCenter) {
    Y_UNIT_TEST(TakeDown) {
        auto request = RunWithService({"takedown", "--datacenter-name", "dc-a"});
        UNIT_ASSERT_VALUES_EQUAL(request.datacenter_name(), "dc-a");
        UNIT_ASSERT(request.action() == TRequest::TAKEDOWN);
        UNIT_ASSERT(!request.dry_run());
    }

    Y_UNIT_TEST(TakeDownDryRun) {
        auto request = RunWithService({"takedown", "--datacenter-name", "dc-b", "--dry-run"});
        UNIT_ASSERT_VALUES_EQUAL(request.datacenter_name(), "dc-b");
        UNIT_ASSERT(request.action() == TRequest::TAKEDOWN);
        UNIT_ASSERT(request.dry_run());
    }

    Y_UNIT_TEST(Rejoin) {
        auto request = RunWithService({"rejoin", "--datacenter-name", "dc-c"});
        UNIT_ASSERT_VALUES_EQUAL(request.datacenter_name(), "dc-c");
        UNIT_ASSERT(request.action() == TRequest::REJOIN);
        UNIT_ASSERT(!request.dry_run());
    }

    Y_UNIT_TEST(ReportsServerError) {
        RunWithService({"takedown", "--datacenter-name", "dc-a"}, Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(RequiresDataCenterName) {
        for (TString command : {"takedown", "rejoin"}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(RunCommand({command}), yexception, "datacenter-name");
            UNIT_ASSERT_EXCEPTION_CONTAINS(RunCommand({command, "--datacenter-name", ""}), yexception, "datacenter-name");
        }
    }

    Y_UNIT_TEST(RejectsDryRunForRejoin) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(RunCommand({"rejoin", "--datacenter-name", "dc-a", "--dry-run"}), yexception, "dry-run");
    }

    Y_UNIT_TEST(CommandVisibilityAndProfiles) {
        TCommandAdmin admin;
        TClientCommand& command = admin;
        command.PropagateFlags({});
        auto* storage = command.FindNextCommand("storage");
        auto* datacenter = storage->FindNextCommand("datacenter");
        UNIT_ASSERT(storage->Visible && datacenter->Visible);
        for (TString name : {"takedown", "rejoin"}) {
            auto* action = datacenter->FindNextCommand(name);
            UNIT_ASSERT(action->Visible && action->Dangerous && action->OnlyExplicitProfile);
        }
        for (TString name : {"fetch", "replace"}) {
            auto* action = storage->FindNextCommand(name);
            UNIT_ASSERT(action->Visible && action->Dangerous && !action->OnlyExplicitProfile);
        }
    }
}

} // namespace NYdb::NConsoleClient
