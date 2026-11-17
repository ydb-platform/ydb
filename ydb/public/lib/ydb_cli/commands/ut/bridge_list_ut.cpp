#include <ydb/public/lib/ydb_cli/commands/ydb_bridge.h>
#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/tempfile.h>

#include <atomic>

namespace NYdb::NConsoleClient {
namespace {

class TBridgeListService : public Ydb::Bridge::V1::BridgeService::Service {
public:
    Ydb::Bridge::GetClusterStateResult Cluster;
    Ydb::Bridge::ListPileSynchronizationResult Summary;
    Ydb::Bridge::ListDatabaseSynchronizationResult Databases;
    Ydb::Bridge::ListGroupSynchronizationResult Groups;
    Ydb::Bridge::ListDatabaseSynchronizationRequest DatabaseRequest;
    Ydb::Bridge::ListGroupSynchronizationRequest GroupRequest;
    std::atomic<size_t> SummaryCalls = 0;
    Ydb::StatusIds::StatusCode SummaryStatus = Ydb::StatusIds::SUCCESS;
    Ydb::StatusIds::StatusCode GroupStatus = Ydb::StatusIds::SUCCESS;
    TString Address;
    std::unique_ptr<grpc::Server> Server;

    ~TBridgeListService() override {
        if (Server) {
            Server->Shutdown();
        }
    }

    void Start() {
        if (Server) {
            return;
        }
        int port = 0;
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(this);
        Server = builder.BuildAndStart();
        UNIT_ASSERT(Server);
        Address = TStringBuilder() << "127.0.0.1:" << port;
    }

    TBridgeListService() {
        Cluster.set_generation(118);
        auto* primary = Cluster.add_pile_states();
        primary->set_pile_name("dc1");
        primary->set_state(Ydb::Bridge::PileState::PRIMARY);
        auto* secondary = Cluster.add_pile_states();
        secondary->set_pile_name("dc2");
        secondary->set_state(Ydb::Bridge::PileState::SYNCHRONIZED);
        Summary.set_cluster_state_generation(118);
        for (const auto& name : {"dc1", "dc2"}) {
            auto* pile = Summary.add_piles();
            pile->set_pile_name(name);
            pile->mutable_databases()->set_total(3);
            pile->mutable_databases()->set_synced(1);
            pile->mutable_databases()->set_syncing(1);
            pile->mutable_databases()->set_pending(1);
        }
        Databases.set_cluster_state_generation(118);
        auto* database = Databases.add_databases();
        database->set_database_path("/Root/db1");
        database->set_pile_name("dc2");
        database->set_storage_mode(Ydb::Bridge::BRIDGED);
        database->mutable_groups()->set_required(2);
        database->mutable_groups()->set_synced(1);
        database->mutable_groups()->set_syncing(1);
        database->mutable_last_sync_error()->set_message("VDisk unavailable");
        Groups.set_cluster_state_generation(118);
        auto* group = Groups.add_groups();
        group->set_group_id(2181038080);
        group->set_pile_name("dc2");
        group->set_sync_status(Ydb::Bridge::SYNCING);
        group->set_sync_phase("WRITE_KEEP_BARRIER_DONOTKEEP_DATA");
        group->mutable_storage_pool()->set_name("/Root/db1:ssd");
        group->mutable_storage_pool()->mutable_id()->set_box_id(1);
        group->mutable_storage_pool()->mutable_id()->set_pool_id(2);
        group->set_physical_group_id(2181038092);
    }

    grpc::Status GetClusterState(grpc::ServerContext*,
                                 const Ydb::Bridge::GetClusterStateRequest*,
                                 Ydb::Bridge::GetClusterStateResponse* response) override {
        return Reply(response, Cluster);
    }

    grpc::Status ListPileSynchronization(grpc::ServerContext*,
                                         const Ydb::Bridge::ListPileSynchronizationRequest*,
                                         Ydb::Bridge::ListPileSynchronizationResponse* response) override {
        ++SummaryCalls;
        return Reply(response, Summary, SummaryStatus);
    }

    grpc::Status ListDatabaseSynchronization(grpc::ServerContext*,
                                             const Ydb::Bridge::ListDatabaseSynchronizationRequest* request,
                                             Ydb::Bridge::ListDatabaseSynchronizationResponse* response) override {
        DatabaseRequest = *request;
        return Reply(response, Databases);
    }

    grpc::Status ListGroupSynchronization(grpc::ServerContext*,
                                          const Ydb::Bridge::ListGroupSynchronizationRequest* request,
                                          Ydb::Bridge::ListGroupSynchronizationResponse* response) override {
        GroupRequest = *request;
        return Reply(response, Groups, GroupStatus);
    }

private:
    template <class TResponse, class TResult>
    static grpc::Status Reply(TResponse* response,
                              const TResult& result,
                              Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(status);
        if (status == Ydb::StatusIds::SUCCESS) {
            operation->mutable_result()->PackFrom(result);
        } else {
            operation->add_issues()->set_message("diagnostic result exceeds API limit");
        }
        return grpc::Status::OK;
    }
};

void RedirectStdout(const TFile& file) {
    TFileHandle handle(file.GetHandle());
    Y_DEFER {
        handle.Release();
    };
    UNIT_ASSERT(handle.Duplicate2Posix(1) != -1);
}

struct TCommandOutput {
    int Code;
    TString Text;
};

TCommandOutput RunList(TBridgeListService& service, TVector<TString> args) {
    service.Start();
    args.insert(args.begin(), "list");
    TVector<char*> argv;
    for (auto& arg : args) {
        argv.push_back(arg.Detach());
    }
    TClientCommand::TConfig config(argv.size(), argv.data());
    config.Address = service.Address;
    config.Database = "/Root/connection";
    config.SkipDiscovery = true;
    config.EnableSsl = false;
    TCommandBridgeList command(true);
    command.Prepare(config);

    Cout.Flush();
    TFile saved = Duplicate(stdout);
    TTempFileHandle captured;
    Y_DEFER {
        Cout.Flush();
        RedirectStdout(saved);
    };
    RedirectStdout(captured);
    const auto code = command.Run(config);
    Cout.Flush();
    return {code, TFileInput(captured.Name()).ReadAll()};
}

void CheckInvalidColumns(TVector<TString> args, const TString& error) {
    args.insert(args.begin(), "list");
    TVector<char*> argv;
    for (auto& arg : args) {
        argv.push_back(arg.Detach());
    }
    TClientCommand::TConfig config(argv.size(), argv.data());
    TCommandBridgeList command(true);
    UNIT_ASSERT_EXCEPTION_CONTAINS(command.Prepare(config), yexception, error);
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeList) {
    Y_UNIT_TEST(DefaultPilesIncludeSynchronization) {
        TBridgeListService service;
        const auto result = RunList(service, {"piles", "--format", "csv"});
        UNIT_ASSERT_VALUES_EQUAL(result.Code, EXIT_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(service.SummaryCalls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.Text,
                                 "Pile,PileState,DatabasesSync,Operation,LastError\n"
                                 "dc1,PRIMARY,1/3,,\ndc2,SYNCHRONIZED,1/3,,\n");
    }

    Y_UNIT_TEST(SelectedColumnsSkipSummaryAndKeepPileRows) {
        TBridgeListService service;
        service.SummaryStatus = Ydb::StatusIds::UNAVAILABLE;
        const auto result = RunList(service, {"--columns", "PileState,Pile", "--format", "csv"});
        UNIT_ASSERT_VALUES_EQUAL(result.Code, EXIT_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(service.SummaryCalls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(result.Text, "PileState,Pile\nPRIMARY,dc1\nSYNCHRONIZED,dc2\n");
    }

    Y_UNIT_TEST(AllColumnsAndPileFilter) {
        TBridgeListService service;
        const auto result = RunList(service, {"piles", "-A", "--pile", "dc2", "--format", "csv"});
        UNIT_ASSERT_VALUES_EQUAL(result.Code, EXIT_SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(result.Text,
                                 "Pile,PileState,DatabasesSync,DatabasesSyncing,DatabasesPending,"
                                 "DatabasesStatusUnavailable,DatabasesNonBridged,Operation,LastError\n"
                                 "dc2,SYNCHRONIZED,1/3,1,1,0,0,,\n");
    }

    Y_UNIT_TEST(PileOperationAndErrorStayInRow) {
        TBridgeListService service;
        service.Cluster.mutable_pile_states(1)->set_state(Ydb::Bridge::PileState::INACTIVE);
        auto* operation = service.Cluster.mutable_pending_pile_operation();
        operation->set_pile_name("dc2");
        operation->set_kind(Ydb::Bridge::PendingPileOperation::ADD);
        operation->set_error_reason("disk, unavailable");
        const auto result = RunList(service, {"piles", "--format", "csv"});
        UNIT_ASSERT_VALUES_EQUAL(result.Text,
                                 "Pile,PileState,DatabasesSync,Operation,LastError\n"
                                 "dc1,PRIMARY,1/3,,\n"
                                 "dc2,NOT_PROVISIONED,,provision-pile (in_progress),\"disk, unavailable\"\n");
    }

    Y_UNIT_TEST(RollbackAndGenerationFollowTable) {
        TBridgeListService service;
        auto* operation = service.Cluster.mutable_pending_pile_operation();
        operation->set_kind(Ydb::Bridge::PendingPileOperation::ROLLBACK);
        operation->set_error_reason("storage unavailable");
        const auto result = RunList(service, {});
        UNIT_ASSERT_STRING_CONTAINS(result.Text,
                                    "Generation: 118\nRollback: storage in progress\n"
                                    "Rollback last error (being retried): storage unavailable");
        UNIT_ASSERT(result.Text.find("Generation: 118") > result.Text.find("dc2"));
    }

    Y_UNIT_TEST(GenerationMismatchDoesNotShowStaleCounts) {
        TBridgeListService service;
        service.Summary.set_cluster_state_generation(117);
        const auto result = RunList(service, {"piles", "--format", "csv"});
        UNIT_ASSERT_VALUES_EQUAL(result.Code, EXIT_FAILURE);
        UNIT_ASSERT_VALUES_EQUAL(result.Text,
                                 "Pile,PileState,DatabasesSync,Operation,LastError\n"
                                 "dc1,PRIMARY,,,\ndc2,SYNCHRONIZED,,,\n");
    }

    Y_UNIT_TEST(UnavailableSummaryFails) {
        TBridgeListService service;
        service.SummaryStatus = Ydb::StatusIds::UNAVAILABLE;
        UNIT_ASSERT_VALUES_EQUAL(RunList(service, {}).Code, EXIT_FAILURE);
    }

    Y_UNIT_TEST(NonBridgedClusterWithPreparedConfiguration) {
        TBridgeListService service;
        service.Cluster.clear_pile_states();
        service.Cluster.mutable_unbridged_status()->add_bridge_config_node_ids(1);
        service.Cluster.mutable_unbridged_status()->add_bridge_config_node_ids(2);
        const auto result = RunList(service, {});
        UNIT_ASSERT_VALUES_EQUAL(result.Text, "Cluster mode: non-bridged\nBridge configuration: present on 2 nodes\n");
        UNIT_ASSERT_VALUES_EQUAL(service.SummaryCalls.load(), 0);
        const auto full = RunList(service, {"-A"});
        UNIT_ASSERT_STRING_CONTAINS(full.Text, "Node IDs with bridge configuration: 1, 2");
    }

    Y_UNIT_TEST(DatabaseConnectionIsNotAFilter) {
        TBridgeListService service;
        const auto result = RunList(service, {"databases", "--format", "csv"});
        UNIT_ASSERT_VALUES_EQUAL(result.Text,
                                 "Database,Mode,Pile,DataSync,GroupsSync,LastError\n"
                                 "/Root/db1,bridged,dc2,SYNCING,1/2,VDisk unavailable\n");
        UNIT_ASSERT(!service.DatabaseRequest.has_database_path());
        UNIT_ASSERT(!service.DatabaseRequest.has_pile_name());
    }

    Y_UNIT_TEST(DatabaseFiltersUseExistingApiEnums) {
        TBridgeListService service;
        RunList(service, {"databases", "--database-path", "/Root/db1", "--pile", "dc2", "--mode", "non-bridged", "--not-synced"});
        UNIT_ASSERT_VALUES_EQUAL(service.DatabaseRequest.database_path(), "/Root/db1");
        UNIT_ASSERT_VALUES_EQUAL(service.DatabaseRequest.pile_name(), "dc2");
        UNIT_ASSERT(service.DatabaseRequest.storage_mode() == Ydb::Bridge::ORDINARY);
        UNIT_ASSERT(service.DatabaseRequest.only_incomplete());
    }

    Y_UNIT_TEST(GroupColumnsAndIndependentFilters) {
        TBridgeListService service;
        const auto result = RunList(service, {"groups", "--columns", "PoolId,GroupId,Phase", "--pile", "dc2", "--format", "csv"});
        UNIT_ASSERT_VALUES_EQUAL(result.Text, "PoolId,GroupId,Phase\n1:2,2181038080,DATA\n");
        UNIT_ASSERT_VALUES_EQUAL(service.GroupRequest.pile_name(), "dc2");
        UNIT_ASSERT(!service.GroupRequest.has_database_path());
        RunList(service, {"groups", "--database-path", "/Root/db1"});
        UNIT_ASSERT_VALUES_EQUAL(service.GroupRequest.database_path(), "/Root/db1");
        UNIT_ASSERT(!service.GroupRequest.has_pile_name());
    }

    Y_UNIT_TEST(GroupFiltersIncludeUnavailableStatuses) {
        TBridgeListService service;
        RunList(service, {"groups", "--format", "csv"});
        UNIT_ASSERT(service.GroupRequest.scope_case() == Ydb::Bridge::ListGroupSynchronizationRequest::SCOPE_NOT_SET);
        UNIT_ASSERT(!service.GroupRequest.has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(service.GroupRequest.statuses_size(), 0);
        RunList(service, {"groups", "--not-synced", "--format", "csv"});
        UNIT_ASSERT_VALUES_EQUAL(service.GroupRequest.statuses_size(), 3);
        UNIT_ASSERT(service.GroupRequest.statuses(0) == Ydb::Bridge::SYNCING);
        UNIT_ASSERT(service.GroupRequest.statuses(1) == Ydb::Bridge::PENDING);
        UNIT_ASSERT(service.GroupRequest.statuses(2) == Ydb::Bridge::UNKNOWN);
    }

    Y_UNIT_TEST(NonBridgedAndUnavailableStatusLabels) {
        TBridgeListService service;
        service.Groups.mutable_groups(0)->set_sync_status(Ydb::Bridge::NOT_REQUIRED);
        auto result = RunList(service, {"groups", "--format", "csv"});
        UNIT_ASSERT_STRING_CONTAINS(result.Text, "NON_BRIDGED");
        service.Groups.mutable_groups(0)->set_sync_status(Ydb::Bridge::UNKNOWN);
        result = RunList(service, {"groups", "--format", "csv"});
        UNIT_ASSERT_STRING_CONTAINS(result.Text, "STATUS_UNAVAILABLE");
        service.Databases.mutable_databases(0)->set_storage_mode(Ydb::Bridge::ORDINARY);
        service.Databases.mutable_databases(0)->clear_groups();
        result = RunList(service, {"databases", "--format", "csv"});
        UNIT_ASSERT_STRING_CONTAINS(result.Text, "/Root/db1,non-bridged,dc2,NON_BRIDGED,,VDisk unavailable");
    }

    Y_UNIT_TEST(OversizedGroupResultFails) {
        TBridgeListService service;
        service.GroupStatus = Ydb::StatusIds::PRECONDITION_FAILED;
        UNIT_ASSERT_EXCEPTION_CONTAINS(RunList(service, {"groups"}), yexception, "diagnostic result exceeds API limit");
    }

    Y_UNIT_TEST(InvalidColumnSelection) {
        CheckInvalidColumns({"piles", "--columns", "GroupId"}, "Available columns: Pile, PileState");
        CheckInvalidColumns({"databases", "--columns", "Database,"}, "Available columns: Database, Mode");
        CheckInvalidColumns({"groups", "--columns", ""}, "Available columns:");
        CheckInvalidColumns({"groups", "--columns", "Pile", "--format", "json"}, "--format pretty or csv");
        CheckInvalidColumns({"piles", "-A", "--columns", "Pile"}, "can't appear together");
    }
}

} // namespace NYdb::NConsoleClient
