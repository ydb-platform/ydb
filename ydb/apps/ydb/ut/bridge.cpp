#include "mock_env.h"

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>
#include <library/cpp/json/json_reader.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/cast.h>

#include <atomic>
#include <optional>

namespace {

void CheckMetadata(TChecker& checker, grpc::ServerContext* context, const TString& name, const TString& expected) {
    const auto entry = context->client_metadata().find(name);
    if (expected.empty()) {
        if (entry != context->client_metadata().end()) {
            checker.AddFailure("Unexpected " + name + " metadata");
        }
    } else if (entry == context->client_metadata().end() || entry->second != expected) {
        checker.AddFailure("Incorrect " + name + " metadata");
    }
}

class TBridgeLogin : public TAuthImpl {
public:
    TString Database = TEST_DATABASE;
    std::atomic<size_t> Calls{0};

    grpc::Status Login(grpc::ServerContext* context, const Ydb::Auth::LoginRequest* request, Ydb::Auth::LoginResponse* response) override {
        ++Calls;
        CheckMetadata(*this, context, "x-ydb-database", Database);
        return TAuthImpl::Login(context, request, response);
    }
};

class TBridgeDiscovery : public TDiscoveryImpl {
public:
    explicit TBridgeDiscovery(const TString& endpoint)
        : TDiscoveryImpl("localhost", FromString<ui16>(TStringBuf(endpoint).RNextTok(':')))
    {}

    TString Database = TEST_DATABASE;
    std::atomic<size_t> Calls{0};

    grpc::Status ListEndpoints(grpc::ServerContext* context,
                               const Ydb::Discovery::ListEndpointsRequest* request,
                               Ydb::Discovery::ListEndpointsResponse* response) override {
        ++Calls;
        CHECK_EXP(request->database() == Database, "Discovery must use the connection database");
        CheckMetadata(*this, context, "x-ydb-database", Database);
        return TDiscoveryImpl::ListEndpoints(context, request, response);
    }
};


template <class TRequest, class TResult>
struct TDiagnosticReply {
    std::optional<TRequest> ExpectedRequest;
    TResult Result;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    TString Issue;
    grpc::Status RpcStatus;
    std::atomic<size_t> Calls{0};
};

class TBridgeService : public TMockGrpcServiceBase<Ydb::Bridge::V1::BridgeService::Service> {
public:
    TBridgeService() {
        ClusterResult.set_generation(42);
        auto* primary = ClusterResult.add_pile_states();
        primary->set_pile_name("primary");
        primary->set_state(Ydb::Bridge::PileState::PRIMARY);
        auto* secondary = ClusterResult.add_pile_states();
        secondary->set_pile_name("secondary");
        secondary->set_state(Ydb::Bridge::PileState::SYNCHRONIZED);
    }

    TString Database = TEST_DATABASE;
    bool Bridged = true;
    TString RetainPile;
    Ydb::Bridge::GetClusterStateResult ClusterResult;
    bool ClusterStateNotFound = false;
    std::atomic<size_t> ClusterCalls{0};
    std::atomic<size_t> UnbridgedStatusRequests{0};
    TDiagnosticReply<Ydb::Bridge::GetRollbackStatusRequest, Ydb::Bridge::GetRollbackStatusResult> LegacyRollbackStatus;
    TDiagnosticReply<Ydb::Bridge::ListPileSynchronizationRequest, Ydb::Bridge::ListPileSynchronizationResult> Summary;
    TDiagnosticReply<Ydb::Bridge::ListDatabaseSynchronizationRequest, Ydb::Bridge::ListDatabaseSynchronizationResult> Databases;
    TDiagnosticReply<Ydb::Bridge::ListGroupSynchronizationRequest, Ydb::Bridge::ListGroupSynchronizationResult> Groups;
    std::atomic<size_t> ModeCalls{0};
    std::atomic<size_t> JoinCalls{0};
    std::atomic<size_t> ProvisionCalls{0};
    std::atomic<size_t> DestroyCalls{0};
    std::atomic<size_t> RollbackCalls{0};
    std::atomic<bool> SkipBscValidation{false};

    grpc::Status GetClusterState(grpc::ServerContext* context, const Ydb::Bridge::GetClusterStateRequest* request,
            Ydb::Bridge::GetClusterStateResponse* response) override {
        ++ClusterCalls;
        if (request->include_unbridged_status()) {
            ++UnbridgedStatusRequests;
        }
        const auto status = ReplySuccess(context, response);
        auto* operation = response->mutable_operation();
        if (ClusterStateNotFound) {
            operation->set_status(Ydb::StatusIds::NOT_FOUND);
            operation->add_issues()->set_message("Bridge cluster state is not configured");
        } else {
            operation->mutable_result()->PackFrom(ClusterResult);
        }
        return status;
    }

    grpc::Status GetRollbackStatus(grpc::ServerContext* context, const Ydb::Bridge::GetRollbackStatusRequest* request,
            Ydb::Bridge::GetRollbackStatusResponse* response) override {
        return ReplyDiagnostic(context, *request, *response, LegacyRollbackStatus);
    }

    grpc::Status ListPileSynchronization(grpc::ServerContext* context, const Ydb::Bridge::ListPileSynchronizationRequest* request,
            Ydb::Bridge::ListPileSynchronizationResponse* response) override {
        return ReplyDiagnostic(context, *request, *response, Summary);
    }

    grpc::Status ListDatabaseSynchronization(grpc::ServerContext* context, const Ydb::Bridge::ListDatabaseSynchronizationRequest* request,
            Ydb::Bridge::ListDatabaseSynchronizationResponse* response) override {
        return ReplyDiagnostic(context, *request, *response, Databases);
    }

    grpc::Status ListGroupSynchronization(grpc::ServerContext* context, const Ydb::Bridge::ListGroupSynchronizationRequest* request,
            Ydb::Bridge::ListGroupSynchronizationResponse* response) override {
        return ReplyDiagnostic(context, *request, *response, Groups);
    }

    grpc::Status SetDatabaseBridgeMode(grpc::ServerContext* context,
                                       const Ydb::Bridge::SetDatabaseBridgeModeRequest* request,
                                       Ydb::Bridge::SetDatabaseBridgeModeResponse* response) override {
        ++ModeCalls;
        CHECK_EXP(request->database_path() == "/Root/app", "RPC must contain the explicit target database");
        CHECK_EXP(request->has_bridge_mode() && request->bridge_mode() == Bridged, "Incorrect database bridge mode");
        CHECK_EXP(request->retain_pile_name() == RetainPile, "Incorrect pile to retain");
        return ReplySuccess(context, response);
    }

    grpc::Status UpdateClusterState(grpc::ServerContext* context,
                                    const Ydb::Bridge::UpdateClusterStateRequest* request,
                                    Ydb::Bridge::UpdateClusterStateResponse* response) override {
        ++JoinCalls;
        CHECK_EXP(request->updates_size() == 1 &&
                  request->updates(0).pile_name() == "secondary" &&
                  request->updates(0).state() == Ydb::Bridge::PileState::NOT_SYNCHRONIZED,
                  "Joining must mark the requested pile as not synchronized");
        return ReplySuccess(context, response);
    }

    grpc::Status AddPile(grpc::ServerContext* context,
                         const Ydb::Bridge::AddPileRequest* request,
                         Ydb::Bridge::AddPileResponse* response) override {
        ++ProvisionCalls;
        CHECK_EXP(request->pile_name() == "secondary", "Incorrect pile to provision");
        return ReplySuccess(context, response);
    }

    grpc::Status RemovePile(grpc::ServerContext* context,
                            const Ydb::Bridge::RemovePileRequest* request,
                            Ydb::Bridge::RemovePileResponse* response) override {
        ++DestroyCalls;
        CHECK_EXP(request->pile_name() == "secondary", "Incorrect pile to destroy");
        return ReplySuccess(context, response);
    }

    grpc::Status Rollback(grpc::ServerContext* context,
                          const Ydb::Bridge::RollbackRequest* request,
                          Ydb::Bridge::RollbackResponse* response) override {
        ++RollbackCalls;
        CHECK_EXP(request->skip_bsc_validation() == SkipBscValidation, "Incorrect skip_bsc_validation");
        return ReplySuccess(context, response);
    }

private:
    template <class TRequest, class TResult, class TResponse>
    grpc::Status ReplyDiagnostic(grpc::ServerContext* context, const TRequest& request, TResponse& response,
            TDiagnosticReply<TRequest, TResult>& reply) {
        ++reply.Calls;
        const auto* expected = reply.ExpectedRequest ? &*reply.ExpectedRequest : nullptr;
        CHECK_EXP(expected, "Unexpected diagnostics RPC");
        if (expected) {
            auto actual = request;
            actual.clear_operation_params();
            auto wanted = *expected;
            wanted.clear_operation_params();
            CHECK_EXP(actual.SerializeAsString() == wanted.SerializeAsString(), "Incorrect request: " << actual.ShortDebugString());
        }
        ReplySuccess(context, &response);
        auto* operation = response.mutable_operation();
        operation->set_status(reply.Status);
        if (!reply.Issue.empty()) {
            operation->add_issues()->set_message(reply.Issue);
        }
        if (reply.Status == Ydb::StatusIds::SUCCESS) {
            operation->mutable_result()->PackFrom(reply.Result);
        }
        return reply.RpcStatus;
    }

    template <class TResponse>
    grpc::Status ReplySuccess(grpc::ServerContext* context, TResponse* response) {
        CheckMetadata(*this, context, "x-ydb-database", Database);
        CheckMetadata(*this, context, "x-ydb-auth-ticket", "bridge-admin-token");
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }
};

class TBridgeCliFixture : public TCliTestFixture {
public:
    void AddServices() override {
        EnvHomeFile("ydb/bin/config.json", R"({"check_version":false})");
        AddService<TBridgeDiscovery>(GetAddress());
        AddService<TBridgeLogin>();
        AddService<TBridgeService>();
    }

    void ExpectLogin() {
        Service<TBridgeLogin>().ExpectUserAndPassword("bridge-admin", "password");
        Service<TBridgeLogin>().SetToken("bridge-admin-token");
    }

    TString RunList(TList<TString> options = {}, TString* stderrOutput = nullptr) {
        TList<TString> args = {"-e", GetEndpoint(), "-d", GetDatabase(), "admin", "cluster", "bridge", "list"};
        args.insert(args.end(), options.begin(), options.end());
        return RunCli(std::move(args), {{"YDB_TOKEN", "bridge-admin-token"}}, {}, stderrOutput);
    }

    template <class TResult>
    void SetObservation(TResult& result) {
        result.set_cluster_state_generation(42);
        result.mutable_observed_at()->set_seconds(1);
    }

    Ydb::Bridge::UnbridgedStatus& PrepareUnbridgedStatus() {
        auto& result = Service<TBridgeService>().ClusterResult;
        result.clear_generation();
        result.clear_pile_states();
        return *result.mutable_unbridged_status();
    }

    Ydb::Bridge::GetRollbackStatusResult& PrepareLegacyRollbackStatus() {
        auto& service = Service<TBridgeService>();
        service.ClusterStateNotFound = true;
        service.LegacyRollbackStatus.ExpectedRequest.emplace();
        return service.LegacyRollbackStatus.Result;
    }

    Ydb::Bridge::ListPileSynchronizationResult& PrepareSummary() {
        auto& reply = Service<TBridgeService>().Summary;
        reply.ExpectedRequest.emplace();
        SetObservation(reply.Result);
        return reply.Result;
    }

    Ydb::Bridge::ListDatabaseSynchronizationResult& PrepareDatabases() {
        auto& reply = Service<TBridgeService>().Databases;
        reply.ExpectedRequest.emplace();
        SetObservation(reply.Result);
        return reply.Result;
    }

    Ydb::Bridge::ListGroupSynchronizationResult& PrepareGroups() {
        auto& reply = Service<TBridgeService>().Groups;
        reply.ExpectedRequest.emplace();
        reply.ExpectedRequest->set_database_path("/Root/app");
        SetObservation(reply.Result);
        return reply.Result;
    }

    void SetPool(Ydb::Bridge::StoragePool* pool, const TString& name = "pool,ssd", ui64 box = 1, ui64 id = 2) {
        pool->mutable_id()->set_box_id(box);
        pool->mutable_id()->set_pool_id(id);
        pool->set_name(name);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(BridgeCli) {
    Y_UNIT_TEST_F(DefaultListOutputIsUnchanged, TBridgeCliFixture) {
        const TString expected = "Pile primary: PRIMARY\nPile secondary: SYNCHRONIZED\nGeneration: 42\n";
        UNIT_ASSERT_VALUES_EQUAL(RunList(), expected);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(RunList({"piles"}), expected);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Summary.Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Databases.Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Groups.Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().UnbridgedStatusRequests.load(), 2);
    }

    Y_UNIT_TEST_F(DefaultListMachineFormatsAndPileFilterAreUnchanged, TBridgeCliFixture) {
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["generation"].GetUInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(json["piles"].GetArray().size(), 2);
        UNIT_ASSERT(!json.Has("synchronization_summary"));
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--format", "csv"}),
            "pile_name,state,generation\nprimary,PRIMARY,42\nsecondary,SYNCHRONIZED,42\n");
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--pile", "secondary"}), "Pile secondary: SYNCHRONIZED\nGeneration: 42\n");
        auto& service = Service<TBridgeService>();
        service.ClusterResult.mutable_pile_states(1)->set_pile_name("");
        UNIT_ASSERT_VALUES_EQUAL(RunList({"piles", "--pile", "", "--format", "csv"}),
            "pile_name,state,generation\n,SYNCHRONIZED,42\n");
        UNIT_ASSERT_VALUES_EQUAL(service.ClusterCalls.load(), 4);
        UNIT_ASSERT_VALUES_EQUAL(service.UnbridgedStatusRequests.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(service.Summary.Calls.load(), 0);
    }

    Y_UNIT_TEST_F(PileListPreservesPendingRollback, TBridgeCliFixture) {
        auto& service = Service<TBridgeService>();
        auto* pending = service.ClusterResult.mutable_pending_pile_operation();
        pending->set_pile_name("primary");
        pending->set_kind(Ydb::Bridge::PendingPileOperation::ROLLBACK);
        pending->set_error_reason("storage controller unavailable");

        UNIT_ASSERT_VALUES_EQUAL(RunList({"piles"}),
            "Pile primary: PRIMARY\nPile secondary: SYNCHRONIZED\nGeneration: 42\n"
            "Rollback: storage in progress\n"
            "Last attempt has failed (being retried): storage controller unavailable\n");

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"piles", "--format", "json"}), &json));
        const auto& operation = json["pending_pile_operation"];
        UNIT_ASSERT_VALUES_EQUAL(operation["pile_name"].GetString(), "primary");
        UNIT_ASSERT_VALUES_EQUAL(operation["kind"].GetString(), "ROLLBACK");
        UNIT_ASSERT_VALUES_EQUAL(operation["error_reason"].GetString(), "storage controller unavailable");
        UNIT_ASSERT_VALUES_EQUAL(service.ClusterCalls.load(), 2);
    }

    Y_UNIT_TEST_F(ListHelpShowsViewsAndOptions, TBridgeCliFixture) {
        for (const auto* option : {"--help", "-hh"}) {
            const auto help = RunList({option});
            for (const auto* item : {"[piles|databases|groups]", "--detailed", "--pile", "--format", "--database-path"}) {
                UNIT_ASSERT_STRING_CONTAINS(help, item);
            }
            UNIT_ASSERT_STRING_CONTAINS(help, "Defaults to piles.");
        }
    }

    Y_UNIT_TEST_F(InvalidListOptionsAreRejectedBeforeRpc, TBridgeCliFixture) {
        const TVector<TList<TString>> invalid = {
            {"unknown"}, {"rollback"}, {"databases", "groups"}, {"--database-path", "/Root/app"}, {"--pool", "pool"},
            {"piles", "--database-path", "/Root/app"},
            {"databases", "--pool", "pool"}, {"groups", "--database-path", "/Root/app", "--pool", "pool"},
            {"--not-synced"}, {"--detailed", "--not-synced"},
            {"--mode", "ordinary"}, {"groups", "--database-path", "/Root/app", "--mode", "ordinary"},
            {"databases", "--mode", "unknown"},
            {"databases", "--page-size", "10"}, {"groups", "--database-path", "/Root/app", "--page-token", "previous"},
            {"groups", "--database-path", "relative/path"}, {"databases", "--database-path", ""}, {"groups", "--pool", ""},
            {"--pool-id", "1:2"}, {"databases", "--pool-id", "1:2"},
            {"groups", "--pool", "pool", "--pool-id", "1:2"},
            {"groups", "--database-path", "/Root/app", "--pool-id", "1:2"},
            {"groups", "--pool-id", "1:0"}, {"groups", "--pool-id", ":2"}, {"groups", "--pool-id", "1:"},
            {"groups", "--pool-id", "1:2:3"}, {"groups", "--pool-id", "-1:2"},
            {"groups", "--pool-id", "18446744073709551616:2"}, {"groups", "--pool-id", "1:+2"},
        };
        for (const auto& options : invalid) {
            ExpectFail();
            RunList(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Summary.Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Databases.Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Groups.Calls.load(), 0);
    }

    Y_UNIT_TEST_F(DetailedPileViewMergesSynchronizationSummary, TBridgeCliFixture) {
        auto& summary = PrepareSummary();
        summary.set_unattributed_storage_pools(2);
        auto* pile = summary.add_piles();
        pile->set_pile_name("secondary");
        pile->mutable_databases()->set_total(3);
        pile->mutable_databases()->set_synced(1);
        pile->mutable_databases()->set_syncing(1);
        pile->mutable_databases()->set_not_required(1);
        auto* unplaced = summary.add_piles();
        unplaced->mutable_databases()->set_total(1);
        unplaced->mutable_databases()->set_not_required(1);
        const auto output = RunList({"--detailed", "--pile", "secondary"});
        UNIT_ASSERT_STRING_CONTAINS(output, "1/3");
        UNIT_ASSERT(!output.Contains("Databases in cluster:"));
        UNIT_ASSERT_STRING_CONTAINS(output, "Database counts exclude storage pools without database attribution: 2");
        for (const auto* header : {"Synced/total databases", "Syncing databases", "Pending databases", "Not required databases", "Unknown databases"}) {
            UNIT_ASSERT_STRING_CONTAINS(output, header);
        }
        UNIT_ASSERT(!output.Contains("primary"));
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--detailed", "--pile", "secondary", "--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["piles"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["synchronization_summary"]["cluster_state_generation"].GetUInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(json["synchronization_summary"]["unattributed_storage_pools"].GetUInteger(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["synchronization_summary"]["piles"].GetArray().size(), 1);
        const auto& counts = json["synchronization_summary"]["piles"][0]["databases"];
        UNIT_ASSERT_VALUES_EQUAL(counts["total"].GetUInteger(), 3);
        UNIT_ASSERT_VALUES_EQUAL(counts["not_required"].GetUInteger(), 1);
        UNIT_ASSERT(!json.Has("storage_state"));
        const auto csv = RunList({"piles", "--detailed", "--pile", "secondary", "--format", "csv"});
        UNIT_ASSERT_STRING_CONTAINS(csv, "pile_name_present");
        UNIT_ASSERT_STRING_CONTAINS(csv, "databases_not_required");
        UNIT_ASSERT(!csv.Contains("false"));
        const auto all = RunList({"--detailed", "--format", "csv"});
        UNIT_ASSERT_STRING_CONTAINS(all, ",false,,0,1,0,0,1,0,2,42\n");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Summary.Calls.load(), 4);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 4);
    }

    Y_UNIT_TEST_F(SummaryGenerationMismatchRetainsOnlyBaselineState, TBridgeCliFixture) {
        PrepareSummary().set_cluster_state_generation(43);
        ExpectFail();
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--detailed", "--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["generation"].GetUInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(json["piles"].GetArray().size(), 2);
        UNIT_ASSERT(!json.Has("synchronization_summary"));
        UNIT_ASSERT_STRING_CONTAINS(json["incomplete_reason"].GetString(), "differs from cluster-state generation");
    }

    Y_UNIT_TEST_F(OlderServerKeepsBaselineStateAndFailsDetailedDiagnostics, TBridgeCliFixture) {
        PrepareSummary();
        Service<TBridgeService>().Summary.RpcStatus = grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "old server");
        ExpectFail();
        TString error;
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--detailed"}, &error),
            "Pile primary: PRIMARY\nPile secondary: SYNCHRONIZED\nGeneration: 42\n");
        UNIT_ASSERT_STRING_CONTAINS(error, "old server");
    }

    Y_UNIT_TEST_F(UnavailableDetailedCsvRetainsPileStateWithoutInventingCounters, TBridgeCliFixture) {
        PrepareSummary();
        Service<TBridgeService>().Summary.Status = Ydb::StatusIds::UNAVAILABLE;
        Service<TBridgeService>().Summary.Issue = "controller unavailable";
        ExpectFail();
        TString error;
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--detailed", "--format", "csv"}, &error),
            "pile_name,pile_name_present,state,databases_synced,databases_total,databases_syncing,databases_pending,databases_not_required,databases_unknown,cluster_unattributed_storage_pools,generation\n"
            "primary,true,PRIMARY,,,,,,,,42\nsecondary,true,SYNCHRONIZED,,,,,,,,42\n");
        UNIT_ASSERT_STRING_CONTAINS(error, "controller unavailable");
    }

    Y_UNIT_TEST_F(DatabaseViewForwardsFiltersWithoutClusterState, TBridgeCliFixture) {
        auto& result = PrepareDatabases();
        auto& expected = *Service<TBridgeService>().Databases.ExpectedRequest;
        expected.set_database_path("/Root/serverless");
        expected.set_pile_name("secondary");
        expected.set_only_incomplete(true);
        expected.set_storage_mode(Ydb::Bridge::MIXED);
        result.set_storage_owner_database_path("/Root/shared");
        auto* row = result.add_databases();
        row->set_database_path("/Root/shared");
        row->set_storage_mode(Ydb::Bridge::MIXED);
        row->set_pile_name("secondary");
        SetPool(row->add_storage_pools(), "pool,ssd", 1, 2);
        SetPool(row->add_storage_pools(), "pool,ssd", 3, 2);
        row->mutable_groups()->set_required(8);
        row->mutable_groups()->set_synced(3);
        row->mutable_groups()->set_syncing(5);
        row->mutable_last_sync_error()->set_message("retrying");
        row->mutable_last_sync_error()->mutable_occurred_at()->set_nanos(500000000);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--format", "json", "databases", "--database-path", "/Root/serverless",
            "--pile", "secondary", "--not-synced", "--mode", "mixed"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["storage_owner_database_path"].GetString(), "/Root/shared");
        UNIT_ASSERT(!json.Has("next_page_token"));
        UNIT_ASSERT_VALUES_EQUAL(json["observed_at"].GetString(), "1970-01-01T00:00:01.000000Z");
        const auto& database = json["databases"][0];
        UNIT_ASSERT_VALUES_EQUAL(database["storage_pools"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(database["storage_pools"][0]["id"]["box_id"].GetUInteger(), 1);
        UNIT_ASSERT_VALUES_EQUAL(database["storage_pools"][1]["id"]["box_id"].GetUInteger(), 3);
        UNIT_ASSERT_VALUES_EQUAL(database["storage_mode"].GetString(), "mixed");
        UNIT_ASSERT_VALUES_EQUAL(database["groups"]["syncing"].GetUInteger(), 5);
        UNIT_ASSERT_VALUES_EQUAL(database["last_sync_error"]["occurred_at"].GetString(), "1970-01-01T00:00:00.500000Z");
        UNIT_ASSERT(!database.Has("state"));
        UNIT_ASSERT(!json.Has("storage_state"));
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Databases.Calls.load(), 1);
    }

    Y_UNIT_TEST_F(DatabaseFormattingUsesCountsAndPreservesPoolIdentity, TBridgeCliFixture) {
        auto* row = PrepareDatabases().add_databases();
        row->set_database_path("/Root/app");
        row->set_storage_mode(Ydb::Bridge::BRIDGED);
        row->set_pile_name("secondary");
        SetPool(row->add_storage_pools());
        row->mutable_groups()->set_required(8);
        row->mutable_groups()->set_synced(3);
        row->mutable_groups()->set_syncing(2);
        row->mutable_groups()->set_pending(1);
        row->mutable_groups()->set_unknown(2);
        row->mutable_last_sync_error()->set_message("retrying");
        row->mutable_last_sync_error()->mutable_occurred_at()->set_nanos(500000000);
        UNIT_ASSERT_VALUES_EQUAL(RunList({"databases", "--detailed", "--format", "csv"}),
            "database_path,storage_pools,storage_mode,pile_name,pile_name_present,groups_synced,groups_required,groups_syncing,groups_pending,groups_unknown,last_sync_error,last_sync_error_occurred_at\n"
            "/Root/app,\"[{\"\"id\"\":{\"\"box_id\"\":1,\"\"pool_id\"\":2},\"\"name\"\":\"\"pool,ssd\"\"}]\",bridged,secondary,true,3,8,2,1,2,retrying,1970-01-01T00:00:00.500000Z\n");
        const auto pretty = RunList({"databases", "--detailed"});
        UNIT_ASSERT_STRING_CONTAINS(pretty, "SYNCING");
        UNIT_ASSERT_STRING_CONTAINS(pretty, "3/8");
        for (const auto* header : {"Synced groups", "Syncing groups", "Pending groups", "Unknown groups"}) {
            UNIT_ASSERT_STRING_CONTAINS(pretty, header);
        }
        UNIT_ASSERT_STRING_CONTAINS(pretty, "pool,ssd");
        UNIT_ASSERT_STRING_CONTAINS(pretty, "1970-01-01T00:00:00.500000Z");
        UNIT_ASSERT(!RunList({"databases", "--format", "csv"}).Contains("last_sync_error"));
    }

    Y_UNIT_TEST_F(OrdinaryStoragePreservesAbsentOwnerAndPile, TBridgeCliFixture) {
        auto* row = PrepareDatabases().add_databases();
        SetPool(row->add_storage_pools(), "ordinary");
        row->set_storage_mode(Ydb::Bridge::ORDINARY);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"databases", "--format", "json"}), &json));
        const auto& database = json["databases"][0];
        UNIT_ASSERT(!database.Has("database_path"));
        UNIT_ASSERT(!database.Has("pile_name"));
        UNIT_ASSERT_VALUES_EQUAL(database["groups"]["required"].GetUInteger(), 0);
        UNIT_ASSERT_STRING_CONTAINS(RunList({"databases"}), "NOT_REQUIRED");
        UNIT_ASSERT_STRING_CONTAINS(RunList({"databases", "--format", "csv"}), ",ordinary,,false,0,0\n");
        Service<TBridgeService>().Databases.ExpectedRequest->set_storage_mode(Ydb::Bridge::ORDINARY);
        UNIT_ASSERT_STRING_CONTAINS(RunList({"databases", "--mode", "ordinary"}), "NOT_REQUIRED");
    }

    Y_UNIT_TEST_F(GroupViewPreservesEmptyPileAndPhysicalGroupPresence, TBridgeCliFixture) {
        auto& result = PrepareGroups();
        auto* placed = result.add_groups();
        placed->set_group_id(123);
        SetPool(placed->mutable_storage_pool());
        placed->set_pile_name("");
        placed->set_sync_status(Ydb::Bridge::UNKNOWN);
        auto* unplaced = result.add_groups();
        unplaced->set_group_id(124);
        SetPool(unplaced->mutable_storage_pool());
        unplaced->set_sync_status(Ydb::Bridge::NOT_REQUIRED);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"groups", "--database-path", "/Root/app", "--format", "json"}), &json));
        UNIT_ASSERT(json["groups"][0].Has("pile_name"));
        UNIT_ASSERT(json["groups"][0]["pile_name"].GetString().empty());
        UNIT_ASSERT(!json["groups"][0].Has("physical_group_id"));
        UNIT_ASSERT(!json["groups"][1].Has("pile_name"));
        const auto csv = RunList({"groups", "--database-path", "/Root/app", "--format", "csv"});
        UNIT_ASSERT(csv.StartsWith("group_id,storage_pool,pile_name,pile_name_present,sync_status\n"));
        UNIT_ASSERT_STRING_CONTAINS(csv, ",,true,UNKNOWN\n");
        UNIT_ASSERT_STRING_CONTAINS(csv, ",,false,NOT_REQUIRED\n");
        UNIT_ASSERT(!csv.Contains("physical_group_id"));
        UNIT_ASSERT_STRING_CONTAINS(RunList({"groups", "--database-path", "/Root/app"}), "NOT_REQUIRED");
        result.mutable_groups()->RemoveLast();
        Service<TBridgeService>().Groups.ExpectedRequest->set_pile_name("");
        UNIT_ASSERT_STRING_CONTAINS(RunList({"groups", "--database-path", "/Root/app", "--pile", ""}), "\"\"");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 0);
    }

    Y_UNIT_TEST_F(DetailedGroupCsvEscapesFieldsAndKeepsRawPhase, TBridgeCliFixture) {
        auto& result = PrepareGroups();
        Service<TBridgeService>().Groups.ExpectedRequest->set_storage_pool_name("pool,ssd");
        Service<TBridgeService>().Groups.ExpectedRequest->set_pile_name("secondary");
        auto* row = result.add_groups();
        row->set_group_id(123);
        row->set_physical_group_id(456);
        SetPool(row->mutable_storage_pool());
        row->set_pile_name("secondary");
        row->set_sync_status(Ydb::Bridge::SYNCING);
        row->set_sync_phase("WRITE_KEEP_BARRIER_DONOTKEEP_DATA");
        row->mutable_last_sync_error()->set_message("retry \"dc,2\"\r\nlater");
        row->mutable_last_sync_error()->mutable_occurred_at()->set_nanos(77000);
        TString error;
        UNIT_ASSERT_VALUES_EQUAL(RunList({"groups", "--pool", "pool,ssd", "--pile", "secondary", "--detailed", "--format", "csv"}, &error),
            "group_id,storage_pool,pile_name,pile_name_present,sync_status,physical_group_id,sync_phase,last_sync_error,last_sync_error_occurred_at\n"
            "123,\"{\"\"id\"\":{\"\"box_id\"\":1,\"\"pool_id\"\":2},\"\"name\"\":\"\"pool,ssd\"\"}\",secondary,true,SYNCING,456,WRITE_KEEP_BARRIER_DONOTKEEP_DATA,\"retry \"\"dc,2\"\"\r\nlater\",1970-01-01T00:00:00.000077Z\n");
        UNIT_ASSERT(!error.Contains("Next page token:"));
        const auto pretty = RunList({"groups", "--pool", "pool,ssd", "--pile", "secondary", "--detailed"});
        UNIT_ASSERT_STRING_CONTAINS(pretty, "Physical group");
        UNIT_ASSERT_STRING_CONTAINS(pretty, "DATA");
        UNIT_ASSERT(!pretty.Contains("WRITE_KEEP"));
    }

    Y_UNIT_TEST_F(QualifiedPoolSelectorAllowsBoxZero, TBridgeCliFixture) {
        auto& result = PrepareGroups();
        auto* id = Service<TBridgeService>().Groups.ExpectedRequest->mutable_storage_pool_id();
        id->set_box_id(0);
        id->set_pool_id(42);
        auto* group = result.add_groups();
        group->set_group_id(123);
        SetPool(group->mutable_storage_pool(), "ambiguous", 0, 42);
        group->set_sync_status(Ydb::Bridge::SYNCED);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"groups", "--pool-id", "0:42", "--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["groups"][0]["storage_pool"]["id"]["box_id"].GetUInteger(), 0);
        UNIT_ASSERT_VALUES_EQUAL(json["groups"][0]["storage_pool"]["id"]["pool_id"].GetUInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Groups.Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 0);
    }

    Y_UNIT_TEST_F(GroupViewWithoutScopeIncludesGroupsWithoutPool, TBridgeCliFixture) {
        auto& result = PrepareGroups();
        auto& expected = *Service<TBridgeService>().Groups.ExpectedRequest;
        expected.clear_scope();
        expected.set_pile_name("secondary");
        for (const auto status : {Ydb::Bridge::SYNCING, Ydb::Bridge::PENDING, Ydb::Bridge::UNKNOWN}) {
            expected.add_statuses(status);
        }
        auto* row = result.add_groups();
        row->set_group_id(0);
        row->set_physical_group_id(0);
        row->set_pile_name("secondary");
        row->set_sync_status(Ydb::Bridge::SYNCING);
        const auto pretty = RunList({"groups", "--pile", "secondary", "--not-synced"});
        UNIT_ASSERT_STRING_CONTAINS(pretty, "SYNCING");
        UNIT_ASSERT_STRING_CONTAINS(pretty, "—");
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"groups", "--pile", "secondary", "--not-synced", "--format", "json"}), &json));
        const auto& group = json["groups"][0];
        UNIT_ASSERT_VALUES_EQUAL(group["group_id"].GetUInteger(), 0);
        UNIT_ASSERT_VALUES_EQUAL(group["physical_group_id"].GetUInteger(), 0);
        UNIT_ASSERT(!group.Has("storage_pool"));
        UNIT_ASSERT_STRING_CONTAINS(RunList({"groups", "--pile", "secondary", "--not-synced", "--format", "csv"}),
            "0,,secondary,true,SYNCING\n");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 0);
    }

    Y_UNIT_TEST_F(GroupViewForwardsScopeAndStatusesForFullList, TBridgeCliFixture) {
        auto& result = PrepareGroups();
        auto& reply = Service<TBridgeService>().Groups;
        reply.ExpectedRequest->set_pile_name("secondary");
        for (const auto status : {Ydb::Bridge::SYNCING, Ydb::Bridge::PENDING, Ydb::Bridge::UNKNOWN}) {
            reply.ExpectedRequest->add_statuses(status);
        }
        for (const auto groupId : {123u, 124u}) {
            auto* group = result.add_groups();
            group->set_group_id(groupId);
            SetPool(group->mutable_storage_pool());
            group->set_pile_name("secondary");
            group->set_sync_status(Ydb::Bridge::PENDING);
        }
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"groups", "--database-path", "/Root/app", "--pile", "secondary",
            "--not-synced", "--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["groups"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["groups"][1]["group_id"].GetUInteger(), 124);
        UNIT_ASSERT(!json.Has("next_page_token"));
        UNIT_ASSERT(!json.Has("incomplete_reason"));
        UNIT_ASSERT_VALUES_EQUAL(reply.Calls.load(), 1);
    }

    Y_UNIT_TEST_F(GroupViewReturnsAllRowsInOneRpc, TBridgeCliFixture) {
        auto& result = PrepareGroups();
        for (ui32 i = 0; i < 1001; ++i) {
            auto* group = result.add_groups();
            group->set_group_id(100 + i);
            SetPool(group->mutable_storage_pool());
            group->set_pile_name("secondary");
            group->set_sync_status(Ydb::Bridge::SYNCED);
        }
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"groups", "--database-path", "/Root/app", "--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["groups"].GetArray().size(), 1001);
        UNIT_ASSERT_VALUES_EQUAL(json["groups"][1000]["group_id"].GetUInteger(), 1100);
        UNIT_ASSERT(!json.Has("next_page_token"));
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Groups.Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 0);
    }

    Y_UNIT_TEST_F(EmptyDatabaseListKeepsResolvedSharedStorageOwner, TBridgeCliFixture) {
        auto& result = PrepareDatabases();
        auto& reply = Service<TBridgeService>().Databases;
        reply.ExpectedRequest->set_database_path("/Root/serverless");
        result.set_storage_owner_database_path("/Root/shared");
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"databases", "--database-path", "/Root/serverless", "--format", "json"}), &json));
        UNIT_ASSERT(json["databases"].GetArray().empty());
        UNIT_ASSERT_VALUES_EQUAL(json["storage_owner_database_path"].GetString(), "/Root/shared");
        UNIT_ASSERT(!json.Has("next_page_token"));
        UNIT_ASSERT(!json.Has("incomplete_reason"));
        const auto pretty = RunList({"databases", "--database-path", "/Root/serverless"});
        UNIT_ASSERT_STRING_CONTAINS(pretty, "Shared storage: /Root/shared\n");
        UNIT_ASSERT_VALUES_EQUAL(reply.Calls.load(), 2);
    }

    Y_UNIT_TEST_F(CanonicalDatabasePathDoesNotIndicateSharedStorage, TBridgeCliFixture) {
        auto& result = PrepareDatabases();
        auto& reply = Service<TBridgeService>().Databases;
        reply.ExpectedRequest->set_database_path("/Root/app/");
        result.set_storage_owner_database_path("/Root/app");

        const auto pretty = RunList({"databases", "--database-path", "/Root/app/"});
        UNIT_ASSERT_STRING_CONTAINS(pretty, "Database: /Root/app/\n");
        UNIT_ASSERT(!pretty.Contains("Shared storage:"));
        UNIT_ASSERT_VALUES_EQUAL(reply.Calls.load(), 1);
    }

    Y_UNIT_TEST_F(OversizedListsFailWithoutPrintingPartialData, TBridgeCliFixture) {
        PrepareDatabases();
        auto& service = Service<TBridgeService>();
        service.Databases.Status = Ydb::StatusIds::PRECONDITION_FAILED;
        service.Databases.Issue = "Result limit exceeded; narrow database/pool/pile/status filter";
        ExpectFail();
        TString error;
        UNIT_ASSERT(RunList({"databases", "--format", "json"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "narrow database/pool/pile/status filter");
        PrepareGroups();
        service.Groups.Status = Ydb::StatusIds::PRECONDITION_FAILED;
        service.Groups.Issue = service.Databases.Issue;
        ExpectFail();
        UNIT_ASSERT(RunList({"groups", "--database-path", "/Root/app", "--format", "csv"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "narrow database/pool/pile/status filter");
        UNIT_ASSERT_VALUES_EQUAL(service.Databases.Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(service.Groups.Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(service.ClusterCalls.load(), 0);
    }

    Y_UNIT_TEST_F(InitialDiagnosticErrorDoesNotPrintSuccessfulState, TBridgeCliFixture) {
        PrepareGroups();
        auto& reply = Service<TBridgeService>().Groups;
        reply.ExpectedRequest->set_storage_pool_name("ambiguous");
        reply.Status = Ydb::StatusIds::BAD_REQUEST;
        reply.Issue = "storage pool name is ambiguous; use its ID";
        ExpectFail();
        TString error;
        UNIT_ASSERT(RunList({"groups", "--pool", "ambiguous", "--format", "json"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "ambiguous");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 0);
    }
    Y_UNIT_TEST_F(TargetDoesNotChangeLoginOrDiscoveryDatabase, TBridgeCliFixture) {
        ExpectLogin();
        const auto password = EnvFile("password");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "--user", "bridge-admin", "--password-file", password,
                "admin", "cluster", "bridge", "set-database-mode", "bridged", "--database-path", "/Root/app"});
        UNIT_ASSERT(Service<TBridgeLogin>().Calls.load() >= 1);
        UNIT_ASSERT(Service<TBridgeDiscovery>().Calls.load() >= 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 1);
    }

    Y_UNIT_TEST_F(TargetWorksWithoutConnectionDatabase, TBridgeCliFixture) {
        ExpectLogin();
        Service<TBridgeLogin>().Database.clear();
        Service<TBridgeDiscovery>().Database.clear();
        Service<TBridgeService>().Database.clear();
        const auto password = EnvFile("password");
        RunCli({"-e", GetEndpoint(), "-y", "--user", "bridge-admin", "--password-file", password,
                "admin", "cluster", "bridge", "set-database-mode", "bridged", "--database-path", "/Root/app"});
        UNIT_ASSERT(Service<TBridgeLogin>().Calls.load() >= 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 1);
    }

    Y_UNIT_TEST_F(ConnectionDatabaseDoesNotSupplyTarget, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge", "set-database-mode", "bridged"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 0);
    }

    Y_UNIT_TEST_F(DatabaseModeIsRequired, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-y", "admin", "cluster", "bridge", "set-database-mode", "--database-path", "/Root/app"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 0);
    }

    Y_UNIT_TEST_F(RetainPileRequiresOrdinaryMode, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-y", "admin", "cluster", "bridge", "set-database-mode", "bridged",
                "--database-path", "/Root/app", "--retain-pile", "secondary"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 0);
    }

    Y_UNIT_TEST_F(OrdinaryModePassesRetainedPile, TBridgeCliFixture) {
        Service<TBridgeDiscovery>().Database.clear();
        Service<TBridgeService>().Database.clear();
        Service<TBridgeService>().Bridged = false;
        Service<TBridgeService>().RetainPile = "secondary";
        RunCli({"-e", GetEndpoint(), "-y", "admin", "cluster", "bridge", "set-database-mode", "ordinary",
                "--database-path", "/Root/app", "--retain-pile", "secondary"},
               {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 1);
    }

    Y_UNIT_TEST_F(UnknownDatabaseModeIsRejected, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-y", "admin", "cluster", "bridge", "set-database-mode", "unknown",
                "--database-path", "/Root/app"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 0);
    }

    Y_UNIT_TEST_F(JoinPileSupportsLegacyRejoinAlias, TBridgeCliFixture) {
        for (const auto& command : {"join-pile", "rejoin"}) {
            RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge", command, "--pile", "secondary"},
                   {{"YDB_TOKEN", "bridge-admin-token"}});
        }
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().JoinCalls.load(), 2);
    }

    Y_UNIT_TEST_F(ProvisionPileUsesAddPileRpc, TBridgeCliFixture) {
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge",
                                    "provision-pile", "--pile", "secondary"},
                                   {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ProvisionCalls.load(), 1);
        UNIT_ASSERT_STRING_CONTAINS(output, "Pile provisioned successfully");
    }

    Y_UNIT_TEST_F(DestroyPileRequiresConfirmation, TBridgeCliFixture) {
        ExpectFail();
        RunCliWithInput({"-e", GetEndpoint(), "-d", GetDatabase(), "admin", "cluster", "bridge", "destroy-pile", "--pile", "secondary"},
                        "n\n",
                        {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().DestroyCalls.load(), 0);
        const auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge",
                                    "destroy-pile", "--pile", "secondary"},
                                   {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().DestroyCalls.load(), 1);
        UNIT_ASSERT_STRING_CONTAINS(output, "Pile destroyed successfully");
    }

    Y_UNIT_TEST_F(RollbackRequiresConfirmation, TBridgeCliFixture) {
        const auto help = RunCli({"admin", "cluster", "bridge", "rollback", "--help"});
        UNIT_ASSERT_STRING_CONTAINS(help, "\"ydb admin cluster bridge list piles\"");
        UNIT_ASSERT_STRING_CONTAINS(help, "The report covers static and connected dynamic nodes.");

        ExpectFail();
        RunCliWithInput({"-e", GetEndpoint(), "-d", GetDatabase(), "admin", "cluster", "bridge", "rollback"},
                        "n\n",
                        {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().RollbackCalls.load(), 0);
        auto output = RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge", "rollback"},
                             {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().RollbackCalls.load(), 1);
        UNIT_ASSERT_STRING_CONTAINS(output, "Rollback started");
        UNIT_ASSERT_STRING_CONTAINS(output, "\"ydb admin cluster bridge list piles\"");
    }

    Y_UNIT_TEST_F(RollbackSkipsBscValidation, TBridgeCliFixture) {
        Service<TBridgeService>().SkipBscValidation = true;
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "admin", "cluster", "bridge", "rollback",
                "--skip-bsc-validation"},
               {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().RollbackCalls.load(), 1);
    }

    Y_UNIT_TEST_F(OrdinaryPileListShowsBridgeConfiguration, TBridgeCliFixture) {
        auto& status = PrepareUnbridgedStatus();
        status.add_bridge_config_node_ids(3);
        status.add_bridge_config_node_ids(5);
        status.add_unreported_static_node_ids(7);

        UNIT_ASSERT_VALUES_EQUAL(RunList(),
            "Bridge configuration still in use\n"
            "Nodes with bridge config: 2\n"
            "Unreported static nodes: 1\n"
            "To return to ordinary mode, remove bridge configuration and restart the nodes.\n");
        UNIT_ASSERT_VALUES_EQUAL(RunList({"piles", "--detailed"}),
            "Bridge configuration still in use\n"
            "Node IDs with bridge config: 3, 5\n"
            "Unreported static node IDs: 7\n"
            "To return to ordinary mode, remove bridge configuration and restart the nodes.\n");

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--format", "json"}), &json));
        UNIT_ASSERT(json["piles"].GetArray().empty());
        UNIT_ASSERT(!json.Has("generation"));
        const auto& report = json["unbridged_status"];
        const auto& configured = report["bridge_config_node_ids"].GetArray();
        UNIT_ASSERT_VALUES_EQUAL(configured.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(configured[0].GetUInteger(), 3);
        UNIT_ASSERT_VALUES_EQUAL(configured[1].GetUInteger(), 5);
        const auto& unreported = report["unreported_static_node_ids"].GetArray();
        UNIT_ASSERT_VALUES_EQUAL(unreported.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(unreported[0].GetUInteger(), 7);

        TString error;
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--format", "csv"}, &error), "pile_name,state,generation\n");
        UNIT_ASSERT_VALUES_EQUAL(error, "Node configuration status is available with --format json.\n");
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--detailed", "--format", "csv"}, &error),
            "pile_name,pile_name_present,state,databases_synced,databases_total,databases_syncing,databases_pending,"
            "databases_not_required,databases_unknown,cluster_unattributed_storage_pools,generation\n");
        UNIT_ASSERT_VALUES_EQUAL(error, "Node configuration status is available with --format json.\n");
        const auto& service = Service<TBridgeService>();
        UNIT_ASSERT_VALUES_EQUAL(service.ClusterCalls.load(), 5);
        UNIT_ASSERT_VALUES_EQUAL(service.UnbridgedStatusRequests.load(), 5);
        UNIT_ASSERT_VALUES_EQUAL(service.Summary.Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(service.LegacyRollbackStatus.Calls.load(), 0);
    }

    Y_UNIT_TEST_F(OrdinaryPileListShowsIncompleteReport, TBridgeCliFixture) {
        auto& status = PrepareUnbridgedStatus();
        status.add_unreported_static_node_ids(7);
        UNIT_ASSERT_VALUES_EQUAL(RunList(),
            "Bridge configuration status incomplete\n"
            "Unreported static nodes: 1\n");

        status.clear_unreported_static_node_ids();
        UNIT_ASSERT_VALUES_EQUAL(RunList(), "Cluster mode: ordinary\n");
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--format", "json"}), &json));
        const auto& report = json["unbridged_status"];
        UNIT_ASSERT(report["bridge_config_node_ids"].IsArray());
        UNIT_ASSERT(report["bridge_config_node_ids"].GetArray().empty());
        UNIT_ASSERT(report["unreported_static_node_ids"].IsArray());
        UNIT_ASSERT(report["unreported_static_node_ids"].GetArray().empty());
    }

    Y_UNIT_TEST_F(UnbridgedPileListFallsBackToLegacyRollbackStatus, TBridgeCliFixture) {
        auto& status = PrepareLegacyRollbackStatus();
        status.add_bridge_config_node_ids(3);
        status.add_unreported_static_node_ids(7);
        const auto& service = Service<TBridgeService>();

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"piles", "--detailed", "--format", "json"}), &json));
        UNIT_ASSERT(json["piles"].GetArray().empty());
        UNIT_ASSERT(!json.Has("generation"));
        const auto& report = json["unbridged_status"];
        UNIT_ASSERT_VALUES_EQUAL(report["bridge_config_node_ids"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(report["bridge_config_node_ids"][0].GetUInteger(), 3);
        UNIT_ASSERT_VALUES_EQUAL(report["unreported_static_node_ids"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(report["unreported_static_node_ids"][0].GetUInteger(), 7);
        UNIT_ASSERT_VALUES_EQUAL(service.ClusterCalls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(service.LegacyRollbackStatus.Calls.load(), 1);

        status.Clear();
        status.set_complete(true);
        UNIT_ASSERT_VALUES_EQUAL(RunList({"piles"}), "Cluster mode: ordinary\n");
        UNIT_ASSERT_VALUES_EQUAL(service.ClusterCalls.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(service.LegacyRollbackStatus.Calls.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(service.UnbridgedStatusRequests.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(service.Summary.Calls.load(), 0);
    }

    Y_UNIT_TEST_F(PileListPreservesNotFoundWithoutLegacyRollbackStatus, TBridgeCliFixture) {
        PrepareLegacyRollbackStatus();
        auto& service = Service<TBridgeService>();
        service.LegacyRollbackStatus.RpcStatus = grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "legacy RPC unavailable");

        ExpectFail();
        TString error;
        UNIT_ASSERT(RunList({"piles", "--format", "json"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "Bridge cluster state is not configured");
        UNIT_ASSERT(!error.Contains("legacy RPC unavailable"));
        UNIT_ASSERT_VALUES_EQUAL(service.LegacyRollbackStatus.Calls.load(), 1);

        ExpectFail();
        UNIT_ASSERT(RunList({"piles", "--pile", "secondary"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "Bridge cluster state is not configured");
        UNIT_ASSERT_VALUES_EQUAL(service.ClusterCalls.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(service.UnbridgedStatusRequests.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(service.LegacyRollbackStatus.Calls.load(), 1);
    }

    Y_UNIT_TEST_F(PileListRejectsUnconfirmedLegacyRollbackStatus, TBridgeCliFixture) {
        auto& status = PrepareLegacyRollbackStatus();
        const auto& service = Service<TBridgeService>();
        TString error;

        ExpectFail();
        UNIT_ASSERT(RunList({"piles"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "Unbridged status report is incomplete.");

        for (const bool rollbackInProgress : {false, true}) {
            status.set_bridged(!rollbackInProgress);
            status.set_rollback_in_progress(rollbackInProgress);
            ExpectFail();
            UNIT_ASSERT(RunList({"piles"}, &error).empty());
            UNIT_ASSERT_STRING_CONTAINS(error, "Bridge cluster state changed. Retry the request.");
        }
        UNIT_ASSERT_VALUES_EQUAL(service.ClusterCalls.load(), 3);
        UNIT_ASSERT_VALUES_EQUAL(service.LegacyRollbackStatus.Calls.load(), 3);
    }
}
