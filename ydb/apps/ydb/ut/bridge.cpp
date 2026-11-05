#include "mock_env.h"

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>

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
    std::atomic<size_t> ClusterCalls{0};
    TDiagnosticReply<Ydb::Bridge::GetSynchronizationSummaryRequest, Ydb::Bridge::GetSynchronizationSummaryResult> Summary;
    TDiagnosticReply<Ydb::Bridge::ListDatabaseStorageRequest, Ydb::Bridge::ListDatabaseStorageResult> Databases;
    TDiagnosticReply<Ydb::Bridge::ListGroupSynchronizationRequest, Ydb::Bridge::ListGroupSynchronizationResult> Groups;
    std::atomic<size_t> ModeCalls{0};
    std::atomic<size_t> JoinCalls{0};
    std::atomic<size_t> ProvisionCalls{0};
    std::atomic<size_t> DestroyCalls{0};

    grpc::Status GetClusterState(grpc::ServerContext* context, const Ydb::Bridge::GetClusterStateRequest*,
            Ydb::Bridge::GetClusterStateResponse* response) override {
        ++ClusterCalls;
        const auto status = ReplySuccess(context, response);
        response->mutable_operation()->mutable_result()->PackFrom(ClusterResult);
        return status;
    }

    grpc::Status GetSynchronizationSummary(grpc::ServerContext* context, const Ydb::Bridge::GetSynchronizationSummaryRequest* request,
            Ydb::Bridge::GetSynchronizationSummaryResponse* response) override {
        return ReplyDiagnostic(context, *request, *response, Summary);
    }

    grpc::Status ListDatabaseStorage(grpc::ServerContext* context, const Ydb::Bridge::ListDatabaseStorageRequest* request,
            Ydb::Bridge::ListDatabaseStorageResponse* response) override {
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

    Ydb::Bridge::GetSynchronizationSummaryResult& PrepareSummary() {
        auto& reply = Service<TBridgeService>().Summary;
        reply.ExpectedRequest.emplace();
        SetObservation(reply.Result);
        return reply.Result;
    }

    Ydb::Bridge::ListDatabaseStorageResult& PrepareDatabases() {
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
        UNIT_ASSERT_VALUES_EQUAL(RunList(), "Pile primary: PRIMARY\nPile secondary: SYNCHRONIZED\nGeneration: 42\n");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Summary.Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Databases.Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Groups.Calls.load(), 0);
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
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Summary.Calls.load(), 0);
    }

    Y_UNIT_TEST_F(InvalidListOptionsAreRejectedBeforeRpc, TBridgeCliFixture) {
        const TVector<TList<TString>> invalid = {
            {"--databases", "--groups"}, {"--groups"}, {"--path", "/Root/app"}, {"--pool", "pool"},
            {"--databases", "--pool", "pool"}, {"--groups", "--path", "/Root/app", "--pool", "pool"},
            {"--not-synced"}, {"--detailed", "--not-synced"},
            {"--databases", "--page-size", "10"}, {"--groups", "--path", "/Root/app", "--page-token", "previous"},
            {"--groups", "--path", "relative/path"}, {"--databases", "--path", ""}, {"--groups", "--pool", ""},
            {"--pool-id", "1:2"}, {"--databases", "--pool-id", "1:2"},
            {"--groups", "--pool", "pool", "--pool-id", "1:2"},
            {"--groups", "--path", "/Root/app", "--pool-id", "1:2"},
            {"--groups", "--pool-id", "1:0"}, {"--groups", "--pool-id", ":2"}, {"--groups", "--pool-id", "1:"},
            {"--groups", "--pool-id", "1:2:3"}, {"--groups", "--pool-id", "-1:2"},
            {"--groups", "--pool-id", "18446744073709551616:2"}, {"--groups", "--pool-id", "1:+2"},
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
        auto* pile = PrepareSummary().add_piles();
        pile->set_pile_name("secondary");
        pile->mutable_user_groups()->set_required(8);
        pile->mutable_user_groups()->set_synced(5);
        pile->mutable_user_groups()->set_syncing(2);
        pile->mutable_user_groups()->set_pending(1);
        pile->mutable_system_groups()->set_required(1);
        pile->mutable_system_groups()->set_synced(1);
        const auto output = RunList({"--detailed", "--pile", "secondary"});
        UNIT_ASSERT_STRING_CONTAINS(output, "5/8");
        UNIT_ASSERT_STRING_CONTAINS(output, "1/1");
        for (const auto* header : {"Synced groups", "Syncing groups", "Pending groups", "Unknown groups", "Synced system groups"}) {
            UNIT_ASSERT_STRING_CONTAINS(output, header);
        }
        UNIT_ASSERT(!output.Contains("primary"));
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--detailed", "--pile", "secondary", "--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["piles"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["synchronization_summary"]["cluster_state_generation"].GetUInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(json["synchronization_summary"]["piles"][0]["user_groups"]["required"].GetUInteger(), 8);
        UNIT_ASSERT(!json.Has("storage_state"));
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Summary.Calls.load(), 2);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 2);
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
            "pile_name,state,groups_synced,groups_required,groups_syncing,groups_pending,groups_unknown,system_groups_synced,system_groups_required,generation\n"
            "primary,PRIMARY,,,,,,,,42\nsecondary,SYNCHRONIZED,,,,,,,,42\n");
        UNIT_ASSERT_STRING_CONTAINS(error, "controller unavailable");
    }

    Y_UNIT_TEST_F(DatabaseViewForwardsFiltersWithoutClusterState, TBridgeCliFixture) {
        auto& result = PrepareDatabases();
        auto& expected = *Service<TBridgeService>().Databases.ExpectedRequest;
        expected.set_database_path("/Root/serverless");
        expected.set_pile_name("secondary");
        expected.set_only_incomplete(true);
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
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--databases", "--path", "/Root/serverless", "--pile", "secondary",
            "--not-synced", "--format", "json"}), &json));
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
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--databases", "--detailed", "--format", "csv"}),
            "database_path,storage_pools,storage_mode,pile_name,pile_name_present,groups_synced,groups_required,groups_syncing,groups_pending,groups_unknown,last_sync_error,last_sync_error_occurred_at\n"
            "/Root/app,\"[{\"\"id\"\":{\"\"box_id\"\":1,\"\"pool_id\"\":2},\"\"name\"\":\"\"pool,ssd\"\"}]\",bridged,secondary,true,3,8,2,1,2,retrying,1970-01-01T00:00:00.500000Z\n");
        const auto pretty = RunList({"--databases", "--detailed"});
        UNIT_ASSERT_STRING_CONTAINS(pretty, "SYNCING");
        UNIT_ASSERT_STRING_CONTAINS(pretty, "3/8");
        for (const auto* header : {"Synced groups", "Syncing groups", "Pending groups", "Unknown groups"}) {
            UNIT_ASSERT_STRING_CONTAINS(pretty, header);
        }
        UNIT_ASSERT_STRING_CONTAINS(pretty, "pool,ssd");
        UNIT_ASSERT_STRING_CONTAINS(pretty, "1970-01-01T00:00:00.500000Z");
        UNIT_ASSERT(!RunList({"--databases", "--format", "csv"}).Contains("last_sync_error"));
    }

    Y_UNIT_TEST_F(OrdinaryStoragePreservesAbsentOwnerAndPile, TBridgeCliFixture) {
        auto* row = PrepareDatabases().add_databases();
        SetPool(row->add_storage_pools(), "ordinary");
        row->set_storage_mode(Ydb::Bridge::ORDINARY);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--databases", "--format", "json"}), &json));
        const auto& database = json["databases"][0];
        UNIT_ASSERT(!database.Has("database_path"));
        UNIT_ASSERT(!database.Has("pile_name"));
        UNIT_ASSERT_VALUES_EQUAL(database["groups"]["required"].GetUInteger(), 0);
        UNIT_ASSERT_STRING_CONTAINS(RunList({"--databases"}), "N/A");
        UNIT_ASSERT_STRING_CONTAINS(RunList({"--databases", "--format", "csv"}), ",ordinary,,false,0,0\n");
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
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--groups", "--path", "/Root/app", "--format", "json"}), &json));
        UNIT_ASSERT(json["groups"][0].Has("pile_name"));
        UNIT_ASSERT(json["groups"][0]["pile_name"].GetString().empty());
        UNIT_ASSERT(!json["groups"][0].Has("physical_group_id"));
        UNIT_ASSERT(!json["groups"][1].Has("pile_name"));
        const auto csv = RunList({"--groups", "--path", "/Root/app", "--format", "csv"});
        UNIT_ASSERT(csv.StartsWith("group_id,storage_pool,pile_name,pile_name_present,sync_status\n"));
        UNIT_ASSERT_STRING_CONTAINS(csv, ",,true,UNKNOWN\n");
        UNIT_ASSERT_STRING_CONTAINS(csv, ",,false,NOT_REQUIRED\n");
        UNIT_ASSERT(!csv.Contains("physical_group_id"));
        result.mutable_groups()->RemoveLast();
        Service<TBridgeService>().Groups.ExpectedRequest->set_pile_name("");
        UNIT_ASSERT_STRING_CONTAINS(RunList({"--groups", "--path", "/Root/app", "--pile", ""}), "\"\"");
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
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--groups", "--pool", "pool,ssd", "--pile", "secondary", "--detailed", "--format", "csv"}, &error),
            "group_id,storage_pool,pile_name,pile_name_present,sync_status,physical_group_id,sync_phase,last_sync_error,last_sync_error_occurred_at\n"
            "123,\"{\"\"id\"\":{\"\"box_id\"\":1,\"\"pool_id\"\":2},\"\"name\"\":\"\"pool,ssd\"\"}\",secondary,true,SYNCING,456,WRITE_KEEP_BARRIER_DONOTKEEP_DATA,\"retry \"\"dc,2\"\"\r\nlater\",1970-01-01T00:00:00.000077Z\n");
        UNIT_ASSERT(!error.Contains("Next page token:"));
        const auto pretty = RunList({"--groups", "--pool", "pool,ssd", "--pile", "secondary", "--detailed"});
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
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--groups", "--pool-id", "0:42", "--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["groups"][0]["storage_pool"]["id"]["box_id"].GetUInteger(), 0);
        UNIT_ASSERT_VALUES_EQUAL(json["groups"][0]["storage_pool"]["id"]["pool_id"].GetUInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().Groups.Calls.load(), 1);
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
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--groups", "--path", "/Root/app", "--pile", "secondary",
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
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--groups", "--path", "/Root/app", "--format", "json"}), &json));
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
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--databases", "--path", "/Root/serverless", "--format", "json"}), &json));
        UNIT_ASSERT(json["databases"].GetArray().empty());
        UNIT_ASSERT_VALUES_EQUAL(json["storage_owner_database_path"].GetString(), "/Root/shared");
        UNIT_ASSERT(!json.Has("next_page_token"));
        UNIT_ASSERT(!json.Has("incomplete_reason"));
        UNIT_ASSERT_VALUES_EQUAL(reply.Calls.load(), 1);
    }

    Y_UNIT_TEST_F(OversizedListsFailWithoutPrintingPartialData, TBridgeCliFixture) {
        PrepareDatabases();
        auto& service = Service<TBridgeService>();
        service.Databases.Status = Ydb::StatusIds::PRECONDITION_FAILED;
        service.Databases.Issue = "Result limit exceeded; narrow database/pool/pile/status filter";
        ExpectFail();
        TString error;
        UNIT_ASSERT(RunList({"--databases", "--format", "json"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "narrow database/pool/pile/status filter");
        PrepareGroups();
        service.Groups.Status = Ydb::StatusIds::PRECONDITION_FAILED;
        service.Groups.Issue = service.Databases.Issue;
        ExpectFail();
        UNIT_ASSERT(RunList({"--groups", "--path", "/Root/app", "--format", "csv"}, &error).empty());
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
        UNIT_ASSERT(RunList({"--groups", "--pool", "ambiguous", "--format", "json"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "ambiguous");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ClusterCalls.load(), 0);
    }
    Y_UNIT_TEST_F(TargetDoesNotChangeLoginOrDiscoveryDatabase, TBridgeCliFixture) {
        ExpectLogin();
        const auto password = EnvFile("password");
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "-y", "--user", "bridge-admin", "--password-file", password,
                "admin", "cluster", "bridge", "set-database-mode", "bridged", "--target-database", "/Root/app"});
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
                "admin", "cluster", "bridge", "set-database-mode", "bridged", "--target-database", "/Root/app"});
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
        RunCli({"-e", GetEndpoint(), "-y", "admin", "cluster", "bridge", "set-database-mode", "--target-database", "/Root/app"});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 0);
    }

    Y_UNIT_TEST_F(RetainPileRequiresOrdinaryMode, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-y", "admin", "cluster", "bridge", "set-database-mode", "bridged",
                "--target-database", "/Root/app", "--retain-pile", "secondary"});
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
                "--target-database", "/Root/app", "--retain-pile", "secondary"},
               {{"YDB_TOKEN", "bridge-admin-token"}});
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ModeCalls.load(), 1);
    }

    Y_UNIT_TEST_F(UnknownDatabaseModeIsRejected, TBridgeCliFixture) {
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-y", "admin", "cluster", "bridge", "set-database-mode", "unknown",
                "--target-database", "/Root/app"});
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
}
