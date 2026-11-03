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

class TBridgeService : public TMockGrpcServiceBase<Ydb::Bridge::V1::BridgeService::Service> {
public:
    struct TListPage {
        Ydb::Bridge::StorageQuery Query;
        Ydb::Bridge::GetClusterStateResult Result;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        TString Issue = {};
    };

    TBridgeService() {
        ListResult.set_generation(42);
        auto* primary = ListResult.add_pile_states();
        primary->set_pile_name("primary");
        primary->set_state(Ydb::Bridge::PileState::PRIMARY);
        auto* secondary = ListResult.add_pile_states();
        secondary->set_pile_name("secondary");
        secondary->set_state(Ydb::Bridge::PileState::SYNCHRONIZED);
    }

    TString Database = TEST_DATABASE;
    bool Bridged = true;
    TString RetainPile;
    std::optional<Ydb::Bridge::StorageQuery> ExpectedStorageQuery;
    Ydb::Bridge::GetClusterStateResult ListResult;
    TVector<TListPage> ListPages;
    Ydb::StatusIds::StatusCode ListStatus = Ydb::StatusIds::SUCCESS;
    TString ListIssue;
    std::atomic<size_t> ListCalls{0};
    std::atomic<size_t> ModeCalls{0};
    std::atomic<size_t> JoinCalls{0};
    std::atomic<size_t> ProvisionCalls{0};
    std::atomic<size_t> DestroyCalls{0};

    grpc::Status GetClusterState(grpc::ServerContext* context,
                                 const Ydb::Bridge::GetClusterStateRequest* request,
                                 Ydb::Bridge::GetClusterStateResponse* response) override {
        const size_t index = ListCalls.fetch_add(1);
        const auto* query = ExpectedStorageQuery ? &*ExpectedStorageQuery : nullptr;
        const auto* result = &ListResult;
        auto operationStatus = ListStatus;
        auto issue = ListIssue;
        if (!ListPages.empty()) {
            CHECK_EXP(index < ListPages.size(), "Unexpected extra storage page request");
            if (index < ListPages.size()) {
                query = &ListPages[index].Query;
                result = &ListPages[index].Result;
                operationStatus = ListPages[index].Status;
                issue = ListPages[index].Issue;
            }
        }
        CHECK_EXP(request->has_storage_query() == (query != nullptr), "Unexpected storage diagnostics request");
        if (query && request->has_storage_query()) {
            CHECK_EXP(request->storage_query().SerializeAsString() == query->SerializeAsString(),
                      "Incorrect storage query: " << request->storage_query().ShortDebugString());
        }
        const auto status = ReplySuccess(context, response);
        auto* operation = response->mutable_operation();
        operation->set_status(operationStatus);
        if (!issue.empty()) {
            operation->add_issues()->set_message(issue);
        }
        if (operationStatus == Ydb::StatusIds::SUCCESS) {
            operation->mutable_result()->PackFrom(*result);
        }
        return status;
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
    template<class TResponse>
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

    Ydb::Bridge::StorageState& PrepareStorage(Ydb::Bridge::StorageQuery::View view) {
        auto& service = Service<TBridgeService>();
        service.ExpectedStorageQuery.emplace();
        service.ExpectedStorageQuery->set_view(view);
        service.ExpectedStorageQuery->set_page_size(1000);
        auto* storage = service.ListResult.mutable_storage_state();
        storage->set_generation(42);
        storage->set_collected_at_us(1000000);
        return *storage;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(BridgeCli) {
    Y_UNIT_TEST_F(DefaultListOutputIsUnchanged, TBridgeCliFixture) {
        UNIT_ASSERT_VALUES_EQUAL(RunList(), "Pile primary: PRIMARY\nPile secondary: SYNCHRONIZED\nGeneration: 42\n");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ListCalls.load(), 1);
    }

    Y_UNIT_TEST_F(DefaultListMachineFormatsAreUnchanged, TBridgeCliFixture) {
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["generation"].GetUInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(json["piles"].GetArray().size(), 2);
        UNIT_ASSERT(!json.Has("storage_state"));
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--format", "csv"}),
                                 "pile_name,state,generation\nprimary,PRIMARY,42\nsecondary,SYNCHRONIZED,42\n");
    }

    Y_UNIT_TEST_F(BaselinePileFilterDoesNotRequestStorage, TBridgeCliFixture) {
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--pile", "secondary"}), "Pile secondary: SYNCHRONIZED\nGeneration: 42\n");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ListCalls.load(), 1);
    }

    Y_UNIT_TEST_F(InvalidListOptionsAreRejectedBeforeRpc, TBridgeCliFixture) {
        const TVector<TList<TString>> invalidOptions = {
            {"--databases", "--groups"},
            {"--groups"},
            {"--db-path", "/Root/app"},
            {"--pool", "/Root/app:ssd"},
            {"--databases", "--pool", "/Root/app:ssd"},
            {"--groups", "--db-path", "/Root/app", "--pool", "/Root/app:ssd"},
            {"--not-synced"},
            {"--detailed", "--not-synced"},
            {"--page-size", "10"},
            {"--page-token", "next-page"},
            {"--databases", "--page-size", "0"},
            {"--databases", "--page-size", "10001"},
            {"--groups", "--db-path", "relative/path"},
            {"--groups", "--pool", ""},
            {"--pile", ""},
        };
        for (const auto& options : invalidOptions) {
            ExpectFail();
            RunList(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeLogin>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeDiscovery>().Calls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ListCalls.load(), 0);
    }

    Y_UNIT_TEST_F(DetailedPileViewRequestsStorageCounters, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::PILES);
        Service<TBridgeService>().ExpectedStorageQuery->set_pile_name("secondary");
        auto* pile = storage.add_piles();
        pile->set_pile_name("secondary");
        auto* groups = pile->mutable_user_groups();
        groups->set_total(8);
        groups->set_synced(5);
        groups->set_running(2);
        groups->set_pending(1);
        pile->mutable_system_groups()->set_total(1);
        pile->mutable_system_groups()->set_synced(1);

        const auto output = RunList({"--detailed", "--pile", "secondary"});
        UNIT_ASSERT_STRING_CONTAINS(output, "secondary");
        UNIT_ASSERT_STRING_CONTAINS(output, "5/8");
        UNIT_ASSERT_STRING_CONTAINS(output, "1/1");
        UNIT_ASSERT(!output.Contains("Next page token:"));
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ListCalls.load(), 1);
    }

    Y_UNIT_TEST_F(DatabaseViewForwardsFiltersAndPaginationWithoutChangingConnectionDatabase, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::DATABASES);
        auto& query = *Service<TBridgeService>().ExpectedStorageQuery;
        query.set_database_path("/Root/app");
        query.set_pile_name("secondary");
        query.set_not_synced(true);
        query.set_page_size(123);
        query.set_page_token("previous-page");
        storage.set_database_path("/Root/app");
        storage.set_storage_owner_database("/Root/shared");
        storage.set_next_page_token("next-page");
        auto* database = storage.add_databases();
        database->set_database_path("/Root/shared");
        database->add_storage_pools("/Root/shared,ssd");
        database->add_storage_pools("/Root/shared:hdd");
        database->set_mode(Ydb::Bridge::DatabaseStorageState::BRIDGED);
        database->set_pile_name("secondary");
        database->set_state(Ydb::Bridge::GroupStorageState::SYNCING);
        database->mutable_groups()->set_total(8);
        database->mutable_groups()->set_synced(3);
        database->mutable_groups()->set_running(5);
        database->set_last_error("retry \"dc,2\"\nnext attempt");
        database->set_last_error_timestamp_us(500000);

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--databases", "--detailed", "--db-path", "/Root/app",
                                                "--pile", "secondary", "--not-synced", "--page-size", "123",
                                                "--page-token", "previous-page", "--format", "json"}), &json));
        const auto& result = json["storage_state"];
        UNIT_ASSERT_VALUES_EQUAL(result["database_path"].GetString(), "/Root/app");
        UNIT_ASSERT_VALUES_EQUAL(result["storage_owner_database"].GetString(), "/Root/shared");
        UNIT_ASSERT_VALUES_EQUAL(result["next_page_token"].GetString(), "next-page");
        UNIT_ASSERT_VALUES_EQUAL(result["databases"].GetArray().size(), 1);
        const auto& row = result["databases"][0];
        UNIT_ASSERT_VALUES_EQUAL(row["storage_pools"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(row["storage_pools"][0].GetString(), "/Root/shared,ssd");
        UNIT_ASSERT_VALUES_EQUAL(row["storage_pools"][1].GetString(), "/Root/shared:hdd");
        UNIT_ASSERT_VALUES_EQUAL(row["mode"].GetString(), "bridged");
        UNIT_ASSERT_VALUES_EQUAL(row["state"].GetString(), "SYNCING");
        UNIT_ASSERT_VALUES_EQUAL(row["groups"]["synced"].GetUInteger(), 3);
        UNIT_ASSERT_VALUES_EQUAL(row["last_error"].GetString(), database->last_error());
        UNIT_ASSERT(Service<TBridgeDiscovery>().Calls.load() >= 1);
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ListCalls.load(), 1);
    }

    Y_UNIT_TEST_F(DatabaseViewUsesGenerousDefaultPageSizeAndPreservesNotRequiredState, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::DATABASES);
        auto* database = storage.add_databases();
        database->set_database_path("/Root/ordinary");
        database->add_storage_pools("/Root/ordinary:ssd");
        database->set_mode(Ydb::Bridge::DatabaseStorageState::ORDINARY);
        database->set_state(Ydb::Bridge::GroupStorageState::NOT_REQUIRED);

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--databases", "--format", "json"}), &json));
        const auto& row = json["storage_state"]["databases"][0];
        UNIT_ASSERT_VALUES_EQUAL(row["mode"].GetString(), "ordinary");
        UNIT_ASSERT_VALUES_EQUAL(row["state"].GetString(), "NOT_REQUIRED");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ListCalls.load(), 1);
    }

    Y_UNIT_TEST_F(DatabaseColumnsPreserveStructuredPoolNamesAndTypedValues, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::DATABASES);
        auto* database = storage.add_databases();
        database->set_database_path("/Root/app");
        database->add_storage_pools("pool,ssd");
        database->add_storage_pools("pool:hdd");
        database->set_mode(Ydb::Bridge::DatabaseStorageState::MIXED);
        database->set_pile_name("secondary");
        database->set_state(Ydb::Bridge::GroupStorageState::SYNCING);
        auto* counts = database->mutable_groups();
        counts->set_total(8);
        counts->set_synced(3);
        counts->set_running(2);
        counts->set_pending(1);
        counts->set_unknown(2);
        database->set_last_error("retrying");
        database->set_last_error_timestamp_us(500000);

        UNIT_ASSERT_VALUES_EQUAL(RunList({"--databases", "--detailed", "--format", "csv"}),
            "database_path,storage_pools,mode,pile_name,state,groups_synced,groups_total,running,pending,unknown,last_error,last_error_timestamp_us\n"
            "/Root/app,\"[\"\"pool,ssd\"\",\"\"pool:hdd\"\"]\",mixed,secondary,SYNCING,3,8,2,1,2,retrying,500000\n");
        const auto pretty = RunList({"--databases", "--detailed"});
        UNIT_ASSERT_STRING_CONTAINS(pretty, "Pools");
        UNIT_ASSERT_STRING_CONTAINS(pretty, "pool,ssd, pool:hdd");
        UNIT_ASSERT_STRING_CONTAINS(pretty, "3/8");
        UNIT_ASSERT_STRING_CONTAINS(pretty, "1970-01-01T00:00:00.500000Z");
        UNIT_ASSERT(!RunList({"--databases", "--format", "csv"}).Contains("last_error"));
    }

    Y_UNIT_TEST_F(DetailedGroupCsvEscapesNamesAndLastErrors, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::GROUPS);
        const TString pool = "pool,\"quoted\"\nline";
        Service<TBridgeService>().ExpectedStorageQuery->set_storage_pool(pool);
        Service<TBridgeService>().ExpectedStorageQuery->set_pile_name("secondary");
        auto* group = storage.add_groups();
        group->set_group_id(123);
        group->set_part_group_id(456);
        group->set_storage_pool_name(pool);
        group->set_pile_name("secondary");
        group->set_state(Ydb::Bridge::GroupStorageState::SYNCING);
        group->set_stage("WRITE_KEEP_BARRIER_DONOTKEEP_DATA");
        group->set_last_error("retry \"dc,2\"\r\nlater");
        group->set_last_error_timestamp_us(77);
        storage.set_next_page_token("next-page");

        TString error;
        const auto output = RunList({"--groups", "--pool", pool, "--pile", "secondary", "--detailed", "--format", "csv"}, &error);
        UNIT_ASSERT_VALUES_EQUAL(output,
            "group_id,part_group_id,storage_pool_name,pile_name,state,stage,last_error,last_error_timestamp_us\n"
            "123,456,\"pool,\"\"quoted\"\"\nline\",secondary,SYNCING,WRITE_KEEP_BARRIER_DONOTKEEP_DATA,\"retry \"\"dc,2\"\"\r\nlater\",77\n");
        UNIT_ASSERT(!output.Contains("Next page token:"));
        UNIT_ASSERT_STRING_CONTAINS(error, "Next page token: next-page\n");

        const auto pretty = RunList({"--groups", "--pool", pool, "--pile", "secondary", "--detailed"});
        UNIT_ASSERT_STRING_CONTAINS(pretty, "DATA");
        UNIT_ASSERT(!pretty.Contains("WRITE_KEEP"));
        UNIT_ASSERT_STRING_CONTAINS(pretty, "1970-01-01T00:00:00.000077Z");
    }

    Y_UNIT_TEST_F(GroupViewAcceptsDatabaseScopeAndPreservesUnknownState, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::GROUPS);
        Service<TBridgeService>().ExpectedStorageQuery->set_database_path("/Root/app");
        auto* group = storage.add_groups();
        group->set_group_id(123);
        group->set_storage_pool_name("/Root/app:ssd");
        group->set_pile_name("secondary");
        group->set_state(Ydb::Bridge::GroupStorageState::UNKNOWN);
        group->set_stage("WRITE_KEEP");
        group->set_last_error("not part of the basic CSV view");
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--groups", "--db-path", "/Root/app", "--format", "csv"}),
            "group_id,part_group_id,storage_pool_name,pile_name,state\n123,0,/Root/app:ssd,secondary,UNKNOWN\n");
    }

    Y_UNIT_TEST_F(PrettyPageTokenAppearsOnlyWhenThereIsAnotherPage, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::DATABASES);
        auto* database = storage.add_databases();
        database->set_database_path("/Root/app");
        database->add_storage_pools("/Root/app:ssd");
        database->set_mode(Ydb::Bridge::DatabaseStorageState::BRIDGED);
        database->set_pile_name("secondary");
        database->set_state(Ydb::Bridge::GroupStorageState::SYNCED);
        database->mutable_groups()->set_total(8);
        database->mutable_groups()->set_synced(8);
        UNIT_ASSERT(!RunList({"--databases"}).Contains("Next page token:"));

        storage.set_next_page_token("next-page");
        const auto output = RunList({"--databases"});
        UNIT_ASSERT(output.EndsWith("Next page token: next-page\n"));
        UNIT_ASSERT(!output.Contains("Showing"));
    }

    Y_UNIT_TEST_F(GroupViewSkipsEmptyScanPagesWithoutChangingFilters, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::GROUPS);
        auto& service = Service<TBridgeService>();
        service.ExpectedStorageQuery->set_database_path("/Root/app");
        service.ExpectedStorageQuery->set_pile_name("secondary");
        service.ExpectedStorageQuery->set_not_synced(true);
        storage.set_next_page_token("next-page");
        service.ListPages.push_back({*service.ExpectedStorageQuery, service.ListResult});

        auto second = service.ListPages.front();
        second.Query.set_page_token("next-page");
        auto* next = second.Result.mutable_storage_state();
        next->clear_next_page_token();
        auto* group = next->add_groups();
        group->set_group_id(123);
        group->set_part_group_id(456);
        group->set_storage_pool_name("/Root/app:ssd");
        group->set_pile_name("secondary");
        group->set_state(Ydb::Bridge::GroupStorageState::PENDING);
        service.ListPages.push_back(std::move(second));

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--groups", "--db-path", "/Root/app", "--pile", "secondary",
                                                "--not-synced", "--format", "json"}), &json));
        const auto& result = json["storage_state"];
        UNIT_ASSERT_VALUES_EQUAL(result["groups"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result["groups"][0]["group_id"].GetUInteger(), 123);
        UNIT_ASSERT(result["next_page_token"].GetString().empty());
        UNIT_ASSERT_VALUES_EQUAL(service.ListCalls.load(), 2);
    }

    Y_UNIT_TEST_F(DatabaseViewRejectsRepeatedEmptyPageToken, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::DATABASES);
        auto& service = Service<TBridgeService>();
        service.ExpectedStorageQuery->set_pile_name("secondary");
        service.ExpectedStorageQuery->set_not_synced(true);
        storage.set_next_page_token("repeated-page");
        service.ListPages.push_back({*service.ExpectedStorageQuery, service.ListResult});
        auto second = service.ListPages.front();
        second.Query.set_page_token("repeated-page");
        service.ListPages.push_back(std::move(second));

        ExpectFail();
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--databases", "--pile", "secondary", "--not-synced",
                                                "--format", "json"}), &json));
        const auto& result = json["storage_state"];
        UNIT_ASSERT_VALUES_EQUAL(json["incomplete_reason"].GetString(), "Server returned a repeated storage page token");
        UNIT_ASSERT(result["unavailable_reason"].GetString().empty());
        UNIT_ASSERT_VALUES_EQUAL(result["next_page_token"].GetString(), "repeated-page");
        UNIT_ASSERT_VALUES_EQUAL(service.ListCalls.load(), 2);
    }

    Y_UNIT_TEST_F(CsvReturnsContinuationTokenWhenEmptyPageSearchBudgetIsReached, TBridgeCliFixture) {
        PrepareStorage(Ydb::Bridge::StorageQuery::GROUPS);
        auto& service = Service<TBridgeService>();
        service.ExpectedStorageQuery->set_database_path("/Root/app");
        service.ExpectedStorageQuery->set_pile_name("secondary");
        service.ExpectedStorageQuery->set_not_synced(true);
        for (size_t i = 0; i < 32; ++i) {
            auto query = *service.ExpectedStorageQuery;
            query.set_page_token(i ? "page-" + ToString(i) : TString{});
            auto result = service.ListResult;
            result.mutable_storage_state()->set_next_page_token("page-" + ToString(i + 1));
            service.ListPages.push_back({std::move(query), std::move(result)});
        }

        ExpectFail();
        TString error;
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--groups", "--db-path", "/Root/app", "--pile", "secondary",
            "--not-synced", "--format", "csv"}, &error),
            "group_id,part_group_id,storage_pool_name,pile_name,state\n");
        UNIT_ASSERT_STRING_CONTAINS(error, "Next page token: page-32\n");
        UNIT_ASSERT_STRING_CONTAINS(error, "Storage listing search is incomplete");
        UNIT_ASSERT_VALUES_EQUAL(service.ListCalls.load(), 32);
    }

    Y_UNIT_TEST_F(FailedContinuationKeepsTheCursorAndDoesNotChangeServerAvailability, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::DATABASES);
        auto& service = Service<TBridgeService>();
        storage.set_next_page_token("next-page");
        service.ListPages.push_back({*service.ExpectedStorageQuery, service.ListResult});
        auto next = service.ListPages.front();
        next.Query.set_page_token("next-page");
        next.Status = Ydb::StatusIds::UNAVAILABLE;
        next.Issue = "page endpoint unavailable";
        service.ListPages.push_back(std::move(next));

        ExpectFail();
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--databases", "--format", "json"}), &json));
        UNIT_ASSERT_STRING_CONTAINS(json["incomplete_reason"].GetString(), "page endpoint unavailable");
        UNIT_ASSERT_VALUES_EQUAL(json["storage_state"]["next_page_token"].GetString(), "next-page");
        UNIT_ASSERT(json["storage_state"]["unavailable_reason"].GetString().empty());
        UNIT_ASSERT_VALUES_EQUAL(service.ListCalls.load(), 2);
    }

    Y_UNIT_TEST_F(UnavailableContinuationKeepsTheCursorAndPreviousObservationInJson, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::DATABASES);
        auto& service = Service<TBridgeService>();
        storage.set_next_page_token("resume-page");
        service.ListPages.push_back({*service.ExpectedStorageQuery, service.ListResult});
        auto next = service.ListPages.front();
        next.Query.set_page_token("resume-page");
        next.Result.set_generation(43);
        auto* unavailable = next.Result.mutable_storage_state();
        unavailable->set_generation(43);
        unavailable->clear_next_page_token();
        unavailable->set_unavailable_reason("Storage generations differ; retry the query");
        service.ListPages.push_back(std::move(next));

        ExpectFail();
        TString error;
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--databases", "--format", "json"}, &error), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["generation"].GetUInteger(), 42);
        const auto& previous = json["storage_state"];
        UNIT_ASSERT_VALUES_EQUAL(previous["generation"].GetUInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(previous["next_page_token"].GetString(), "resume-page");
        UNIT_ASSERT(previous["unavailable_reason"].GetString().empty());
        UNIT_ASSERT_VALUES_EQUAL(json["incomplete_reason"].GetString(),
            "Storage page diagnostics unavailable: Storage generations differ; retry the query");
        UNIT_ASSERT_STRING_CONTAINS(error, "Storage generations differ; retry the query");
        UNIT_ASSERT_VALUES_EQUAL(service.ListCalls.load(), 2);
    }

    Y_UNIT_TEST_F(UnavailableContinuationReturnsThePreviousCursorAndReasonInCsv, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::GROUPS);
        auto& service = Service<TBridgeService>();
        service.ExpectedStorageQuery->set_database_path("/Root/app");
        storage.set_next_page_token("resume-page");
        service.ListPages.push_back({*service.ExpectedStorageQuery, service.ListResult});
        auto next = service.ListPages.front();
        next.Query.set_page_token("resume-page");
        auto* unavailable = next.Result.mutable_storage_state();
        unavailable->clear_next_page_token();
        unavailable->set_unavailable_reason("Storage controller is unavailable");
        service.ListPages.push_back(std::move(next));

        ExpectFail();
        TString error;
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--groups", "--db-path", "/Root/app", "--format", "csv"}, &error),
            "group_id,part_group_id,storage_pool_name,pile_name,state\n");
        UNIT_ASSERT_STRING_CONTAINS(error, "Next page token: resume-page\n");
        UNIT_ASSERT_STRING_CONTAINS(error,
            "Bridge storage listing incomplete: Storage page diagnostics unavailable: Storage controller is unavailable\n");
        UNIT_ASSERT_VALUES_EQUAL(service.ListCalls.load(), 2);
    }

    Y_UNIT_TEST_F(InitialRpcErrorIsReportedWithoutPrintingSuccessfulState, TBridgeCliFixture) {
        PrepareStorage(Ydb::Bridge::StorageQuery::PILES);
        auto& service = Service<TBridgeService>();
        service.ListStatus = Ydb::StatusIds::BAD_REQUEST;
        service.ListIssue = "invalid diagnostic filter";
        ExpectFail();
        TString error;
        UNIT_ASSERT(RunList({"--detailed", "--format", "json"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "invalid diagnostic filter");
        UNIT_ASSERT_VALUES_EQUAL(service.ListCalls.load(), 1);
    }

    Y_UNIT_TEST_F(OlderServerCannotPretendStorageDiagnosticsAreHealthy, TBridgeCliFixture) {
        PrepareStorage(Ydb::Bridge::StorageQuery::PILES);
        Service<TBridgeService>().ListResult.clear_storage_state();
        ExpectFail();
        TString error;
        UNIT_ASSERT(RunList({"--detailed"}, &error).empty());
        UNIT_ASSERT_STRING_CONTAINS(error, "Server does not support bridge storage diagnostics");
        UNIT_ASSERT_VALUES_EQUAL(Service<TBridgeService>().ListCalls.load(), 1);
    }

    Y_UNIT_TEST_F(UnavailableStorageKeepsClusterStateButFailsTheDiagnosticQuery, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::PILES);
        storage.set_unavailable_reason("controller is unavailable");
        ExpectFail();
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(RunList({"--detailed", "--format", "json"}), &json));
        UNIT_ASSERT_VALUES_EQUAL(json["generation"].GetUInteger(), 42);
        UNIT_ASSERT_VALUES_EQUAL(json["piles"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["storage_state"]["unavailable_reason"].GetString(), "controller is unavailable");
        UNIT_ASSERT(json["storage_state"]["piles"].GetArray().empty());
        UNIT_ASSERT(!json.Has("incomplete_reason"));
    }

    Y_UNIT_TEST_F(UnavailableDetailedCsvRetainsPileStateWithoutInventingCounters, TBridgeCliFixture) {
        auto& storage = PrepareStorage(Ydb::Bridge::StorageQuery::PILES);
        storage.set_unavailable_reason("controller is unavailable");
        ExpectFail();
        TString error;
        UNIT_ASSERT_VALUES_EQUAL(RunList({"--detailed", "--format", "csv"}, &error),
            "pile_name,state,groups_synced,groups_total,running,pending,unknown,system_groups_synced,system_groups_total,generation\n"
            "primary,PRIMARY,,,,,,,,42\nsecondary,SYNCHRONIZED,,,,,,,,42\n");
        UNIT_ASSERT_STRING_CONTAINS(error, "controller is unavailable");
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
