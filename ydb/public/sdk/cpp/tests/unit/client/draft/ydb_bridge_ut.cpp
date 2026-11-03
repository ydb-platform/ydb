#include "helpers/grpc_server.h"

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NBridge;

namespace {

    class TBridgeService: public Ydb::Bridge::V1::BridgeService::Service {
    public:
        Ydb::Bridge::GetClusterStateRequest LastRequest;
        Ydb::Bridge::GetClusterStateResult Result;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        grpc::Status RpcStatus;
        std::string Issue;

        grpc::Status GetClusterState(grpc::ServerContext*, const Ydb::Bridge::GetClusterStateRequest* request,
                                     Ydb::Bridge::GetClusterStateResponse* response) override {
            LastRequest = *request;
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Status);
            if (!Issue.empty()) {
                operation->add_issues()->set_message(Issue);
            }
            if (Status == Ydb::StatusIds::SUCCESS) {
                operation->mutable_result()->PackFrom(Result);
            }
            return RpcStatus;
        }
    };

    struct TFixture {
        NTesting::TPortHolder Port = NTesting::GetFreePort();
        TBridgeService Service;
        std::string Address = "localhost:" + std::to_string(static_cast<ui16>(Port));
        std::unique_ptr<grpc::Server> Server = StartGrpcServer(Address, Service);
        TDriver Driver{TDriverConfig().SetEndpoint(Address).SetDiscoveryMode(EDiscoveryMode::Off)};
        TBridgeClient Client{Driver};
    };

    void SetCounts(Ydb::Bridge::GroupSyncCounts* counts) {
        counts->set_total(14);
        counts->set_synced(5);
        counts->set_running(4);
        counts->set_pending(3);
        counts->set_unknown(2);
    }

    void CheckCounts(const TGroupSyncCounts& counts) {
        UNIT_ASSERT_VALUES_EQUAL(counts.Total, 14);
        UNIT_ASSERT_VALUES_EQUAL(counts.Synced, 5);
        UNIT_ASSERT_VALUES_EQUAL(counts.Running, 4);
        UNIT_ASSERT_VALUES_EQUAL(counts.Pending, 3);
        UNIT_ASSERT_VALUES_EQUAL(counts.Unknown, 2);
    }

} // namespace

Y_UNIT_TEST_SUITE(BridgeClient) {
    Y_UNIT_TEST(StorageQueryAndResult) {
        TFixture fixture;
        auto& result = fixture.Service.Result;
        result.set_generation(10);
        result.set_conversion_in_progress(true);
        auto* pileState = result.add_pile_states();
        pileState->set_pile_name("dc2");
        pileState->set_state(Ydb::Bridge::PileState::NOT_SYNCHRONIZED);
        auto* operation = result.mutable_pending_pile_operation();
        operation->set_pile_name("dc3");
        operation->set_kind(Ydb::Bridge::PendingPileOperation::ADD);
        operation->set_error_reason("retrying provisioning");
        auto* storage = result.mutable_storage_state();
        storage->set_generation(11);
        storage->set_collected_at_us(123456789);
        storage->set_database_path("/Root/serverless");
        storage->set_storage_owner_database("/Root/shared");
        storage->set_next_page_token("next");
        auto* pile = storage->add_piles();
        pile->set_pile_name("dc2");
        SetCounts(pile->mutable_user_groups());
        SetCounts(pile->mutable_system_groups());
        auto* database = storage->add_databases();
        database->set_database_path("/Root/shared");
        database->add_storage_pools("pool,ssd");
        database->add_storage_pools("pool:hdd");
        database->set_mode(Ydb::Bridge::DatabaseStorageState::MIXED);
        database->set_pile_name("dc2");
        SetCounts(database->mutable_groups());
        database->set_state(Ydb::Bridge::GroupStorageState::SYNCING);
        database->set_last_error("last database error");
        database->set_last_error_timestamp_us(200);
        auto* group = storage->add_groups();
        group->set_group_id(100);
        group->set_part_group_id(101);
        group->set_storage_pool_name("pool");
        group->set_pile_name("dc2");
        group->set_state(Ydb::Bridge::GroupStorageState::PENDING);
        group->set_stage("waiting");
        group->set_last_error("last group error");
        group->set_last_error_timestamp_us(300);

        TStorageQuery query;
        query.View = EStorageView::Groups;
        query.DatabasePath = "/Root/serverless";
        query.StoragePool = "pool";
        query.PileName = "dc2";
        query.NotSynced = true;
        query.PageSize = 23;
        query.PageToken = "previous";
        const auto settings = TGetClusterStateSettings().StorageQuery(query);
        const auto response = fixture.Client.GetClusterState(settings).GetValueSync();
        UNIT_ASSERT_C(response.IsSuccess(), response.GetIssues().ToString());
        UNIT_ASSERT(fixture.Service.LastRequest.has_storage_query());
        const auto& request = fixture.Service.LastRequest.storage_query();
        UNIT_ASSERT(request.view() == Ydb::Bridge::StorageQuery::GROUPS);
        UNIT_ASSERT_VALUES_EQUAL(request.database_path(), query.DatabasePath);
        UNIT_ASSERT_VALUES_EQUAL(request.storage_pool(), query.StoragePool);
        UNIT_ASSERT_VALUES_EQUAL(request.pile_name(), query.PileName);
        UNIT_ASSERT(request.not_synced());
        UNIT_ASSERT_VALUES_EQUAL(request.page_size(), query.PageSize);
        UNIT_ASSERT_VALUES_EQUAL(request.page_token(), query.PageToken);
        UNIT_ASSERT_VALUES_EQUAL(response.GetGeneration(), 10);
        UNIT_ASSERT(response.IsConversionInProgress());
        UNIT_ASSERT_VALUES_EQUAL(response.GetState().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetState().front().PileName, "dc2");
        UNIT_ASSERT(response.GetState().front().State == NBridge::EPileState::NOT_SYNCHRONIZED);
        UNIT_ASSERT(response.GetPendingPileOperation());
        UNIT_ASSERT_VALUES_EQUAL(response.GetPendingPileOperation()->ErrorReason, "retrying provisioning");
        UNIT_ASSERT(response.GetStorageState());
        const auto& state = *response.GetStorageState();
        UNIT_ASSERT_VALUES_EQUAL(state.Generation, 11);
        UNIT_ASSERT_VALUES_EQUAL(state.CollectedAtUs, 123456789);
        UNIT_ASSERT_VALUES_EQUAL(state.DatabasePath, "/Root/serverless");
        UNIT_ASSERT_VALUES_EQUAL(state.StorageOwnerDatabase, "/Root/shared");
        UNIT_ASSERT_VALUES_EQUAL(state.NextPageToken, "next");
        UNIT_ASSERT_VALUES_EQUAL(state.Piles.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(state.Piles.front().PileName, "dc2");
        CheckCounts(state.Piles.front().UserGroups);
        CheckCounts(state.Piles.front().SystemGroups);
        UNIT_ASSERT_VALUES_EQUAL(state.Databases.size(), 1);
        const auto& databaseState = state.Databases.front();
        UNIT_ASSERT_VALUES_EQUAL(databaseState.DatabasePath, "/Root/shared");
        UNIT_ASSERT_VALUES_EQUAL(databaseState.StoragePools.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(databaseState.StoragePools[0], "pool,ssd");
        UNIT_ASSERT_VALUES_EQUAL(databaseState.StoragePools[1], "pool:hdd");
        UNIT_ASSERT(databaseState.Mode == EStorageMode::Mixed);
        UNIT_ASSERT_VALUES_EQUAL(databaseState.PileName, "dc2");
        CheckCounts(databaseState.Groups);
        UNIT_ASSERT(databaseState.State == ECopySyncState::Syncing);
        UNIT_ASSERT_VALUES_EQUAL(databaseState.LastError, "last database error");
        UNIT_ASSERT_VALUES_EQUAL(databaseState.LastErrorTimestampUs, 200);
        UNIT_ASSERT_VALUES_EQUAL(state.Groups.size(), 1);
        const auto& groupState = state.Groups.front();
        UNIT_ASSERT_VALUES_EQUAL(groupState.GroupId, 100);
        UNIT_ASSERT_VALUES_EQUAL(groupState.PartGroupId, 101);
        UNIT_ASSERT_VALUES_EQUAL(groupState.StoragePoolName, "pool");
        UNIT_ASSERT_VALUES_EQUAL(groupState.PileName, "dc2");
        UNIT_ASSERT(groupState.State == ECopySyncState::Pending);
        UNIT_ASSERT_VALUES_EQUAL(groupState.Stage, "waiting");
        UNIT_ASSERT_VALUES_EQUAL(groupState.LastError, "last group error");
        UNIT_ASSERT_VALUES_EQUAL(groupState.LastErrorTimestampUs, 300);
    }

    Y_UNIT_TEST(OptionalStorageState) {
        TFixture fixture;
        auto result = fixture.Client.GetClusterState().GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT(!fixture.Service.LastRequest.has_storage_query());
        UNIT_ASSERT(!result.GetStorageState());

        result = fixture.Client.GetClusterState(TGetClusterStateSettings().StorageQuery(TStorageQuery{})).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT(fixture.Service.LastRequest.has_storage_query());
        UNIT_ASSERT(fixture.Service.LastRequest.storage_query().view() == Ydb::Bridge::StorageQuery::PILES);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.LastRequest.storage_query().page_size(), 1000);
        UNIT_ASSERT(!result.GetStorageState());

        fixture.Service.Result.mutable_storage_state()->set_unavailable_reason("controller unavailable");
        result = fixture.Client.GetClusterState(TGetClusterStateSettings().StorageQuery(TStorageQuery{})).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT(result.GetStorageState());
        UNIT_ASSERT_VALUES_EQUAL(result.GetStorageState()->UnavailableReason, "controller unavailable");
    }

    Y_UNIT_TEST(UnknownStorageEnums) {
        TFixture fixture;
        auto* storage = fixture.Service.Result.mutable_storage_state();
        const std::vector<ECopySyncState> states = {ECopySyncState::Unspecified, ECopySyncState::Synced,
                                                    ECopySyncState::Syncing, ECopySyncState::Pending, ECopySyncState::NotRequired, ECopySyncState::Unknown};
        for (size_t i = 0; i < states.size(); ++i) {
            storage->add_groups()->set_state(static_cast<Ydb::Bridge::GroupStorageState::State>(i));
        }
        storage->add_groups()->set_state(static_cast<Ydb::Bridge::GroupStorageState::State>(1000));
        auto* database = storage->add_databases();
        database->set_mode(static_cast<Ydb::Bridge::DatabaseStorageState::Mode>(1000));
        database->set_state(static_cast<Ydb::Bridge::GroupStorageState::State>(1000));
        const auto result = fixture.Client.GetClusterState(TGetClusterStateSettings().StorageQuery(TStorageQuery{})).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT(result.GetStorageState());
        const auto& state = *result.GetStorageState();
        UNIT_ASSERT_VALUES_EQUAL(state.Groups.size(), states.size() + 1);
        for (size_t i = 0; i < states.size(); ++i) {
            UNIT_ASSERT(state.Groups[i].State == states[i]);
        }
        UNIT_ASSERT(state.Groups.back().State == ECopySyncState::Unknown);
        UNIT_ASSERT(state.Databases.front().Mode == EStorageMode::Unspecified);
        UNIT_ASSERT(state.Databases.front().State == ECopySyncState::Unknown);
        UNIT_ASSERT(state.Databases.front().StoragePools.empty());
    }

    Y_UNIT_TEST(StorageErrorsPreserveStatus) {
        TFixture fixture;
        const auto settings = TGetClusterStateSettings().StorageQuery(TStorageQuery{});
        fixture.Service.Status = Ydb::StatusIds::BAD_REQUEST;
        fixture.Service.Issue = "invalid storage filter";
        auto result = fixture.Client.GetClusterState(settings).GetValueSync();
        UNIT_ASSERT(result.GetStatus() == EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "invalid storage filter");
        UNIT_ASSERT(!result.GetStorageState());

        fixture.Service.RpcStatus = grpc::Status(grpc::StatusCode::UNAVAILABLE, "transport unavailable");
        result = fixture.Client.GetClusterState(settings).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "transport unavailable");
        UNIT_ASSERT(!result.GetStorageState());
    }
} // Y_UNIT_TEST_SUITE(BridgeClient)
