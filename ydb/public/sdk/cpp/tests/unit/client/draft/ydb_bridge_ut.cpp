#include "helpers/grpc_server.h"

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NBridge;

namespace {

    template <class TRequest, class TResult>
    struct TReply {
        TRequest LastRequest;
        TResult Result;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        grpc::Status RpcStatus;
        std::string Issue;
        bool WrongResultType = false;
        bool OmitResult = false;

        template <class TResponse>
        grpc::Status Respond(const TRequest& request, TResponse& response) {
            LastRequest = request;
            auto* operation = response.mutable_operation();
            operation->set_ready(true);
            operation->set_status(Status);
            if (!Issue.empty()) {
                operation->add_issues()->set_message(Issue);
            }
            if (Status == Ydb::StatusIds::SUCCESS && !OmitResult) {
                if (WrongResultType) {
                    operation->mutable_result()->PackFrom(Ydb::Bridge::UpdateClusterStateResult{});
                } else {
                    operation->mutable_result()->PackFrom(Result);
                }
            }
            return RpcStatus;
        }
    };

    class TBridgeService: public Ydb::Bridge::V1::BridgeService::Service {
    public:
        TReply<Ydb::Bridge::GetClusterStateRequest, Ydb::Bridge::GetClusterStateResult> Cluster;
        TReply<Ydb::Bridge::ListPileSynchronizationRequest, Ydb::Bridge::ListPileSynchronizationResult> Piles;
        TReply<Ydb::Bridge::ListDatabaseSynchronizationRequest, Ydb::Bridge::ListDatabaseSynchronizationResult> Databases;
        TReply<Ydb::Bridge::ListGroupSynchronizationRequest, Ydb::Bridge::ListGroupSynchronizationResult> Groups;

        grpc::Status GetClusterState(grpc::ServerContext*, const Ydb::Bridge::GetClusterStateRequest* request,
                                     Ydb::Bridge::GetClusterStateResponse* response) override {
            return Cluster.Respond(*request, *response);
        }

        grpc::Status ListPileSynchronization(grpc::ServerContext*, const Ydb::Bridge::ListPileSynchronizationRequest* request,
                                             Ydb::Bridge::ListPileSynchronizationResponse* response) override {
            return Piles.Respond(*request, *response);
        }

        grpc::Status ListDatabaseSynchronization(grpc::ServerContext*, const Ydb::Bridge::ListDatabaseSynchronizationRequest* request,
                                                 Ydb::Bridge::ListDatabaseSynchronizationResponse* response) override {
            return Databases.Respond(*request, *response);
        }

        grpc::Status ListGroupSynchronization(grpc::ServerContext*, const Ydb::Bridge::ListGroupSynchronizationRequest* request,
                                              Ydb::Bridge::ListGroupSynchronizationResponse* response) override {
            return Groups.Respond(*request, *response);
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
        counts->set_required(14);
        counts->set_synced(5);
        counts->set_syncing(4);
        counts->set_pending(3);
        counts->set_unknown(2);
    }

    void SetCounts(Ydb::Bridge::DatabaseSyncCounts* counts) {
        counts->set_total(20);
        counts->set_synced(5);
        counts->set_syncing(4);
        counts->set_pending(3);
        counts->set_unknown(2);
        counts->set_not_required(6);
    }

    void CheckCounts(const TGroupSyncCounts& counts) {
        UNIT_ASSERT_VALUES_EQUAL(counts.Required, 14);
        UNIT_ASSERT_VALUES_EQUAL(counts.Synced, 5);
        UNIT_ASSERT_VALUES_EQUAL(counts.Syncing, 4);
        UNIT_ASSERT_VALUES_EQUAL(counts.Pending, 3);
        UNIT_ASSERT_VALUES_EQUAL(counts.Unknown, 2);
    }

    void CheckCounts(const TDatabaseSyncCounts& counts) {
        UNIT_ASSERT_VALUES_EQUAL(counts.Total, 20);
        UNIT_ASSERT_VALUES_EQUAL(counts.Synced, 5);
        UNIT_ASSERT_VALUES_EQUAL(counts.Syncing, 4);
        UNIT_ASSERT_VALUES_EQUAL(counts.Pending, 3);
        UNIT_ASSERT_VALUES_EQUAL(counts.Unknown, 2);
        UNIT_ASSERT_VALUES_EQUAL(counts.NotRequired, 6);
    }

    template <class TResult>
    void SetObservation(TResult& result) {
        result.set_cluster_state_generation(11);
        result.mutable_observed_at()->set_seconds(123);
        result.mutable_observed_at()->set_nanos(456789000);
    }

    void SetPool(Ydb::Bridge::StoragePool* pool, uint64_t boxId, uint64_t poolId, const std::string& name) {
        pool->mutable_id()->set_box_id(boxId);
        pool->mutable_id()->set_pool_id(poolId);
        pool->set_name(name);
    }

} // namespace

Y_UNIT_TEST_SUITE(BridgeClient) {
    Y_UNIT_TEST(ClusterStateContractIsUnchanged) {
        TFixture fixture;
        auto& proto = fixture.Service.Cluster.Result;
        proto.set_generation(10);
        proto.set_conversion_in_progress(true);
        auto* pile = proto.add_pile_states();
        pile->set_pile_name("dc2");
        pile->set_state(Ydb::Bridge::PileState::NOT_SYNCHRONIZED);
        auto* operation = proto.mutable_pending_pile_operation();
        operation->set_pile_name("dc3");
        operation->set_kind(Ydb::Bridge::PendingPileOperation::ADD);
        operation->set_error_reason("retrying provisioning");
        const auto result = fixture.Client.GetClusterState().GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.GetGeneration(), 10);
        UNIT_ASSERT(result.IsConversionInProgress());
        UNIT_ASSERT_VALUES_EQUAL(result.GetState().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.GetState()[0].PileName, "dc2");
        UNIT_ASSERT(result.GetState()[0].State == NYdb::NBridge::EPileState::NOT_SYNCHRONIZED);
        UNIT_ASSERT(result.GetPendingPileOperation());
        UNIT_ASSERT_VALUES_EQUAL(result.GetPendingPileOperation()->PileName, "dc3");
        UNIT_ASSERT(result.GetPendingPileOperation()->Kind == TPendingPileOperation::EKind::ADD);
        UNIT_ASSERT_VALUES_EQUAL(result.GetPendingPileOperation()->ErrorReason, "retrying provisioning");
        UNIT_ASSERT(!fixture.Service.Cluster.LastRequest.include_pile_connectivity());
        UNIT_ASSERT(!fixture.Service.Cluster.LastRequest.include_unbridged_status());
        UNIT_ASSERT(!result.GetUnbridgedStatus());
    }

    Y_UNIT_TEST(ClusterStateUnbridgedStatusSettingsAndPresence) {
        TFixture fixture;
        auto& reply = fixture.Service.Cluster;
        const auto settings = TGetClusterStateSettings().IncludeUnbridgedStatus(true);

        reply.Result.set_generation(10);
        const auto omitted = fixture.Client.GetClusterState(settings).GetValueSync();
        UNIT_ASSERT(omitted.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(omitted.GetGeneration(), 10);
        UNIT_ASSERT(reply.LastRequest.include_unbridged_status());
        UNIT_ASSERT(!omitted.GetUnbridgedStatus());

        reply.Result.clear_generation();
        auto* report = reply.Result.mutable_unbridged_status();
        const auto empty = fixture.Client.GetClusterState(settings).GetValueSync();
        UNIT_ASSERT(empty.IsSuccess());
        UNIT_ASSERT(empty.GetUnbridgedStatus());
        UNIT_ASSERT(empty.GetUnbridgedStatus()->BridgeConfigNodeIds.empty());
        UNIT_ASSERT(empty.GetUnbridgedStatus()->UnreportedStaticNodeIds.empty());

        report->add_bridge_config_node_ids(3);
        report->add_bridge_config_node_ids(5);
        report->add_unreported_static_node_ids(7);
        const auto populated = fixture.Client.GetClusterState(settings).GetValueSync();
        UNIT_ASSERT(populated.IsSuccess());
        UNIT_ASSERT(populated.GetUnbridgedStatus());
        const auto& status = *populated.GetUnbridgedStatus();
        UNIT_ASSERT_VALUES_EQUAL(status.BridgeConfigNodeIds.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(status.BridgeConfigNodeIds[0], 3);
        UNIT_ASSERT_VALUES_EQUAL(status.BridgeConfigNodeIds[1], 5);
        UNIT_ASSERT_VALUES_EQUAL(status.UnreportedStaticNodeIds.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(status.UnreportedStaticNodeIds[0], 7);
    }

    Y_UNIT_TEST(PileSynchronizationAndOperationSettings) {
        TFixture fixture;
        auto& proto = fixture.Service.Piles.Result;
        SetObservation(proto);
        proto.set_unattributed_storage_pools(3);
        auto* pile = proto.add_piles();
        pile->set_pile_name("");
        SetCounts(pile->mutable_databases());
        auto* unplaced = proto.add_piles();
        unplaced->mutable_databases()->set_total(1);
        unplaced->mutable_databases()->set_not_required(1);
        const auto result = fixture.Client.ListPileSynchronization(
                                              TListPileSynchronizationSettings().OperationTimeout(TDuration::Seconds(3)))
                                .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetClusterStateGeneration(), 11);
        UNIT_ASSERT_VALUES_EQUAL(result.GetObservedAt().MicroSeconds(), 123456789);
        UNIT_ASSERT_VALUES_EQUAL(result.GetUnattributedStoragePools(), 3);
        const auto& piles = result.GetPiles();
        UNIT_ASSERT_VALUES_EQUAL(piles.size(), 2);
        UNIT_ASSERT(piles[0].PileName.has_value());
        UNIT_ASSERT(piles[0].PileName->empty());
        CheckCounts(piles[0].Databases);
        UNIT_ASSERT(!piles[1].PileName);
        UNIT_ASSERT_VALUES_EQUAL(piles[1].Databases.Total, 1);
        UNIT_ASSERT_VALUES_EQUAL(piles[1].Databases.NotRequired, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Piles.LastRequest.operation_params().operation_timeout().seconds(), 3);
    }

    Y_UNIT_TEST(DatabaseSynchronizationSettingsAndQualifiedPools) {
        TFixture fixture;
        auto& proto = fixture.Service.Databases.Result;
        SetObservation(proto);
        proto.set_storage_owner_database_path("/Root/shared");
        auto* database = proto.add_databases();
        database->set_database_path("/Root/shared");
        SetPool(database->add_storage_pools(), 1, 2, "pool,ssd");
        SetPool(database->add_storage_pools(), 3, 2, "pool,ssd");
        database->set_storage_mode(Ydb::Bridge::MIXED);
        database->set_pile_name("");
        SetCounts(database->mutable_groups());
        database->mutable_last_sync_error()->set_message("retrying");
        database->mutable_last_sync_error()->mutable_occurred_at()->set_nanos(200000);
        auto* anonymous = proto.add_databases();
        anonymous->set_storage_mode(Ydb::Bridge::ORDINARY);
        SetPool(anonymous->add_storage_pools(), 9, 9, "unresolved");
        anonymous->mutable_last_sync_error()->set_message("timestamp unavailable");

        const auto settings = TListDatabaseSynchronizationSettings().DatabasePath("/Root/serverless").PileName("").OnlyIncomplete(true).StorageMode(EStorageMode::Ordinary);
        const auto result = fixture.Client.ListDatabaseSynchronization(settings).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& request = fixture.Service.Databases.LastRequest;
        UNIT_ASSERT(request.has_database_path());
        UNIT_ASSERT_VALUES_EQUAL(request.database_path(), "/Root/serverless");
        UNIT_ASSERT(request.has_pile_name());
        UNIT_ASSERT(request.pile_name().empty());
        UNIT_ASSERT(request.only_incomplete());
        UNIT_ASSERT(request.has_storage_mode());
        UNIT_ASSERT(request.storage_mode() == Ydb::Bridge::ORDINARY);
        UNIT_ASSERT_VALUES_EQUAL(result.GetClusterStateGeneration(), 11);
        UNIT_ASSERT_VALUES_EQUAL(result.GetObservedAt().MicroSeconds(), 123456789);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStorageOwnerDatabasePath().value(), "/Root/shared");
        UNIT_ASSERT_VALUES_EQUAL(result.GetDatabases().size(), 2);
        const auto& row = result.GetDatabases()[0];
        UNIT_ASSERT_VALUES_EQUAL(row.DatabasePath.value(), "/Root/shared");
        UNIT_ASSERT(row.PileName.has_value() && row.PileName.value().empty());
        UNIT_ASSERT(row.StorageMode == EStorageMode::Mixed);
        CheckCounts(row.Groups);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePools.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePools[0].Id.BoxId, 1);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePools[1].Id.BoxId, 3);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePools[0].Id.PoolId, 2);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePools[1].Id.PoolId, 2);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePools[0].Name, "pool,ssd");
        UNIT_ASSERT(row.LastSyncError);
        UNIT_ASSERT_VALUES_EQUAL(row.LastSyncError->Message, "retrying");
        UNIT_ASSERT(row.LastSyncError->OccurredAt);
        UNIT_ASSERT_VALUES_EQUAL(row.LastSyncError->OccurredAt->MicroSeconds(), 200);
        const auto& unknown = result.GetDatabases()[1];
        UNIT_ASSERT(!unknown.DatabasePath);
        UNIT_ASSERT(!unknown.PileName);
        UNIT_ASSERT(unknown.StorageMode == EStorageMode::Ordinary);
        UNIT_ASSERT(unknown.LastSyncError);
        UNIT_ASSERT(!unknown.LastSyncError->OccurredAt);
    }

    Y_UNIT_TEST(GroupSynchronizationSettingsAndPresence) {
        TFixture fixture;
        auto& proto = fixture.Service.Groups.Result;
        SetObservation(proto);
        proto.set_storage_owner_database_path("/Root/shared");
        auto* group = proto.add_groups();
        group->set_group_id(100);
        group->set_physical_group_id(0);
        SetPool(group->mutable_storage_pool(), 2, 5, "pool");
        group->set_pile_name("");
        group->set_sync_status(Ydb::Bridge::SYNCING);
        group->set_sync_phase("WRITE_KEEP");
        group->mutable_last_sync_error()->set_message("previous failure");
        group->mutable_last_sync_error()->mutable_occurred_at()->set_nanos(300000);
        proto.add_groups()->set_group_id(101);
        const auto result = fixture.Client.ListGroupSynchronization(TGroupSynchronizationScope::Database("/Root/serverless"),
                                                                    TListGroupSynchronizationSettings().PileName("").Statuses({ESynchronizationStatus::Pending, ESynchronizationStatus::Unknown}))
                                .GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& request = fixture.Service.Groups.LastRequest;
        UNIT_ASSERT(request.scope_case() == Ydb::Bridge::ListGroupSynchronizationRequest::kDatabasePath);
        UNIT_ASSERT_VALUES_EQUAL(request.database_path(), "/Root/serverless");
        UNIT_ASSERT(request.has_pile_name() && request.pile_name().empty());
        UNIT_ASSERT_VALUES_EQUAL(request.statuses_size(), 2);
        UNIT_ASSERT(request.statuses(0) == Ydb::Bridge::PENDING);
        UNIT_ASSERT(request.statuses(1) == Ydb::Bridge::UNKNOWN);
        UNIT_ASSERT_VALUES_EQUAL(result.GetClusterStateGeneration(), 11);
        UNIT_ASSERT_VALUES_EQUAL(result.GetObservedAt().MicroSeconds(), 123456789);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStorageOwnerDatabasePath().value(), "/Root/shared");
        UNIT_ASSERT_VALUES_EQUAL(result.GetGroups().size(), 2);
        const auto& row = result.GetGroups()[0];
        UNIT_ASSERT_VALUES_EQUAL(row.GroupId, 100);
        UNIT_ASSERT(row.PhysicalGroupId.has_value());
        UNIT_ASSERT_VALUES_EQUAL(row.PhysicalGroupId.value(), 0);
        UNIT_ASSERT(row.PileName.has_value() && row.PileName.value().empty());
        UNIT_ASSERT(row.StoragePool);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePool->Id.BoxId, 2);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePool->Id.PoolId, 5);
        UNIT_ASSERT_VALUES_EQUAL(row.StoragePool->Name, "pool");
        UNIT_ASSERT(row.SyncStatus == ESynchronizationStatus::Syncing);
        UNIT_ASSERT_VALUES_EQUAL(row.SyncPhase, "WRITE_KEEP");
        UNIT_ASSERT(row.LastSyncError);
        UNIT_ASSERT_VALUES_EQUAL(row.LastSyncError->Message, "previous failure");
        UNIT_ASSERT(row.LastSyncError->OccurredAt);
        UNIT_ASSERT_VALUES_EQUAL(row.LastSyncError->OccurredAt->MicroSeconds(), 300);
        UNIT_ASSERT(!result.GetGroups()[1].PhysicalGroupId);
        UNIT_ASSERT(!result.GetGroups()[1].PileName);
        UNIT_ASSERT(!result.GetGroups()[1].LastSyncError);
    }

    Y_UNIT_TEST(PoolScopeAndDefaultSettings) {
        TFixture fixture;
        auto* unowned = fixture.Service.Groups.Result.add_groups();
        unowned->set_group_id(0);
        unowned->set_physical_group_id(0);
        unowned->set_pile_name("");
        unowned->set_sync_status(Ydb::Bridge::SYNCED);
        const auto all = fixture.Client.ListGroupSynchronization(TGroupSynchronizationScope::All()).GetValueSync();
        UNIT_ASSERT_C(all.IsSuccess(), all.GetIssues().ToString());
        UNIT_ASSERT(fixture.Service.Groups.LastRequest.scope_case() == Ydb::Bridge::ListGroupSynchronizationRequest::SCOPE_NOT_SET);
        UNIT_ASSERT_VALUES_EQUAL(all.GetGroups().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(all.GetGroups()[0].GroupId, 0);
        UNIT_ASSERT(all.GetGroups()[0].PhysicalGroupId.has_value());
        UNIT_ASSERT_VALUES_EQUAL(all.GetGroups()[0].PhysicalGroupId.value(), 0);
        UNIT_ASSERT(!all.GetGroups()[0].StoragePool);
        UNIT_ASSERT(all.GetGroups()[0].PileName.has_value() && all.GetGroups()[0].PileName->empty());
        fixture.Service.Groups.Result.clear_groups();
        auto result = fixture.Client.ListGroupSynchronization(TGroupSynchronizationScope::PoolName("pool,ssd")).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Groups.LastRequest.storage_pool_name(), "pool,ssd");
        UNIT_ASSERT(!fixture.Service.Groups.LastRequest.has_pile_name());
        UNIT_ASSERT(fixture.Service.Groups.LastRequest.statuses().empty());
        UNIT_ASSERT(!result.GetStorageOwnerDatabasePath());
        fixture.Service.Groups.Status = Ydb::StatusIds::BAD_REQUEST;
        fixture.Service.Groups.Issue = "storage pool name must not be empty";
        const auto emptyPool = fixture.Client.ListGroupSynchronization(TGroupSynchronizationScope::PoolName("")).GetValueSync();
        UNIT_ASSERT(emptyPool.GetStatus() == EStatus::BAD_REQUEST);
        UNIT_ASSERT(fixture.Service.Groups.LastRequest.scope_case() == Ydb::Bridge::ListGroupSynchronizationRequest::kStoragePoolName);
        UNIT_ASSERT(fixture.Service.Groups.LastRequest.storage_pool_name().empty());
        fixture.Service.Groups.Status = Ydb::StatusIds::SUCCESS;
        fixture.Service.Groups.Issue.clear();
        result = fixture.Client.ListGroupSynchronization(TGroupSynchronizationScope::PoolId({7, 13})).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT(fixture.Service.Groups.LastRequest.scope_case() == Ydb::Bridge::ListGroupSynchronizationRequest::kStoragePoolId);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Groups.LastRequest.storage_pool_id().box_id(), 7);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Service.Groups.LastRequest.storage_pool_id().pool_id(), 13);
        const auto databases = fixture.Client.ListDatabaseSynchronization().GetValueSync();
        UNIT_ASSERT(databases.IsSuccess());
        UNIT_ASSERT(!fixture.Service.Databases.LastRequest.has_database_path());
        UNIT_ASSERT(!fixture.Service.Databases.LastRequest.has_pile_name());
        UNIT_ASSERT(!fixture.Service.Databases.LastRequest.only_incomplete());
        UNIT_ASSERT(!fixture.Service.Databases.LastRequest.has_storage_mode());
        UNIT_ASSERT(databases.GetDatabases().empty());
    }

    Y_UNIT_TEST(SynchronizationStatusesFromProto) {
        struct TCase {
            Ydb::Bridge::SynchronizationStatus Proto;
            ESynchronizationStatus Sdk;
        };
        const std::vector<TCase> statuses = {
            {Ydb::Bridge::SYNCHRONIZATION_STATUS_UNSPECIFIED, ESynchronizationStatus::Unspecified},
            {Ydb::Bridge::SYNCED, ESynchronizationStatus::Synced},
            {Ydb::Bridge::SYNCING, ESynchronizationStatus::Syncing},
            {Ydb::Bridge::PENDING, ESynchronizationStatus::Pending},
            {Ydb::Bridge::NOT_REQUIRED, ESynchronizationStatus::NotRequired},
            {Ydb::Bridge::UNKNOWN, ESynchronizationStatus::Unknown},
            {static_cast<Ydb::Bridge::SynchronizationStatus>(1000), ESynchronizationStatus::Unknown},
        };
        TFixture fixture;
        for (const auto& status : statuses) {
            fixture.Service.Groups.Result.add_groups()->set_sync_status(status.Proto);
        }
        const auto result = fixture.Client.ListGroupSynchronization(TGroupSynchronizationScope::PoolName("pool")).GetValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.GetGroups().size(), statuses.size());
        for (size_t i = 0; i < statuses.size(); ++i) {
            UNIT_ASSERT(result.GetGroups()[i].SyncStatus == statuses[i].Sdk);
        }
    }

    Y_UNIT_TEST(StorageModesFromProto) {
        struct TModeCase {
            Ydb::Bridge::StorageMode Proto;
            EStorageMode Sdk;
        };
        const std::vector<TModeCase> modes = {
            {Ydb::Bridge::BRIDGED, EStorageMode::Bridged},
            {Ydb::Bridge::ORDINARY, EStorageMode::Ordinary},
            {Ydb::Bridge::MIXED, EStorageMode::Mixed},
            {Ydb::Bridge::STORAGE_MODE_UNSPECIFIED, EStorageMode::Unspecified},
            {static_cast<Ydb::Bridge::StorageMode>(1000), EStorageMode::Unspecified},
        };
        TFixture fixture;
        for (const auto& mode : modes) {
            fixture.Service.Databases.Result.add_databases()->set_storage_mode(mode.Proto);
        }
        const auto result = fixture.Client.ListDatabaseSynchronization().GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetDatabases().size(), modes.size());
        for (size_t i = 0; i < modes.size(); ++i) {
            UNIT_ASSERT_C(result.GetDatabases()[i].StorageMode == modes[i].Sdk,
                          "Unexpected SDK storage mode for proto value " << static_cast<int>(modes[i].Proto));
        }
    }

    Y_UNIT_TEST(DiagnosticsErrorsAndUnsupportedServer) {
        TFixture fixture;
        fixture.Service.Piles.Status = Ydb::StatusIds::UNAVAILABLE;
        fixture.Service.Piles.Issue = "controller unavailable";
        const auto piles = fixture.Client.ListPileSynchronization().GetValueSync();
        UNIT_ASSERT(piles.GetStatus() == EStatus::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(piles.GetIssues().ToString(), "controller unavailable");
        UNIT_ASSERT(piles.GetPiles().empty());
        fixture.Service.Databases.Status = Ydb::StatusIds::BAD_REQUEST;
        fixture.Service.Databases.Issue = "invalid database scope";
        const auto databases = fixture.Client.ListDatabaseSynchronization().GetValueSync();
        UNIT_ASSERT(databases.GetStatus() == EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(databases.GetIssues().ToString(), "invalid database scope");
        UNIT_ASSERT(databases.GetDatabases().empty());
        fixture.Service.Groups.Status = Ydb::StatusIds::PRECONDITION_FAILED;
        fixture.Service.Groups.Issue = "Result limit exceeded; narrow database/pool/pile/status filter";
        const auto oversized = fixture.Client.ListGroupSynchronization(TGroupSynchronizationScope::PoolName("pool")).GetValueSync();
        UNIT_ASSERT(oversized.GetStatus() == EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT_STRING_CONTAINS(oversized.GetIssues().ToString(), "narrow database/pool/pile/status filter");
        UNIT_ASSERT(oversized.GetGroups().empty());
        fixture.Service.Groups.Status = Ydb::StatusIds::SUCCESS;
        fixture.Service.Groups.Issue.clear();
        fixture.Service.Groups.RpcStatus = grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "old server");
        const auto groups = fixture.Client.ListGroupSynchronization(TGroupSynchronizationScope::PoolName("pool")).GetValueSync();
        UNIT_ASSERT(!groups.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(groups.GetIssues().ToString(), "old server");
        UNIT_ASSERT(groups.GetGroups().empty());
    }

    Y_UNIT_TEST(InvalidSuccessfulPayloadIsNotAnEmptySuccessfulObservation) {
        TFixture fixture;
        fixture.Service.Piles.WrongResultType = true;
        const auto result = fixture.Client.ListPileSynchronization().GetValueSync();
        UNIT_ASSERT(result.GetStatus() == EStatus::CLIENT_INTERNAL_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Invalid bridge diagnostics response");
        fixture.Service.Databases.OmitResult = true;
        const auto response = fixture.Client.ListDatabaseSynchronization().GetValueSync();
        UNIT_ASSERT(response.GetStatus() == EStatus::CLIENT_INTERNAL_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(response.GetIssues().ToString(), "Invalid bridge diagnostics response");
        UNIT_ASSERT(response.GetDatabases().empty());
    }
} // Y_UNIT_TEST_SUITE(BridgeClient)
