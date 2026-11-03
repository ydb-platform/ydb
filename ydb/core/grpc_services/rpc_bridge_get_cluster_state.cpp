#include "rpc_bridge_base.h"
#include "service_bridge.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/bridge_events.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/blobstorage_distributed_config.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NGRpcService {
    namespace {

        using namespace NActors;

        using TEvGetClusterStateRequest = TGrpcRequestOperationCall<Ydb::Bridge::GetClusterStateRequest,
                                                                    Ydb::Bridge::GetClusterStateResponse>;

        void CopyFromInternalClusterState(const NKikimrBridge::TClusterState& from, const TBridgeInfo& bridgeInfo, Ydb::Bridge::GetClusterStateResult& to) {
            for (ui32 i = 0; i < from.PerPileStateSize(); ++i) {
                const auto* pile = bridgeInfo.FindPile(TBridgePileId::FromPileIndex(i));
                if (!pile || pile->IsFreeSlot) {
                    continue;
                }
                auto* state = to.add_pile_states();
                state->set_pile_name(pile->Name);
                state->set_state(GetPublicBridgePileState(from, TBridgePileId::FromPileIndex(i)));
            }
            to.set_generation(from.GetGeneration());
        }

        void CopyFromInternalClusterStateDetails(const NKikimrBridge::TClusterStateDetails& from, const TBridgeInfo& bridgeInfo,
                                                 Ydb::Bridge::GetClusterStateResult& to) {
            to.set_conversion_in_progress(from.GetBscConversionPending());
            if (from.HasPendingPileOperation()) {
                const auto& op = from.GetPendingPileOperation();
                auto* pb = to.mutable_pending_pile_operation();
                using TOperation = NKikimrBridge::TClusterStateDetails::TPendingPileOperation;
                if (const auto* pile = bridgeInfo.FindPile(TBridgePileId::FromProto(&op, &TOperation::GetBridgePileId))) {
                    pb->set_pile_name(pile->Name);
                }
                switch (op.GetKind()) {
                    case TOperation::ADD:
                        pb->set_kind(Ydb::Bridge::PendingPileOperation::ADD);
                        break;

                    case TOperation::REMOVE:
                        pb->set_kind(Ydb::Bridge::PendingPileOperation::REMOVE);
                        break;

                    case NKikimrBridge::TClusterStateDetails_TPendingPileOperation_EKind_TClusterStateDetails_TPendingPileOperation_EKind_INT_MIN_SENTINEL_DO_NOT_USE_:
                    case NKikimrBridge::TClusterStateDetails_TPendingPileOperation_EKind_TClusterStateDetails_TPendingPileOperation_EKind_INT_MAX_SENTINEL_DO_NOT_USE_:
                        Y_ABORT();
                }
                pb->set_error_reason(op.GetErrorReason());
            }
        }

        void CopyStorageCounts(const NKikimrBridge::TStorageCounts& from, Ydb::Bridge::GroupSyncCounts* to) {
            to->set_total(from.GetTotal());
            to->set_synced(from.GetSynced());
            to->set_running(from.GetRunning());
            to->set_pending(from.GetPending());
            to->set_unknown(from.GetUnknown());
        }

        NKikimrBridge::TStorageQuery::EView GetStorageView(Ydb::Bridge::StorageQuery::View view) {
            switch (view) {
                case Ydb::Bridge::StorageQuery::PILES:
                    return NKikimrBridge::TStorageQuery::PILES;
                case Ydb::Bridge::StorageQuery::DATABASES:
                    return NKikimrBridge::TStorageQuery::DATABASES;
                case Ydb::Bridge::StorageQuery::GROUPS:
                    return NKikimrBridge::TStorageQuery::GROUPS;
                default:
                    return NKikimrBridge::TStorageQuery::VIEW_UNSPECIFIED;
            }
        }

        Ydb::Bridge::GroupStorageState::State GetPublicStorageState(NKikimrBridge::TStorageGroup::EState state) {
            switch (state) {
                case NKikimrBridge::TStorageGroup::SYNCED:
                    return Ydb::Bridge::GroupStorageState::SYNCED;
                case NKikimrBridge::TStorageGroup::SYNCING:
                    return Ydb::Bridge::GroupStorageState::SYNCING;
                case NKikimrBridge::TStorageGroup::PENDING:
                    return Ydb::Bridge::GroupStorageState::PENDING;
                case NKikimrBridge::TStorageGroup::NOT_REQUIRED:
                    return Ydb::Bridge::GroupStorageState::NOT_REQUIRED;
                case NKikimrBridge::TStorageGroup::UNKNOWN:
                    return Ydb::Bridge::GroupStorageState::UNKNOWN;
                default:
                    return Ydb::Bridge::GroupStorageState::STATE_UNSPECIFIED;
            }
        }

        Ydb::Bridge::DatabaseStorageState::Mode GetPublicStorageMode(NKikimrBridge::TStorageDatabase::EMode mode) {
            switch (mode) {
                case NKikimrBridge::TStorageDatabase::BRIDGED:
                    return Ydb::Bridge::DatabaseStorageState::BRIDGED;
                case NKikimrBridge::TStorageDatabase::ORDINARY:
                    return Ydb::Bridge::DatabaseStorageState::ORDINARY;
                case NKikimrBridge::TStorageDatabase::MIXED:
                    return Ydb::Bridge::DatabaseStorageState::MIXED;
                default:
                    return Ydb::Bridge::DatabaseStorageState::MODE_UNSPECIFIED;
            }
        }

        class TGetClusterStateRequest: public TBridgeRequestGrpc<TGetClusterStateRequest, TEvGetClusterStateRequest,
                                                                 Ydb::Bridge::GetClusterStateResult> {
            using TBase = TBridgeRequestGrpc<TGetClusterStateRequest, TEvGetClusterStateRequest, Ydb::Bridge::GetClusterStateResult>;
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;

            enum class EStoragePhase { TargetDatabase,
                                       SharedDatabase,
                                       Controller,
                                       DatabaseOwners };

            Ydb::Bridge::GetClusterStateResult Result;
            NKikimrBridge::TStorageQuery StorageQuery;
            TActorId StoragePipe;
            EStoragePhase StoragePhase = EStoragePhase::TargetDatabase;
            TString StorageOwnerDatabase;
            TPathId SharedDatabaseId;
            THashMap<TPathId, TVector<int>> DatabaseOwnerRows;
            static constexpr ui64 StorageTimeoutTag = 0x425249444745;

        public:
            using TBase::TBase;

            void SendRequest(const TActorContext& ctx) {
                ctx.Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new TEvNodeWardenQueryStorageConfig(false));
            }

            bool ValidateRequest(Ydb::StatusIds::StatusCode& status, NYql::TIssues& issues) {
                if (!GetProtoRequest()->has_storage_query()) {
                    return true;
                }
                const auto& query = GetProtoRequest()->storage_query();
                const TString databasePath(query.database_path());
                TString error;
                if (query.view() < Ydb::Bridge::StorageQuery::PILES || query.view() > Ydb::Bridge::StorageQuery::GROUPS) {
                    error = "Unknown storage diagnostic view";
                } else if (query.page_size() > 10000) {
                    error = "Storage page_size must not exceed 10000";
                } else if (query.page_token().size() > 1024) {
                    error = "Storage page_token is too long";
                } else if (!databasePath.empty() && (!databasePath.StartsWith('/') || NKikimr::SplitPath(databasePath).empty())) {
                    error = "Storage database_path must be an absolute database path";
                } else if (query.view() == Ydb::Bridge::StorageQuery::GROUPS &&
                           query.database_path().empty() && query.storage_pool().empty()) {
                    error = "Group diagnostics require a database or storage pool filter";
                }
                if (error.empty()) {
                    return true;
                }
                status = Ydb::StatusIds::BAD_REQUEST;
                issues.AddIssue(error);
                return false;
            }

            STFUNC(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvNodeWardenStorageConfig, HandleConfiguration);
                    hFunc(NStorage::TEvNodeConfigInvokeOnRootResult, HandleConnectivity);
                    hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleDatabaseMetadata);
                    hFunc(TEvBlobStorage::TEvControllerQueryBridgeStorageResult, HandleStorageSnapshot);
                    hFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnected);
                    hFunc(TEvTabletPipe::TEvClientDestroyed, HandlePipeDestroyed);
                    case TEvents::TSystem::Wakeup:
                        if (ev->Get<TEvents::TEvWakeup>()->Tag == StorageTimeoutTag) {
                            HandleStorageTimeout();
                        } else {
                            TBase::StateFuncBase(ev);
                        }
                        break;
                    default:
                        return TBase::StateFuncBase(ev); // operation timeout and client cancellation
                }
            }

        private:
            void PassAway() override {
                if (StoragePipe) {
                    NTabletPipe::CloseClient(SelfId(), StoragePipe);
                }
                TBase::PassAway();
            }

            void Finish() {
                ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ActorContext());
            }

            void StorageUnavailable(const TString& reason) {
                Result.mutable_storage_state()->set_unavailable_reason(reason);
                Finish();
            }

            void HandleStorageTimeout() {
                if (StoragePhase == EStoragePhase::DatabaseOwners) {
                    Finish(); // keep the snapshot and pool identities when optional names cannot be resolved
                } else {
                    StorageUnavailable("Storage diagnostics timed out");
                }
            }

            void HandleConfiguration(TEvNodeWardenStorageConfig::TPtr& ev) {
                const auto& config = *ev->Get()->Config;
                const auto& bridgeInfo = ev->Get()->BridgeInfo;
                if (!config.HasSelfManagementConfig() || !config.GetSelfManagementConfig().GetEnabled()) {
                    Reply(Ydb::StatusIds::UNSUPPORTED, "Bridge operations require self-management mode to be enabled",
                          NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ActorContext());
                    return;
                }
                if (!config.HasClusterState()) {
                    Reply(Ydb::StatusIds::NOT_FOUND, "Bridge cluster state is not configured",
                          NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ActorContext());
                    return;
                }
                if (!bridgeInfo) {
                    Reply(Ydb::StatusIds::UNAVAILABLE, NoBridgeConfigError, NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                          ActorContext());
                    return;
                }
                CopyFromInternalClusterState(config.GetClusterState(), *bridgeInfo, Result);
                CopyFromInternalClusterStateDetails(config.GetClusterStateDetails(), *bridgeInfo, Result);
                if (GetProtoRequest()->include_pile_connectivity()) {
                    auto request = std::make_unique<NStorage::TEvNodeConfigInvokeOnRoot>();
                    request->Record.MutableQueryPileConnectivity();
                    Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), request.release());
                } else {
                    StartStorageQuery();
                }
            }

            void HandleConnectivity(NStorage::TEvNodeConfigInvokeOnRootResult::TPtr& ev) {
                using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
                const auto& record = ev->Get()->Record;
                if (record.GetStatus() != TResult::OK) {
                    ReplyWithInvokeError(record);
                    return;
                }
                if (!record.HasPileConnectivity()) {
                    Reply(Ydb::StatusIds::GENERIC_ERROR, "Missing pile connectivity report",
                          NKikimrIssues::TIssuesIds::DEFAULT_ERROR, ActorContext());
                    return;
                }
                const auto& report = record.GetPileConnectivity();
                auto* result = Result.mutable_pile_connectivity();
                result->set_reporter_node_id(report.GetReporterNodeId());
                result->set_reporter_is_root(report.GetHasScepter());
                result->set_binding_in_progress(report.GetBindingInProgress());
                result->set_generation(report.GetClusterStateGeneration());
                for (const auto& pile : report.GetPiles()) {
                    auto* item = result->add_piles();
                    // Names travel with the measurements: the local registry may have changed or reused a slot.
                    item->set_pile_name(pile.GetPileName());
                    item->set_static_nodes(pile.GetNumStaticNodes());
                    item->set_connected_static_nodes(pile.GetNumConnectedStaticNodes());
                    item->set_has_node_quorum(pile.GetHasNodeQuorum());
                    item->set_in_config_quorum(pile.GetRequiresConfigQuorum());
                    item->set_in_data_quorum(pile.GetRequiresDataQuorum());
                }
                StartStorageQuery();
            }

            void StartStorageQuery() {
                if (!GetProtoRequest()->has_storage_query()) {
                    return Finish();
                }
                const auto& query = GetProtoRequest()->storage_query();
                const TString databasePath(query.database_path());
                if (!databasePath.empty()) {
                    Result.mutable_storage_state()->set_database_path(CanonizePath(databasePath));
                }
                if (!IsAdministrator(AppData(), Request_->GetSerializedToken())) {
                    return StorageUnavailable("Storage diagnostics require administrator privileges");
                }
                Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup(StorageTimeoutTag));
                StorageQuery.SetView(GetStorageView(query.view()));
                StorageQuery.SetStoragePool(query.storage_pool());
                StorageQuery.SetPileName(query.pile_name());
                StorageQuery.SetNotSynced(query.not_synced());
                StorageQuery.SetPageSize(query.page_size());
                StorageQuery.SetPageToken(query.page_token());
                if (databasePath.empty()) {
                    return SendStorageQuery();
                }
                TNavigate::TEntry entry;
                entry.Path = NKikimr::SplitPath(databasePath);
                NavigateDatabaseRoots(EStoragePhase::TargetDatabase, {std::move(entry)});
            }

            void NavigateDatabaseRoots(EStoragePhase phase, TVector<TNavigate::TEntry> entries) {
                auto navigate = MakeHolder<TNavigate>();
                // Resolve global domain keys as metadata, including GSS external-subdomain descriptions.
                navigate->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
                navigate->ResultSet = std::move(entries);
                for (auto& entry : navigate->ResultSet) {
                    entry.Operation = TNavigate::OpPath;
                    entry.RedirectRequired = false;
                }
                StoragePhase = phase;
                Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.Release()));
            }

            static bool IsDatabaseRoot(const TNavigate::TEntry& entry) {
                return entry.Status == TNavigate::EStatus::Ok && entry.DomainInfo && entry.DomainDescription &&
                       (entry.Kind == TNavigate::KindSubdomain || entry.Kind == TNavigate::KindExtSubdomain ||
                        (entry.Kind == TNavigate::KindPath && entry.Path.size() == 1));
            }

            void HandleDatabaseMetadata(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
                const auto& entries = ev->Get()->Request->ResultSet;
                if (StoragePhase == EStoragePhase::DatabaseOwners) {
                    FillDatabaseOwnerPaths(entries);
                    return Finish();
                }
                if (entries.size() != 1 || !IsDatabaseRoot(entries.front())) {
                    return StorageUnavailable("Cannot resolve the selected database storage pools");
                }
                const auto& entry = entries.front();
                if (StoragePhase == EStoragePhase::SharedDatabase && entry.DomainInfo->DomainKey != SharedDatabaseId) {
                    return StorageUnavailable("Cannot resolve the selected database storage pools");
                }
                if (StoragePhase == EStoragePhase::TargetDatabase && entry.DomainInfo->IsServerless()) {
                    SharedDatabaseId = entry.DomainInfo->ResourcesDomainKey;
                    TNavigate::TEntry owner;
                    owner.TableId = SharedDatabaseId;
                    owner.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
                    NavigateDatabaseRoots(EStoragePhase::SharedDatabase, {std::move(owner)});
                    return;
                }
                StorageOwnerDatabase = CanonizePath(entry.Path);
                Result.mutable_storage_state()->set_storage_owner_database(StorageOwnerDatabase);
                StorageQuery.SetFilterPools(true);
                for (const auto& pool : entry.DomainDescription->Description.GetStoragePools()) {
                    StorageQuery.AddPoolNames(pool.GetName());
                }
                if (!StorageQuery.PoolNamesSize()) {
                    return StorageUnavailable("The selected database has no storage pools");
                }
                SendStorageQuery();
            }

            void FillDatabaseOwnerPaths(const TVector<TNavigate::TEntry>& entries) {
                for (const auto& entry : entries) {
                    const auto owner = DatabaseOwnerRows.find(entry.TableId.PathId);
                    if (owner == DatabaseOwnerRows.end() || !IsDatabaseRoot(entry) ||
                        entry.DomainInfo->DomainKey != owner->first) {
                        continue;
                    }
                    for (const auto row : owner->second) {
                        Result.mutable_storage_state()->mutable_databases(row)->set_database_path(CanonizePath(entry.Path));
                    }
                }
            }

            void SendStorageQuery() {
                NTabletPipe::TClientConfig config;
                config.RetryPolicy = {.RetryLimitCount = 3u};
                StoragePipe = Register(NTabletPipe::CreateClient(SelfId(), MakeBSControllerID(), config));
                auto query = MakeHolder<TEvBlobStorage::TEvControllerQueryBridgeStorage>();
                query->Record.CopyFrom(StorageQuery);
                StoragePhase = EStoragePhase::Controller;
                NTabletPipe::SendData(SelfId(), StoragePipe, query.Release());
            }

            void HandlePipeConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
                if (ev->Get()->Status != NKikimrProto::OK) {
                    StorageUnavailable("Storage controller is unavailable");
                }
            }

            void HandlePipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
                if (StoragePhase == EStoragePhase::Controller) {
                    StorageUnavailable("Connection to the storage controller was lost");
                }
            }

            void HandleStorageSnapshot(TEvBlobStorage::TEvControllerQueryBridgeStorageResult::TPtr& ev) {
                const auto& snapshot = ev->Get()->Record;
                auto* storage = Result.mutable_storage_state();
                storage->set_generation(snapshot.GetGeneration());
                storage->set_collected_at_us(snapshot.GetCollectedAtUs());
                storage->set_next_page_token(snapshot.GetNextPageToken());
                if (!snapshot.GetUnavailableReason().empty()) {
                    return StorageUnavailable(snapshot.GetUnavailableReason());
                }
                if (snapshot.GetGeneration() != Result.generation()) {
                    return StorageUnavailable(TStringBuilder() << "Storage generations differ: controller " << snapshot.GetGeneration()
                                                               << ", serving node " << Result.generation() << "; retry the query");
                }
                FillStorageRows(snapshot);
                if (DatabaseOwnerRows.empty()) {
                    return Finish();
                }
                TVector<TNavigate::TEntry> entries;
                entries.reserve(DatabaseOwnerRows.size());
                for (const auto& [owner, rows] : DatabaseOwnerRows) {
                    Y_UNUSED(rows);
                    auto& entry = entries.emplace_back();
                    entry.TableId = owner;
                    entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
                }
                NavigateDatabaseRoots(EStoragePhase::DatabaseOwners, std::move(entries));
            }

            void FillStorageRows(const NKikimrBridge::TStorageState& snapshot) {
                auto* storage = Result.mutable_storage_state();
                for (const auto& pile : snapshot.GetPiles()) {
                    auto* row = storage->add_piles();
                    row->set_pile_name(pile.GetPileName());
                    CopyStorageCounts(pile.GetUserGroups(), row->mutable_user_groups());
                    CopyStorageCounts(pile.GetSystemGroups(), row->mutable_system_groups());
                }
                for (const auto& database : snapshot.GetDatabases()) {
                    const int index = storage->databases_size();
                    auto* row = storage->add_databases();
                    for (const auto& name : database.GetStoragePools()) {
                        row->add_storage_pools(name);
                    }
                    row->set_pile_name(database.GetPileName());
                    row->set_mode(GetPublicStorageMode(database.GetMode()));
                    row->set_state(GetPublicStorageState(database.GetState()));
                    CopyStorageCounts(database.GetGroups(), row->mutable_groups());
                    row->set_last_error(database.GetLastError());
                    row->set_last_error_timestamp_us(database.GetLastErrorTimestampUs());
                    if (!StorageOwnerDatabase.empty()) {
                        row->set_database_path(StorageOwnerDatabase);
                    } else if (database.GetOwnerSchemeShardId() && database.GetOwnerPathId()) {
                        DatabaseOwnerRows[TPathId(database.GetOwnerSchemeShardId(), database.GetOwnerPathId())].push_back(index);
                    }
                }
                for (const auto& group : snapshot.GetGroups()) {
                    auto* row = storage->add_groups();
                    row->set_group_id(group.GetGroupId());
                    row->set_part_group_id(group.GetPartGroupId());
                    row->set_storage_pool_name(group.GetPoolName());
                    row->set_pile_name(group.GetPileName());
                    row->set_state(GetPublicStorageState(group.GetState()));
                    row->set_stage(group.GetStage());
                    row->set_last_error(group.GetLastError());
                    row->set_last_error_timestamp_us(group.GetLastErrorTimestampUs());
                }
            }
        };

    } // namespace

    void DoGetClusterState(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider&) {
        NActors::TActivationContext::AsActorContext().Register(new TGetClusterStateRequest(p.release()));
    }

} // namespace NKikimr::NGRpcService
