#include "rpc_bridge_base.h"
#include "service_bridge.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/bridge_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <google/protobuf/timestamp.pb.h>

#include <optional>
#include <type_traits>

namespace NKikimr::NGRpcService {
    namespace {

        using namespace NActors;
        using TStorageQuery = NKikimrBridge::TStorageQuery;
        using TStorageState = NKikimrBridge::TStorageState;
        using TPileCall = TGrpcRequestOperationCall<Ydb::Bridge::ListPileSynchronizationRequest, Ydb::Bridge::ListPileSynchronizationResponse>;
        using TDatabaseCall = TGrpcRequestOperationCall<Ydb::Bridge::ListDatabaseSynchronizationRequest, Ydb::Bridge::ListDatabaseSynchronizationResponse>;
        using TGroupCall = TGrpcRequestOperationCall<Ydb::Bridge::ListGroupSynchronizationRequest, Ydb::Bridge::ListGroupSynchronizationResponse>;
        using TPileResult = Ydb::Bridge::ListPileSynchronizationResult;
        using TDatabaseResult = Ydb::Bridge::ListDatabaseSynchronizationResult;
        using TGroupResult = Ydb::Bridge::ListGroupSynchronizationResult;

        // The RPC adapter converts enum values in both directions.
        static_assert(static_cast<int>(NKikimrBridge::STORAGE_MODE_UNSPECIFIED) == Ydb::Bridge::STORAGE_MODE_UNSPECIFIED);
        static_assert(static_cast<int>(NKikimrBridge::BRIDGED) == Ydb::Bridge::BRIDGED);
        static_assert(static_cast<int>(NKikimrBridge::ORDINARY) == Ydb::Bridge::ORDINARY);
        static_assert(static_cast<int>(NKikimrBridge::MIXED) == Ydb::Bridge::MIXED);
        static_assert(static_cast<int>(NKikimrBridge::SYNCHRONIZATION_STATUS_UNSPECIFIED) == Ydb::Bridge::SYNCHRONIZATION_STATUS_UNSPECIFIED);
        static_assert(static_cast<int>(NKikimrBridge::SYNCED) == Ydb::Bridge::SYNCED);
        static_assert(static_cast<int>(NKikimrBridge::SYNCING) == Ydb::Bridge::SYNCING);
        static_assert(static_cast<int>(NKikimrBridge::PENDING) == Ydb::Bridge::PENDING);
        static_assert(static_cast<int>(NKikimrBridge::NOT_REQUIRED) == Ydb::Bridge::NOT_REQUIRED);
        static_assert(static_cast<int>(NKikimrBridge::UNKNOWN) == Ydb::Bridge::UNKNOWN);

        void MovePool(NKikimrBridge::TStoragePool& from, Ydb::Bridge::StoragePool& to) {
            if (from.has_id()) {
                auto* id = to.mutable_id();
                id->set_box_id(from.id().box_id());
                id->set_pool_id(from.id().pool_id());
            }
            to.set_name(std::move(*from.mutable_name()));
        }

        void MoveError(NKikimrBridge::TSynchronizationError& from, Ydb::Bridge::SynchronizationError& to) {
            to.set_message(std::move(*from.mutable_message()));
            if (from.has_occurred_at()) {
                to.mutable_occurred_at()->Swap(from.mutable_occurred_at());
            }
        }

        template <class TPrivate, class TPublic>
        void MoveObservation(TPrivate& from, TPublic& to) {
            to.set_cluster_state_generation(from.cluster_state_generation());
            if (from.has_observed_at()) {
                to.mutable_observed_at()->Swap(from.mutable_observed_at());
            }
        }

        bool MoveResult(TStorageState& response, TPileResult& to) {
            if (!response.HasPiles()) {
                return false;
            }
            auto& from = *response.MutablePiles();
            MoveObservation(from, to);
            to.set_unattributed_storage_pools(from.unattributed_storage_pools());
            to.mutable_piles()->Reserve(from.piles_size());
            for (auto& pile : *from.mutable_piles()) {
                auto* row = to.add_piles();
                if (pile.has_pile_name()) {
                    row->set_pile_name(std::move(*pile.mutable_pile_name()));
                }
                if (pile.has_databases()) {
                    const auto& counts = pile.databases();
                    auto* databases = row->mutable_databases();
                    databases->set_total(counts.total());
                    databases->set_synced(counts.synced());
                    databases->set_syncing(counts.syncing());
                    databases->set_pending(counts.pending());
                    databases->set_unknown(counts.unknown());
                    databases->set_not_required(counts.not_required());
                }
            }
            return true;
        }

        bool MoveResult(TStorageState& response, TDatabaseResult& to) {
            if (!response.HasDatabases()) {
                return false;
            }
            auto& from = *response.MutableDatabases();
            MoveObservation(from, to);
            to.mutable_databases()->Reserve(from.databases_size());
            for (auto& database : *from.mutable_databases()) {
                auto* row = to.add_databases();
                row->mutable_storage_pools()->Reserve(database.storage_pools_size());
                for (auto& pool : *database.mutable_storage_pools()) {
                    MovePool(pool, *row->add_storage_pools());
                }
                row->set_storage_mode(static_cast<Ydb::Bridge::StorageMode>(database.storage_mode()));
                if (database.has_pile_name()) {
                    row->set_pile_name(std::move(*database.mutable_pile_name()));
                }
                if (database.has_groups()) {
                    const auto& counts = database.groups();
                    auto* groups = row->mutable_groups();
                    groups->set_required(counts.required());
                    groups->set_synced(counts.synced());
                    groups->set_syncing(counts.syncing());
                    groups->set_pending(counts.pending());
                    groups->set_unknown(counts.unknown());
                }
                if (database.has_last_sync_error()) {
                    MoveError(*database.mutable_last_sync_error(), *row->mutable_last_sync_error());
                }
            }
            return true;
        }

        bool MoveResult(TStorageState& response, TGroupResult& to) {
            if (!response.HasGroups()) {
                return false;
            }
            auto& from = *response.MutableGroups();
            MoveObservation(from, to);
            to.mutable_groups()->Reserve(from.groups_size());
            for (auto& group : *from.mutable_groups()) {
                auto* row = to.add_groups();
                row->set_group_id(group.group_id());
                if (group.has_physical_group_id()) {
                    row->set_physical_group_id(group.physical_group_id());
                }
                if (group.has_storage_pool()) {
                    MovePool(*group.mutable_storage_pool(), *row->mutable_storage_pool());
                }
                if (group.has_pile_name()) {
                    row->set_pile_name(std::move(*group.mutable_pile_name()));
                }
                row->set_sync_status(static_cast<Ydb::Bridge::SynchronizationStatus>(group.sync_status()));
                row->set_sync_phase(std::move(*group.mutable_sync_phase()));
                if (group.has_last_sync_error()) {
                    MoveError(*group.mutable_last_sync_error(), *row->mutable_last_sync_error());
                }
            }
            return true;
        }

        template <class TCall, class TResult>
        class TStorageRequest: public TRpcOperationRequestActor<TStorageRequest<TCall, TResult>, TCall> {
            using TBase = TRpcOperationRequestActor<TStorageRequest<TCall, TResult>, TCall>;
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            enum class EPhase { TargetDatabase,
                                SharedDatabase,
                                Controller,
                                DatabaseOwners };

            TStorageQuery Query;
            std::optional<TString> DatabasePath;
            TResult Result;
            TActorId StoragePipe;
            EPhase Phase = EPhase::TargetDatabase;
            TString StorageOwnerDatabase;
            TPathId SharedDatabaseId;
            THashMap<TPathId, TVector<int>> OwnerRows;
            static constexpr ui64 TimeoutTag = 0x425249444745;

        public:
            TStorageRequest(IRequestOpCtx* request, TStorageQuery query, std::optional<TString> databasePath = std::nullopt)
                : TBase(request)
                , Query(std::move(query))
                , DatabasePath(std::move(databasePath))
            {
            }

            void Bootstrap() {
                TBase::Bootstrap(this->ActorContext());
                if (!IsAdministrator(AppData(), this->Request_->GetSerializedToken())) {
                    return Fail(Ydb::StatusIds::UNAUTHORIZED, "Storage diagnostics require administrator privileges");
                }
                if (DatabasePath && (!DatabasePath->StartsWith('/') || NKikimr::SplitPath(*DatabasePath).empty())) {
                    return Fail(Ydb::StatusIds::BAD_REQUEST, "database_path must be an absolute database path");
                }
                if (Query.HasStorageMode() && (Query.GetView() != TStorageQuery::DATABASES ||
                        Query.GetStorageMode() < NKikimrBridge::BRIDGED || Query.GetStorageMode() > NKikimrBridge::MIXED)) {
                    return Fail(Ydb::StatusIds::BAD_REQUEST, "storage_mode requires database synchronization and must be BRIDGED, ORDINARY, or MIXED");
                }
                if (Query.GetView() == TStorageQuery::GROUPS) {
                    if (Query.HasStoragePool() && Query.GetStoragePool().empty()) {
                        return Fail(Ydb::StatusIds::BAD_REQUEST, "Storage pool name must not be empty");
                    }
                    for (const auto& pool : Query.GetStoragePoolIds()) {
                        if (!pool.pool_id()) {
                            return Fail(Ydb::StatusIds::BAD_REQUEST, "storage_pool_id.pool_id must not be zero");
                        }
                    }
                    for (const auto status : Query.GetStatuses()) {
                        if (status < NKikimrBridge::SYNCED || status > NKikimrBridge::UNKNOWN) {
                            return Fail(Ydb::StatusIds::BAD_REQUEST, "Unsupported synchronization status");
                        }
                    }
                }
                this->Become(&TStorageRequest::StateFunc);
                this->Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup(TimeoutTag));
                if (DatabasePath) {
                    TNavigate::TEntry entry;
                    entry.Path = NKikimr::SplitPath(*DatabasePath);
                    Navigate(EPhase::TargetDatabase, {std::move(entry)});
                } else {
                    SendStorageQuery();
                }
            }

            STFUNC(StateFunc) {
                switch (ev->GetTypeRewrite()) {
                    hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleMetadata);
                    hFunc(TEvBlobStorage::TEvControllerQueryBridgeStorageResult, HandleResult);
                    hFunc(TEvTabletPipe::TEvClientConnected, HandleConnected);
                    hFunc(TEvTabletPipe::TEvClientDestroyed, HandleDestroyed);
                    case TEvents::TSystem::Wakeup:
                        if (ev->Get<TEvents::TEvWakeup>()->Tag == TimeoutTag) {
                            if (Phase == EPhase::DatabaseOwners) {
                                Finish(); // owner names are optional; the full storage result remains usable
                            } else {
                                Fail(Ydb::StatusIds::UNAVAILABLE, "Storage diagnostics timed out");
                            }
                        } else {
                            TBase::StateFuncBase(ev);
                        }
                        break;
                    default:
                        return TBase::StateFuncBase(ev);
                }
            }

        private:
            void PassAway() override {
                if (StoragePipe) {
                    NTabletPipe::CloseClient(this->SelfId(), StoragePipe);
                }
                TBase::PassAway();
            }

            void Finish() {
                // The 32 MB result budget leaves ample room for the Operation/Any envelope below the 64 MB gRPC limit.
                if (Result.ByteSizeLong() > TEvBlobStorage::TEvControllerQueryBridgeStorage::MaxResultBytes) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED,
                                "Storage diagnostics result is too large; narrow the database/pool/pile/status filter");
                }
                this->ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, this->ActorContext());
            }

            void Fail(Ydb::StatusIds::StatusCode status, const TString& reason) {
                this->Reply(status, reason, NKikimrIssues::TIssuesIds::DEFAULT_ERROR, this->ActorContext());
            }

            void Navigate(EPhase phase, TVector<TNavigate::TEntry> entries) {
                auto navigate = MakeHolder<TNavigate>();
                // Keep global domain keys and GSS external-subdomain descriptions rather than redirecting to TSS.
                navigate->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
                navigate->ResultSet = std::move(entries);
                for (auto& entry : navigate->ResultSet) {
                    entry.Operation = TNavigate::OpPath;
                    entry.RedirectRequired = false;
                }
                Phase = phase;
                this->Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.Release()));
            }

            static bool IsDatabaseRoot(const TNavigate::TEntry& entry) {
                return entry.Status == TNavigate::EStatus::Ok && entry.DomainInfo && entry.DomainDescription &&
                       (entry.Kind == TNavigate::KindSubdomain || entry.Kind == TNavigate::KindExtSubdomain ||
                        (entry.Kind == TNavigate::KindPath && entry.Path.size() == 1));
            }

            void HandleMetadata(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
                const auto& entries = ev->Get()->Request->ResultSet;
                if constexpr (std::is_same_v<TResult, TDatabaseResult>) {
                    if (Phase == EPhase::DatabaseOwners) {
                        for (const auto& entry : entries) {
                            const auto owner = OwnerRows.find(entry.TableId.PathId);
                            if (owner != OwnerRows.end() && IsDatabaseRoot(entry) && entry.DomainInfo->DomainKey == owner->first) {
                                const TString path = CanonizePath(entry.Path);
                                for (const auto row : owner->second) {
                                    Result.mutable_databases(row)->set_database_path(path);
                                }
                            }
                        }
                        return Finish();
                    }
                }
                if (entries.size() != 1) {
                    return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Invalid database metadata response");
                }
                const auto& entry = entries.front();
                if (entry.Status == TNavigate::EStatus::RootUnknown || entry.Status == TNavigate::EStatus::PathErrorUnknown) {
                    return Fail(Ydb::StatusIds::NOT_FOUND, "Selected database was not found");
                }
                if (entry.Status != TNavigate::EStatus::Ok || !entry.DomainInfo || !entry.DomainDescription) {
                    return Fail(Ydb::StatusIds::UNAVAILABLE, "Cannot resolve the selected database storage pools");
                }
                if (!IsDatabaseRoot(entry)) {
                    return Fail(Ydb::StatusIds::BAD_REQUEST, "The selected path must be a database root");
                }
                if (Phase == EPhase::SharedDatabase && entry.DomainInfo->DomainKey != SharedDatabaseId) {
                    return Fail(Ydb::StatusIds::UNAVAILABLE, "Cannot resolve the selected database storage pools");
                }
                if (Phase == EPhase::TargetDatabase && entry.DomainInfo->IsServerless()) {
                    SharedDatabaseId = entry.DomainInfo->ResourcesDomainKey;
                    TNavigate::TEntry owner;
                    owner.TableId = SharedDatabaseId;
                    owner.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
                    Navigate(EPhase::SharedDatabase, {std::move(owner)});
                    return;
                }
                StorageOwnerDatabase = CanonizePath(entry.Path);
                Query.SetFilterPools(true);
                for (const auto& pool : entry.DomainDescription->Description.GetStoragePools()) {
                    Query.AddPoolNames(pool.GetName());
                }
                if (!Query.PoolNamesSize()) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED, "The selected database has no storage pools");
                }
                SendStorageQuery();
            }

            void SendStorageQuery() {
                NTabletPipe::TClientConfig config;
                config.RetryPolicy = {.RetryLimitCount = 3u};
                StoragePipe = this->Register(NTabletPipe::CreateClient(this->SelfId(), MakeBSControllerID(), config));
                auto query = MakeHolder<TEvBlobStorage::TEvControllerQueryBridgeStorage>();
                query->Record.CopyFrom(Query);
                Phase = EPhase::Controller;
                NTabletPipe::SendData(this->SelfId(), StoragePipe, query.Release());
            }

            void HandleConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
                if (ev->Get()->Status != NKikimrProto::OK) {
                    Fail(Ydb::StatusIds::UNAVAILABLE, "Storage controller is unavailable");
                }
            }

            void HandleDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
                if (Phase == EPhase::Controller) {
                    Fail(Ydb::StatusIds::UNAVAILABLE, "Connection to the storage controller was lost");
                }
            }

            void HandleResult(TEvBlobStorage::TEvControllerQueryBridgeStorageResult::TPtr& ev) {
                if (Phase != EPhase::Controller) {
                    return;
                }
                auto& response = ev->Get()->Record;
                if (response.GetStatus() != Ydb::StatusIds::SUCCESS) {
                    return Fail(response.GetStatus(), response.GetErrorMessage());
                }
                if (!MoveResult(response, Result)) {
                    return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Missing storage diagnostics result");
                }
                if constexpr (std::is_same_v<TResult, TDatabaseResult>) {
                    if (!StorageOwnerDatabase.empty()) {
                        Result.set_storage_owner_database_path(StorageOwnerDatabase);
                        for (auto& row : *Result.mutable_databases()) {
                            row.set_database_path(StorageOwnerDatabase);
                        }
                    } else {
                        for (const auto& owner : response.GetOwners()) {
                            if (owner.GetOwnerSchemeShardId() && owner.GetOwnerPathId() &&
                                owner.GetRowIndex() < static_cast<ui32>(Result.databases_size())) {
                                OwnerRows[TPathId(owner.GetOwnerSchemeShardId(), owner.GetOwnerPathId())].push_back(owner.GetRowIndex());
                            }
                        }
                    }
                } else if constexpr (std::is_same_v<TResult, TGroupResult>) {
                    if (!StorageOwnerDatabase.empty()) {
                        Result.set_storage_owner_database_path(StorageOwnerDatabase);
                    }
                }
                if (OwnerRows.empty()) {
                    return Finish();
                }
                TVector<TNavigate::TEntry> entries;
                entries.reserve(OwnerRows.size());
                for (const auto& [owner, rows] : OwnerRows) {
                    Y_UNUSED(rows);
                    auto& entry = entries.emplace_back();
                    entry.TableId = owner;
                    entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
                }
                Navigate(EPhase::DatabaseOwners, std::move(entries));
            }
        };

    } // namespace

    void DoListPileSynchronization(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider&) {
        TStorageQuery query;
        query.SetView(TStorageQuery::PILES);
        TActivationContext::AsActorContext().Register(new TStorageRequest<TPileCall, TPileResult>(request.release(), std::move(query)));
    }

    void DoListDatabaseSynchronization(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider&) {
        const auto* proto = TDatabaseCall::GetProtoRequest(request.get());
        TStorageQuery query;
        query.SetView(TStorageQuery::DATABASES);
        query.SetNotSynced(proto->only_incomplete());
        if (proto->has_storage_mode()) {
            query.SetStorageMode(static_cast<NKikimrBridge::EStorageMode>(proto->storage_mode()));
        }
        if (proto->has_pile_name()) {
            query.SetPileName(proto->pile_name());
        }
        std::optional<TString> database;
        if (proto->has_database_path()) {
            database = TString(proto->database_path());
        }
        TActivationContext::AsActorContext().Register(new TStorageRequest<TDatabaseCall, TDatabaseResult>(
            request.release(), std::move(query), std::move(database)));
    }

    void DoListGroupSynchronization(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider&) {
        const auto* proto = TGroupCall::GetProtoRequest(request.get());
        TStorageQuery query;
        query.SetView(TStorageQuery::GROUPS);
        if (proto->has_storage_pool_name()) {
            query.SetStoragePool(proto->storage_pool_name());
        } else if (proto->has_storage_pool_id()) {
            auto* id = query.AddStoragePoolIds();
            id->set_box_id(proto->storage_pool_id().box_id());
            id->set_pool_id(proto->storage_pool_id().pool_id());
        }
        if (proto->has_pile_name()) {
            query.SetPileName(proto->pile_name());
        }
        for (const auto status : proto->statuses()) {
            query.AddStatuses(static_cast<NKikimrBridge::ESynchronizationStatus>(status));
        }
        std::optional<TString> database;
        if (proto->has_database_path()) {
            database = TString(proto->database_path());
        }
        TActivationContext::AsActorContext().Register(new TStorageRequest<TGroupCall, TGroupResult>(
            request.release(), std::move(query), std::move(database)));
    }

} // namespace NKikimr::NGRpcService
