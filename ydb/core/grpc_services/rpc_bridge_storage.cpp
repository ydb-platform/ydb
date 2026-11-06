#include "rpc_bridge_base.h"
#include "service_bridge.h"

#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/bridge_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <optional>

namespace NKikimr::NGRpcService {
    namespace {

        using namespace NActors;
        using TStorageQuery = NKikimrBridge::TStorageQuery;
        using TPileCall = TGrpcRequestOperationCall<Ydb::Bridge::ListPileSynchronizationRequest, Ydb::Bridge::ListPileSynchronizationResponse>;
        using TDatabaseCall = TGrpcRequestOperationCall<Ydb::Bridge::ListDatabaseSynchronizationRequest, Ydb::Bridge::ListDatabaseSynchronizationResponse>;
        using TGroupCall = TGrpcRequestOperationCall<Ydb::Bridge::ListGroupSynchronizationRequest, Ydb::Bridge::ListGroupSynchronizationResponse>;

        template <class TCall>
        class TStorageRequest: public TRpcOperationRequestActor<TStorageRequest<TCall>, TCall> {
            using TBase = TRpcOperationRequestActor<TStorageRequest<TCall>, TCall>;
            using TNavigate = NSchemeCache::TSchemeCacheNavigate;
            enum class EPhase { TargetDatabase,
                                SharedDatabase,
                                Controller,
                                DatabaseOwners };

            TStorageQuery Query;
            std::optional<TString> DatabasePath;
            NKikimrBridge::TStorageState Result;
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
                if (Query.GetView() == TStorageQuery::GROUPS) {
                    if (!DatabasePath && Query.GetStoragePool().empty() && !Query.HasStoragePoolId()) {
                        return Fail(Ydb::StatusIds::BAD_REQUEST, "Group synchronization requires a database or storage pool scope");
                    }
                    if (Query.HasStoragePoolId() && !Query.GetStoragePoolId().pool_id()) {
                        return Fail(Ydb::StatusIds::BAD_REQUEST, "storage_pool_id.pool_id must not be zero");
                    }
                    for (const auto status : Query.GetStatuses()) {
                        if (status < Ydb::Bridge::SYNCED || status > Ydb::Bridge::UNKNOWN) {
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

            google::protobuf::Message* Payload() {
                switch (Query.GetView()) {
                    case TStorageQuery::PILES:
                        return Result.HasPiles() ? Result.MutablePiles() : nullptr;
                    case TStorageQuery::DATABASES:
                        return Result.HasDatabases() ? Result.MutableDatabases() : nullptr;
                    case TStorageQuery::GROUPS:
                        return Result.HasGroups() ? Result.MutableGroups() : nullptr;
                    default:
                        return nullptr;
                }
            }

            void Finish() {
                const auto* payload = Payload();
                if (!payload) {
                    return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Missing storage diagnostics result");
                }
                // The 32 MB result budget leaves ample room for the Operation/Any envelope below the 64 MB gRPC limit.
                if (payload->ByteSizeLong() > TEvBlobStorage::TEvControllerQueryBridgeStorage::MaxResultBytes) {
                    return Fail(Ydb::StatusIds::PRECONDITION_FAILED,
                                "Storage diagnostics result is too large; narrow the database/pool/pile/status filter");
                }
                this->ReplyWithResult(Ydb::StatusIds::SUCCESS, *payload, this->ActorContext());
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
                if (Phase == EPhase::DatabaseOwners) {
                    for (const auto& entry : entries) {
                        const auto owner = OwnerRows.find(entry.TableId.PathId);
                        if (owner != OwnerRows.end() && IsDatabaseRoot(entry) && entry.DomainInfo->DomainKey == owner->first) {
                            const TString path = CanonizePath(entry.Path);
                            for (const auto row : owner->second) {
                                Result.MutableDatabases()->mutable_databases(row)->set_database_path(path);
                            }
                        }
                    }
                    return Finish();
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
                Result.Swap(&ev->Get()->Record);
                if (Result.GetStatus() != Ydb::StatusIds::SUCCESS) {
                    return Fail(Result.GetStatus(), Result.GetErrorMessage());
                }
                if (!Payload()) {
                    return Fail(Ydb::StatusIds::INTERNAL_ERROR, "Missing storage diagnostics result");
                }
                if (!StorageOwnerDatabase.empty()) {
                    if (Result.HasDatabases()) {
                        auto* data = Result.MutableDatabases();
                        data->set_storage_owner_database_path(StorageOwnerDatabase);
                        for (auto& row : *data->mutable_databases()) {
                            row.set_database_path(StorageOwnerDatabase);
                        }
                    } else if (Result.HasGroups()) {
                        Result.MutableGroups()->set_storage_owner_database_path(StorageOwnerDatabase);
                    }
                } else if (Result.HasDatabases()) {
                    for (const auto& owner : Result.GetOwners()) {
                        if (owner.GetOwnerSchemeShardId() && owner.GetOwnerPathId() &&
                            owner.GetRowIndex() < static_cast<ui32>(Result.GetDatabases().databases_size())) {
                            OwnerRows[TPathId(owner.GetOwnerSchemeShardId(), owner.GetOwnerPathId())].push_back(owner.GetRowIndex());
                        }
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
        TActivationContext::AsActorContext().Register(new TStorageRequest<TPileCall>(request.release(), std::move(query)));
    }

    void DoListDatabaseSynchronization(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider&) {
        const auto* proto = TDatabaseCall::GetProtoRequest(request.get());
        TStorageQuery query;
        query.SetView(TStorageQuery::DATABASES);
        query.SetNotSynced(proto->only_incomplete());
        if (proto->has_pile_name()) {
            query.SetPileName(proto->pile_name());
        }
        std::optional<TString> database;
        if (proto->has_database_path()) {
            database = TString(proto->database_path());
        }
        TActivationContext::AsActorContext().Register(new TStorageRequest<TDatabaseCall>(request.release(), std::move(query), std::move(database)));
    }

    void DoListGroupSynchronization(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider&) {
        const auto* proto = TGroupCall::GetProtoRequest(request.get());
        TStorageQuery query;
        query.SetView(TStorageQuery::GROUPS);
        if (proto->has_storage_pool_name()) {
            query.SetStoragePool(proto->storage_pool_name());
        } else if (proto->has_storage_pool_id()) {
            query.MutableStoragePoolId()->CopyFrom(proto->storage_pool_id());
        }
        if (proto->has_pile_name()) {
            query.SetPileName(proto->pile_name());
        }
        query.MutableStatuses()->CopyFrom(proto->statuses());
        std::optional<TString> database;
        if (proto->has_database_path()) {
            database = TString(proto->database_path());
        }
        TActivationContext::AsActorContext().Register(new TStorageRequest<TGroupCall>(request.release(), std::move(query), std::move(database)));
    }

} // namespace NKikimr::NGRpcService
