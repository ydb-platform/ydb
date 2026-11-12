#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>
#include <ydb/public/api/protos/draft/ydb_bridge.pb.h>

namespace NYdb::inline Dev::NBridge {

namespace {

void UpdatesToProto(const std::vector<TPileStateUpdate>& updates, Ydb::Bridge::UpdateClusterStateRequest* proto) {
    for (const auto& update : updates) {
        auto* u = proto->add_updates();
        u->set_pile_name(update.PileName);
        u->set_state(static_cast<Ydb::Bridge::PileState::State>(update.State));
    }
}

std::vector<TPileStateUpdate> StateFromProto(const Ydb::Bridge::GetClusterStateResult& proto) {
    std::vector<TPileStateUpdate> state;
    state.reserve(proto.pile_states_size());
    for (const auto& s : proto.pile_states()) {
        state.push_back({
            .PileName = s.pile_name(),
            .State = static_cast<EPileState>(s.state()),
        });
    }
    return state;
}

TGroupSyncCounts GroupSyncCountsFromProto(const Ydb::Bridge::GroupSyncCounts& proto) {
    return {
        .Required = proto.required(),
        .Synced = proto.synced(),
        .Syncing = proto.syncing(),
        .Pending = proto.pending(),
        .Unknown = proto.unknown(),
    };
}

TDatabaseSyncCounts DatabaseSyncCountsFromProto(const Ydb::Bridge::DatabaseSyncCounts& proto) {
    return {
        .Total = proto.total(),
        .Synced = proto.synced(),
        .Syncing = proto.syncing(),
        .Pending = proto.pending(),
        .Unknown = proto.unknown(),
        .NotRequired = proto.not_required(),
    };
}

ESynchronizationStatus SynchronizationStatusFromProto(Ydb::Bridge::SynchronizationStatus status) {
    switch (status) {
        case Ydb::Bridge::SYNCHRONIZATION_STATUS_UNSPECIFIED:
            return ESynchronizationStatus::Unspecified;
        case Ydb::Bridge::SYNCED:
            return ESynchronizationStatus::Synced;
        case Ydb::Bridge::SYNCING:
            return ESynchronizationStatus::Syncing;
        case Ydb::Bridge::PENDING:
            return ESynchronizationStatus::Pending;
        case Ydb::Bridge::NOT_REQUIRED:
            return ESynchronizationStatus::NotRequired;
        default:
            return ESynchronizationStatus::Unknown;
    }
}

EStorageMode StorageModeFromProto(Ydb::Bridge::StorageMode mode) {
    switch (mode) {
        case Ydb::Bridge::BRIDGED:
            return EStorageMode::Bridged;
        case Ydb::Bridge::ORDINARY:
            return EStorageMode::Ordinary;
        case Ydb::Bridge::MIXED:
            return EStorageMode::Mixed;
        default:
            return EStorageMode::Unspecified;
    }
}

TStoragePool StoragePoolFromProto(const Ydb::Bridge::StoragePool& proto) {
    return {.Id = {.BoxId = proto.id().box_id(), .PoolId = proto.id().pool_id()}, .Name = proto.name()};
}

TSynchronizationError SynchronizationErrorFromProto(const Ydb::Bridge::SynchronizationError& proto) {
    return {
        .Message = proto.message(),
        .OccurredAt = proto.has_occurred_at() ? std::optional<TInstant>(ProtoTimestampToInstant(proto.occurred_at())) : std::nullopt,
    };
}

TPileSynchronization PileSynchronizationFromProto(const Ydb::Bridge::PileSynchronization& proto) {
    return {
        .PileName = proto.has_pile_name() ? std::optional<std::string>(proto.pile_name()) : std::nullopt,
        .Databases = DatabaseSyncCountsFromProto(proto.databases()),
    };
}

TDatabaseSynchronization DatabaseSynchronizationFromProto(const Ydb::Bridge::DatabaseSynchronization& proto) {
    TDatabaseSynchronization result;
    if (proto.has_database_path()) {
        result.DatabasePath = proto.database_path();
    }
    for (const auto& pool : proto.storage_pools()) {
        result.StoragePools.push_back(StoragePoolFromProto(pool));
    }
    result.StorageMode = StorageModeFromProto(proto.storage_mode());
    if (proto.has_pile_name()) {
        result.PileName = proto.pile_name();
    }
    result.Groups = GroupSyncCountsFromProto(proto.groups());
    if (proto.has_last_sync_error()) {
        result.LastSyncError = SynchronizationErrorFromProto(proto.last_sync_error());
    }
    return result;
}

TGroupSynchronization GroupSynchronizationFromProto(const Ydb::Bridge::GroupSynchronization& proto) {
    return {
        .GroupId = proto.group_id(),
        .PhysicalGroupId = proto.has_physical_group_id() ? std::optional<uint32_t>(proto.physical_group_id()) : std::nullopt,
        .StoragePool = proto.has_storage_pool() ? std::optional<TStoragePool>(StoragePoolFromProto(proto.storage_pool())) : std::nullopt,
        .PileName = proto.has_pile_name() ? std::optional<std::string>(proto.pile_name()) : std::nullopt,
        .SyncStatus = SynchronizationStatusFromProto(proto.sync_status()),
        .SyncPhase = proto.sync_phase(),
        .LastSyncError = proto.has_last_sync_error() ? std::optional<TSynchronizationError>(SynchronizationErrorFromProto(proto.last_sync_error())) : std::nullopt,
    };
}

template <class TProto>
bool UnpackDiagnostics(google::protobuf::Any* any, TPlainStatus& status, TProto& proto) {
    if (!status.Ok()) {
        return false;
    }
    if (!any || !any->UnpackTo(&proto)) {
        status.Status = EStatus::CLIENT_INTERNAL_ERROR;
        status.Issues.AddIssue(NIssue::TIssue("Invalid bridge diagnostics response"));
        return false;
    }
    return true;
}

} // namespace

class TBridgeClient::TImpl: public TClientImplCommon<TBridgeClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {
    }

    TAsyncStatus UpdateClusterState(const std::vector<TPileStateUpdate>& updates,
                                    const std::vector<std::string>& quorumPiles, const TUpdateClusterStateSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::UpdateClusterStateRequest>(settings);
        UpdatesToProto(updates, &request);
        for (const auto& quorumPile : quorumPiles) {
            request.add_quorum_piles(quorumPile);
        }
        if (settings.ExpectedGeneration_) {
            request.set_expected_generation(*settings.ExpectedGeneration_);
        }

        return RunSimple<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::UpdateClusterStateRequest, Ydb::Bridge::UpdateClusterStateResponse>(
            std::move(request),
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncUpdateClusterState,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncGetClusterStateResult GetClusterState(const TGetClusterStateSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::GetClusterStateRequest>(settings);
        request.set_include_pile_connectivity(settings.IncludePileConnectivity_);
        request.set_include_unbridged_status(settings.IncludeUnbridgedStatus_);

        auto promise = NThreading::NewPromise<TGetClusterStateResult>();

        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            std::vector<TPileStateUpdate> state;
            uint64_t generation = 0;
            bool conversionInProgress = false;
            std::optional<TPendingPileOperation> pendingPileOperation;
            std::optional<TPileConnectivityReport> pileConnectivity;
            std::optional<TUnbridgedStatus> unbridgedStatus;
            if (any) {
                Ydb::Bridge::GetClusterStateResult result;
                if (any->UnpackTo(&result)) {
                    if (result.has_pile_connectivity()) {
                        const auto& report = result.pile_connectivity();
                        pileConnectivity.emplace(TPileConnectivityReport{
                            .ReporterNodeId = report.reporter_node_id(),
                            .ReporterIsRoot = report.reporter_is_root(),
                            .BindingInProgress = report.binding_in_progress(),
                            .Generation = report.generation(),
                        });
                        pileConnectivity->Piles.reserve(report.piles_size());
                        for (const auto& pile : report.piles()) {
                            pileConnectivity->Piles.push_back({
                                .PileName = pile.pile_name(),
                                .StaticNodes = pile.static_nodes(),
                                .ConnectedStaticNodes = pile.connected_static_nodes(),
                                .HasNodeQuorum = pile.has_node_quorum(),
                                .InConfigQuorum = pile.in_config_quorum(),
                                .InDataQuorum = pile.in_data_quorum(),
                            });
                        }
                    }
                    if (result.has_unbridged_status()) {
                        const auto& report = result.unbridged_status();
                        auto& parsed = unbridgedStatus.emplace();
                        parsed.BridgeConfigNodeIds.assign(report.bridge_config_node_ids().begin(), report.bridge_config_node_ids().end());
                        parsed.UnreportedStaticNodeIds.assign(report.unreported_static_node_ids().begin(), report.unreported_static_node_ids().end());
                    }
                    state = StateFromProto(result);
                    generation = result.generation();
                    conversionInProgress = result.conversion_in_progress();
                    if (result.has_pending_pile_operation()) {
                        const auto& op = result.pending_pile_operation();
                        pendingPileOperation.emplace(TPendingPileOperation{
                            .PileName = op.pile_name(),
                            .Kind = static_cast<TPendingPileOperation::EKind>(op.kind()),
                            .ErrorReason = op.error_reason(),
                        });
                    }
                }
            }
            promise.SetValue(TGetClusterStateResult(TStatus(std::move(status)), std::move(state), generation,
                                                    conversionInProgress, std::move(pendingPileOperation), std::move(pileConnectivity),
                                                    std::move(unbridgedStatus)));
        };

        Connections_->RunDeferred<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::GetClusterStateRequest, Ydb::Bridge::GetClusterStateResponse>(
            std::move(request),
            extractor,
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncGetClusterState,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncListPileSynchronizationResult ListPileSynchronization(const TListPileSynchronizationSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::ListPileSynchronizationRequest>(settings);
        auto promise = NThreading::NewPromise<TListPileSynchronizationResult>();
        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Bridge::ListPileSynchronizationResult proto;
            if (!UnpackDiagnostics(any, status, proto)) {
                promise.SetValue(TListPileSynchronizationResult(TStatus(std::move(status))));
                return;
            }
            std::vector<TPileSynchronization> piles;
            piles.reserve(proto.piles_size());
            for (const auto& row : proto.piles()) {
                piles.push_back(PileSynchronizationFromProto(row));
            }
            promise.SetValue(TListPileSynchronizationResult(TStatus(std::move(status)), std::move(piles),
                                                          proto.cluster_state_generation(), ProtoTimestampToInstant(proto.observed_at()),
                                                          proto.unattributed_storage_pools()));
        };
        Connections_->RunDeferred<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::ListPileSynchronizationRequest, Ydb::Bridge::ListPileSynchronizationResponse>(
            std::move(request), extractor, &Ydb::Bridge::V1::BridgeService::Stub::AsyncListPileSynchronization,
            DbDriverState_, INITIAL_DEFERRED_CALL_DELAY, TRpcRequestSettings::Make(settings));
        return promise.GetFuture();
    }

    TAsyncListDatabaseSynchronizationResult ListDatabaseSynchronization(const TListDatabaseSynchronizationSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::ListDatabaseSynchronizationRequest>(settings);
        if (settings.DatabasePath_) {
            request.set_database_path(*settings.DatabasePath_);
        }
        if (settings.PileName_) {
            request.set_pile_name(*settings.PileName_);
        }
        request.set_only_incomplete(settings.OnlyIncomplete_);
        if (settings.StorageMode_) {
            request.set_storage_mode(static_cast<Ydb::Bridge::StorageMode>(*settings.StorageMode_));
        }
        auto promise = NThreading::NewPromise<TListDatabaseSynchronizationResult>();
        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Bridge::ListDatabaseSynchronizationResult proto;
            if (!UnpackDiagnostics(any, status, proto)) {
                promise.SetValue(TListDatabaseSynchronizationResult(TStatus(std::move(status))));
                return;
            }
            std::vector<TDatabaseSynchronization> databases;
            databases.reserve(proto.databases_size());
            for (const auto& row : proto.databases()) {
                databases.push_back(DatabaseSynchronizationFromProto(row));
            }
            promise.SetValue(TListDatabaseSynchronizationResult(TStatus(std::move(status)), std::move(databases),
                                                                proto.cluster_state_generation(), ProtoTimestampToInstant(proto.observed_at()),
                                                                proto.has_storage_owner_database_path() ? std::optional<std::string>(proto.storage_owner_database_path()) : std::nullopt));
        };
        Connections_->RunDeferred<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::ListDatabaseSynchronizationRequest, Ydb::Bridge::ListDatabaseSynchronizationResponse>(
            std::move(request), extractor, &Ydb::Bridge::V1::BridgeService::Stub::AsyncListDatabaseSynchronization,
            DbDriverState_, INITIAL_DEFERRED_CALL_DELAY, TRpcRequestSettings::Make(settings));
        return promise.GetFuture();
    }

    TAsyncListGroupSynchronizationResult ListGroupSynchronization(const TGroupSynchronizationScope& scope,
                                                                  const TListGroupSynchronizationSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::ListGroupSynchronizationRequest>(settings);
        switch (scope.GetKind()) {
            case TGroupSynchronizationScope::EKind::All:
                break;
            case TGroupSynchronizationScope::EKind::Database:
                request.set_database_path(scope.GetName());
                break;
            case TGroupSynchronizationScope::EKind::StoragePoolName:
                request.set_storage_pool_name(scope.GetName());
                break;
            case TGroupSynchronizationScope::EKind::StoragePoolId:
                request.mutable_storage_pool_id()->set_box_id(scope.GetPoolId().BoxId);
                request.mutable_storage_pool_id()->set_pool_id(scope.GetPoolId().PoolId);
                break;
        }
        if (settings.PileName_) {
            request.set_pile_name(*settings.PileName_);
        }
        for (const auto status : settings.Statuses_) {
            request.add_statuses(static_cast<Ydb::Bridge::SynchronizationStatus>(status));
        }
        auto promise = NThreading::NewPromise<TListGroupSynchronizationResult>();
        auto extractor = [promise](google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Bridge::ListGroupSynchronizationResult proto;
            if (!UnpackDiagnostics(any, status, proto)) {
                promise.SetValue(TListGroupSynchronizationResult(TStatus(std::move(status))));
                return;
            }
            std::vector<TGroupSynchronization> groups;
            groups.reserve(proto.groups_size());
            for (const auto& row : proto.groups()) {
                groups.push_back(GroupSynchronizationFromProto(row));
            }
            promise.SetValue(TListGroupSynchronizationResult(TStatus(std::move(status)), std::move(groups),
                                                             proto.cluster_state_generation(), ProtoTimestampToInstant(proto.observed_at()),
                                                             proto.has_storage_owner_database_path() ? std::optional<std::string>(proto.storage_owner_database_path()) : std::nullopt));
        };
        Connections_->RunDeferred<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::ListGroupSynchronizationRequest, Ydb::Bridge::ListGroupSynchronizationResponse>(
            std::move(request), extractor, &Ydb::Bridge::V1::BridgeService::Stub::AsyncListGroupSynchronization,
            DbDriverState_, INITIAL_DEFERRED_CALL_DELAY, TRpcRequestSettings::Make(settings));
        return promise.GetFuture();
    }

    TAsyncStatus ConvertToBridge(const TConvertToBridgeSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::ConvertToBridgeRequest>(settings);
        request.set_skip_storage_pool_conversion(settings.SkipStoragePoolConversion_);

        return RunSimple<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::ConvertToBridgeRequest, Ydb::Bridge::ConvertToBridgeResponse>(
            std::move(request),
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncConvertToBridge,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus AddPile(const std::string& pileName, const TAddPileSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::AddPileRequest>(settings);
        request.set_pile_name(pileName);

        return RunSimple<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::AddPileRequest, Ydb::Bridge::AddPileResponse>(
            std::move(request),
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncAddPile,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus SetDatabaseBridgeMode(const std::string& databasePath, bool bridgeMode, const TSetDatabaseBridgeModeSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::SetDatabaseBridgeModeRequest>(settings);
        request.set_database_path(databasePath);
        request.set_bridge_mode(bridgeMode);
        request.set_retain_pile_name(settings.RetainPileName_);

        return RunSimple<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::SetDatabaseBridgeModeRequest, Ydb::Bridge::SetDatabaseBridgeModeResponse>(
            std::move(request),
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncSetDatabaseBridgeMode,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncStatus RemovePile(const std::string& pileName, const TRemovePileSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::RemovePileRequest>(settings);
        request.set_pile_name(pileName);

        return RunSimple<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::RemovePileRequest, Ydb::Bridge::RemovePileResponse>(
            std::move(request),
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncRemovePile,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncGetRollbackStatusResult GetRollbackStatus(const TGetRollbackStatusSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::GetRollbackStatusRequest>(settings);

        auto promise = NThreading::NewPromise<TGetRollbackStatusResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                bool bridged = false;
                std::vector<uint32_t> bridgeConfigNodeIds;
                std::vector<uint32_t> unreportedStaticNodeIds;
                bool complete = false;
                bool rollbackInProgress = false;
                std::string errorReason;
                if (any) {
                    Ydb::Bridge::GetRollbackStatusResult result;
                    if (any->UnpackTo(&result)) {
                        bridged = result.bridged();
                        bridgeConfigNodeIds.assign(result.bridge_config_node_ids().begin(),
                            result.bridge_config_node_ids().end());
                        unreportedStaticNodeIds.assign(result.unreported_static_node_ids().begin(),
                            result.unreported_static_node_ids().end());
                        complete = result.complete();
                        rollbackInProgress = result.rollback_in_progress();
                        errorReason = result.error_reason();
                    }
                }
                promise.SetValue(TGetRollbackStatusResult(TStatus(std::move(status)), bridged,
                    std::move(bridgeConfigNodeIds), std::move(unreportedStaticNodeIds), complete, rollbackInProgress,
                    std::move(errorReason)));
            };

        Connections_->RunDeferred<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::GetRollbackStatusRequest, Ydb::Bridge::GetRollbackStatusResponse>(
            std::move(request),
            extractor,
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncGetRollbackStatus,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncStatus Rollback(const TRollbackSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::RollbackRequest>(settings);
        request.set_skip_bsc_validation(settings.SkipBscValidation_);

        return RunSimple<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::RollbackRequest, Ydb::Bridge::RollbackResponse>(
            std::move(request),
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncRollback,
            TRpcRequestSettings::Make(settings));
    }
};

TBridgeClient::TBridgeClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{
}

TBridgeClient::~TBridgeClient() = default;

TAsyncStatus TBridgeClient::UpdateClusterState(const std::vector<TPileStateUpdate>& updates,
                                               const std::vector<std::string>& quorumPiles, const TUpdateClusterStateSettings& settings) {
    return Impl_->UpdateClusterState(updates, quorumPiles, settings);
}

TAsyncGetClusterStateResult TBridgeClient::GetClusterState(const TGetClusterStateSettings& settings) {
    return Impl_->GetClusterState(settings);
}

TAsyncListPileSynchronizationResult TBridgeClient::ListPileSynchronization(const TListPileSynchronizationSettings& settings) {
    return Impl_->ListPileSynchronization(settings);
}

TAsyncListDatabaseSynchronizationResult TBridgeClient::ListDatabaseSynchronization(const TListDatabaseSynchronizationSettings& settings) {
    return Impl_->ListDatabaseSynchronization(settings);
}

TAsyncListGroupSynchronizationResult TBridgeClient::ListGroupSynchronization(const TGroupSynchronizationScope& scope,
                                                                             const TListGroupSynchronizationSettings& settings) {
    return Impl_->ListGroupSynchronization(scope, settings);
}

TAsyncStatus TBridgeClient::ConvertToBridge(const TConvertToBridgeSettings& settings) {
    return Impl_->ConvertToBridge(settings);
}

TAsyncStatus TBridgeClient::AddPile(const std::string& pileName, const TAddPileSettings& settings) {
    return Impl_->AddPile(pileName, settings);
}

TAsyncStatus TBridgeClient::SetDatabaseBridgeMode(const std::string& databasePath, bool bridgeMode,
                                                  const TSetDatabaseBridgeModeSettings& settings) {
    return Impl_->SetDatabaseBridgeMode(databasePath, bridgeMode, settings);
}

TAsyncStatus TBridgeClient::RemovePile(const std::string& pileName, const TRemovePileSettings& settings) {
    return Impl_->RemovePile(pileName, settings);
}

TAsyncGetRollbackStatusResult TBridgeClient::GetRollbackStatus(const TGetRollbackStatusSettings& settings) {
    return Impl_->GetRollbackStatus(settings);
}

TAsyncStatus TBridgeClient::Rollback(const TRollbackSettings& settings) {
    return Impl_->Rollback(settings);
}

} // namespace NYdb::inline Dev::NBridge
