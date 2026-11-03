#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/make_request/make.h>

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

void StorageQueryToProto(const TStorageQuery& query, Ydb::Bridge::StorageQuery* proto) {
    proto->set_view(static_cast<Ydb::Bridge::StorageQuery::View>(query.View));
    proto->set_database_path(query.DatabasePath);
    proto->set_storage_pool(query.StoragePool);
    proto->set_pile_name(query.PileName);
    proto->set_not_synced(query.NotSynced);
    proto->set_page_size(query.PageSize);
    proto->set_page_token(query.PageToken);
}

TGroupSyncCounts GroupSyncCountsFromProto(const Ydb::Bridge::GroupSyncCounts& proto) {
    return {
        .Total = proto.total(),
        .Synced = proto.synced(),
        .Running = proto.running(),
        .Pending = proto.pending(),
        .Unknown = proto.unknown(),
    };
}

ECopySyncState CopySyncStateFromProto(Ydb::Bridge::GroupStorageState::State state) {
    switch (state) {
        case Ydb::Bridge::GroupStorageState::STATE_UNSPECIFIED: return ECopySyncState::Unspecified;
        case Ydb::Bridge::GroupStorageState::SYNCED: return ECopySyncState::Synced;
        case Ydb::Bridge::GroupStorageState::SYNCING: return ECopySyncState::Syncing;
        case Ydb::Bridge::GroupStorageState::PENDING: return ECopySyncState::Pending;
        case Ydb::Bridge::GroupStorageState::NOT_REQUIRED: return ECopySyncState::NotRequired;
        default: return ECopySyncState::Unknown;
    }
}

EStorageMode StorageModeFromProto(Ydb::Bridge::DatabaseStorageState::Mode mode) {
    switch (mode) {
        case Ydb::Bridge::DatabaseStorageState::BRIDGED: return EStorageMode::Bridged;
        case Ydb::Bridge::DatabaseStorageState::ORDINARY: return EStorageMode::Ordinary;
        case Ydb::Bridge::DatabaseStorageState::MIXED: return EStorageMode::Mixed;
        default: return EStorageMode::Unspecified;
    }
}

TStorageState StorageStateFromProto(const Ydb::Bridge::StorageState& proto) {
    TStorageState state;
    state.Generation = proto.generation();
    state.CollectedAtUs = proto.collected_at_us();
    state.DatabasePath = proto.database_path();
    state.StorageOwnerDatabase = proto.storage_owner_database();
    state.NextPageToken = proto.next_page_token();
    state.UnavailableReason = proto.unavailable_reason();
    state.Piles.reserve(proto.piles_size());
    for (const auto& pile : proto.piles()) {
        state.Piles.push_back({
            .PileName = pile.pile_name(),
            .UserGroups = GroupSyncCountsFromProto(pile.user_groups()),
            .SystemGroups = GroupSyncCountsFromProto(pile.system_groups()),
        });
    }
    state.Databases.reserve(proto.databases_size());
    for (const auto& database : proto.databases()) {
        state.Databases.push_back({
            .DatabasePath = database.database_path(),
            .StoragePools = {database.storage_pools().begin(), database.storage_pools().end()},
            .Mode = StorageModeFromProto(database.mode()),
            .PileName = database.pile_name(),
            .Groups = GroupSyncCountsFromProto(database.groups()),
            .State = CopySyncStateFromProto(database.state()),
            .LastError = database.last_error(),
            .LastErrorTimestampUs = database.last_error_timestamp_us(),
        });
    }
    state.Groups.reserve(proto.groups_size());
    for (const auto& group : proto.groups()) {
        state.Groups.push_back({
            .GroupId = group.group_id(),
            .PartGroupId = group.part_group_id(),
            .StoragePoolName = group.storage_pool_name(),
            .PileName = group.pile_name(),
            .State = CopySyncStateFromProto(group.state()),
            .Stage = group.stage(),
            .LastError = group.last_error(),
            .LastErrorTimestampUs = group.last_error_timestamp_us(),
        });
    }
    return state;
}

}

class TBridgeClient::TImpl : public TClientImplCommon<TBridgeClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

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
        if (settings.StorageQuery_) {
            StorageQueryToProto(*settings.StorageQuery_, request.mutable_storage_query());
        }

        auto promise = NThreading::NewPromise<TGetClusterStateResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::vector<TPileStateUpdate> state;
                uint64_t generation = 0;
                bool conversionInProgress = false;
                std::optional<TPendingPileOperation> pendingPileOperation;
                std::optional<TPileConnectivityReport> pileConnectivity;
                std::optional<TStorageState> storageState;
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
                        if (result.has_storage_state()) {
                            storageState = StorageStateFromProto(result.storage_state());
                        }
                    }
                }
                promise.SetValue(TGetClusterStateResult(TStatus(std::move(status)), std::move(state), generation,
                    conversionInProgress, std::move(pendingPileOperation), std::move(pileConnectivity), std::move(storageState)));
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
};

TBridgeClient::TBridgeClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TBridgeClient::~TBridgeClient() = default;

TAsyncStatus TBridgeClient::UpdateClusterState(const std::vector<TPileStateUpdate>& updates,
        const std::vector<std::string>& quorumPiles, const TUpdateClusterStateSettings& settings) {
    return Impl_->UpdateClusterState(updates, quorumPiles, settings);
}

TAsyncGetClusterStateResult TBridgeClient::GetClusterState(const TGetClusterStateSettings& settings) {
    return Impl_->GetClusterState(settings);
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

}
