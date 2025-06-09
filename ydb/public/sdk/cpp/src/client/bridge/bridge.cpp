#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/bridge/bridge.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request/make.h>

#include <ydb/public/api/grpc/draft/ydb_bridge_v1.grpc.pb.h>

namespace NYdb::inline Dev::NBridge {

namespace {

void StateToProto(const TClusterState& state, Ydb::Bridge::ClusterState* proto) {
    proto->set_generation(state.Generation);
    proto->set_primary_pile(state.PrimaryPile);
    proto->set_promoted_pile(state.PromotedPile);
    for (const auto& s : state.PerPileState) {
        proto->add_per_pile_state(static_cast<Ydb::Bridge::ClusterState_PileState>(s));
    }
}

TClusterState StateFromProto(const Ydb::Bridge::ClusterState& proto) {
    TClusterState state;
    state.Generation = proto.generation();
    state.PrimaryPile = proto.primary_pile();
    state.PromotedPile = proto.promoted_pile();
    for (const auto& s : proto.per_pile_state()) {
        state.PerPileState.push_back(static_cast<TClusterState::EPileState>(s));
    }
    return state;
}

}

class TBridgeClient::TImpl : public TClientImplCommon<TBridgeClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl> connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    TAsyncStatus SwitchClusterState(const TClusterState& state, const TSwitchClusterStateSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::SwitchClusterStateRequest>(settings);
        StateToProto(state, request.mutable_cluster_state());

        return RunSimple<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::SwitchClusterStateRequest, Ydb::Bridge::SwitchClusterStateResponse>(
            std::move(request),
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncSwitchClusterState,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncGetClusterStateResult GetClusterState(const TGetClusterStateSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::GetClusterStateRequest>(settings);

        auto promise = NThreading::NewPromise<TGetClusterStateResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                TClusterState state;
                if (any) {
                    Ydb::Bridge::GetClusterStateResult result;
                    if (any->UnpackTo(&result)) {
                        state = StateFromProto(result.cluster_state());
                    }
                }
                promise.SetValue(TGetClusterStateResult(TStatus(std::move(status)), std::move(state)));
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
};

TBridgeClient::TBridgeClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TBridgeClient::~TBridgeClient() = default;

TAsyncStatus TBridgeClient::SwitchClusterState(const TClusterState& state, const TSwitchClusterStateSettings& settings) {
    return Impl_->SwitchClusterState(state, settings);
}

TAsyncGetClusterStateResult TBridgeClient::GetClusterState(const TGetClusterStateSettings& settings) {
    return Impl_->GetClusterState(settings);
}

}
