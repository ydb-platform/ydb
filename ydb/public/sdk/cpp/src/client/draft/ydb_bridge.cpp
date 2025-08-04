#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request/make.h>

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

        return RunSimple<Ydb::Bridge::V1::BridgeService, Ydb::Bridge::UpdateClusterStateRequest, Ydb::Bridge::UpdateClusterStateResponse>(
            std::move(request),
            &Ydb::Bridge::V1::BridgeService::Stub::AsyncUpdateClusterState,
            TRpcRequestSettings::Make(settings));
    }

    TAsyncGetClusterStateResult GetClusterState(const TGetClusterStateSettings& settings) {
        auto request = MakeOperationRequest<Ydb::Bridge::GetClusterStateRequest>(settings);

        auto promise = NThreading::NewPromise<TGetClusterStateResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::vector<TPileStateUpdate> state;
                if (any) {
                    Ydb::Bridge::GetClusterStateResult result;
                    if (any->UnpackTo(&result)) {
                        state = StateFromProto(result);
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

TAsyncStatus TBridgeClient::UpdateClusterState(const std::vector<TPileStateUpdate>& updates,
        const std::vector<std::string>& quorumPiles, const TUpdateClusterStateSettings& settings) {
    return Impl_->UpdateClusterState(updates, quorumPiles, settings);
}

TAsyncGetClusterStateResult TBridgeClient::GetClusterState(const TGetClusterStateSettings& settings) {
    return Impl_->GetClusterState(settings);
}

}
