#include "grpc_request_proxy.h"
#include "rpc_calls.h"
#include "rpc_common/rpc_common.h"

#include <ydb/library/aclib/aclib.h>
#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <util/string/vector.h>
#include <util/generic/hash.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvGetShardLocationsRequest = TGrpcRequestOperationCall<Ydb::ClickhouseInternal::GetShardLocationsRequest,
    Ydb::ClickhouseInternal::GetShardLocationsResponse>;

class TGetShardLocationsRPC : public TActorBootstrapped<TGetShardLocationsRPC> {
    using TBase = TActorBootstrapped<TGetShardLocationsRPC>;

private:
    constexpr static ui64 INVALID_TABLET_ID = Max<ui64>();
    constexpr static ui64 DEFAULT_TIMEOUT_MSEC = 100;

    struct TNodeInfo {
        TString Host;
        ui16 Port;
    };

    std::unique_ptr<IRequestOpCtx> Request;
    Ydb::ClickhouseInternal::GetShardLocationsResult Result;

    THashMap<ui64, TActorId> ShardPipes;
    THashMap<ui64, ui32> ShardNodes;
    THashMap<ui32, TNodeInfo> NodeInfos;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    explicit TGetShardLocationsRPC(std::unique_ptr<IRequestOpCtx>&& request)
        : TBase()
        , Request(std::move(request))
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TString errorMessage;
        if (!CheckAccess(errorMessage)) {
            return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, errorMessage, ctx);
        }

        TDuration timeout = TDuration::MilliSeconds(DEFAULT_TIMEOUT_MSEC);
        auto proto = TEvGetShardLocationsRequest::GetProtoRequest(Request);
        if (proto->operation_params().has_operation_timeout())
            timeout = GetDuration(proto->operation_params().operation_timeout());
        ctx.Schedule(timeout, new TEvents::TEvWakeup());
        ResolveShards(ctx);
    }

    void Die(const NActors::TActorContext& ctx) override {
        // Destroy all pipe clients
        for (const auto& p : ShardPipes) {
            ctx.Send(p.second, new TEvents::TEvPoisonPill());
        }
        TBase::Die(ctx);
    }

private:
    STFUNC(StateWaitResolve) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default:
                break;
        }
    }

    void ResolveShards(const NActors::TActorContext& ctx) {
        // Create pipes to all shards
        auto proto = TEvGetShardLocationsRequest::GetProtoRequest(Request);
        for (ui64 ti : proto->tablet_ids()) {
            if (ti == 0)
                ti = INVALID_TABLET_ID;

            if (ShardPipes.contains(ti))
                continue;

            NTabletPipe::TClientConfig clientConfig;
            clientConfig.AllowFollower = false;
            clientConfig.CheckAliveness = false;
            clientConfig.RetryPolicy = {
                .RetryLimitCount = 2,
                .MinRetryTime = TDuration::MilliSeconds(5),
            };
            ShardPipes[ti] = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, ti, clientConfig));
        }

        // Get list of cluster nodes
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());

        TBase::Become(&TThis::StateWaitResolve);
    }

    void HandleTimeout(const TActorContext& ctx) {
        return ReplyWithError(Ydb::StatusIds::TIMEOUT, "Request timed out", ctx);
    }

    bool CheckAccess(TString& errorMessage) {
        if (Request->GetSerializedToken().empty())
            return true;

        NACLib::TUserToken userToken(Request->GetSerializedToken());
        // TODO: check describe rights for root?

        Y_UNUSED(errorMessage);
        return true;
    }


    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected* msg = ev->Get();
        const ui64 tabletId = msg->TabletId;
        Y_ABORT_UNLESS(tabletId != 0);
        if (msg->Status != NKikimrProto::OK) {
            ShardNodes[tabletId] = -1;
        } else {
            ShardNodes[tabletId] =  msg->ServerId.NodeId();
        }

        return CheckFinished(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        const ui64 tabletId = ev->Get()->TabletId;
        Y_ABORT_UNLESS(tabletId != 0);
        ShardNodes[tabletId] = -1;

        return CheckFinished(ctx);
    }

    void CheckFinished(const TActorContext& ctx) {
        if (ShardNodes.size() == ShardPipes.size() && !NodeInfos.empty())
            ReplySuccess(ctx);
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        const TEvInterconnect::TEvNodesInfo* nodesInfo = ev->Get();
        Y_ABORT_UNLESS(!nodesInfo->Nodes.empty());
        for (const auto& ni : nodesInfo->Nodes) {
            NodeInfos[ni.NodeId].Host = ni.Host;
            NodeInfos[ni.NodeId].Port = ni.Port;
        }

        CheckFinished(ctx);
    }

    void ReplySuccess(const NActors::TActorContext& ctx) {
        for (const auto& sn : ShardNodes) {
            auto* info = Result.add_tablets();
            info->set_tablet_id(sn.first);
            info->set_host(NodeInfos[sn.second].Host);
            info->set_port(NodeInfos[sn.second].Port);
        }
        ReplyWithResult(Ydb::StatusIds::SUCCESS, Result, ctx);
    }

    void ReplyWithError(StatusIds::StatusCode status, const TString& message, const TActorContext& ctx) {
        Request->RaiseIssue(NYql::TIssue(message));
        Request->ReplyWithYdbStatus(status);
        Die(ctx);
    }

    void ReplyWithResult(StatusIds::StatusCode status,
                         const Ydb::ClickhouseInternal::GetShardLocationsResult& result,
                         const TActorContext& ctx) {
        Request->SendResult(result, status);
        Die(ctx);
    }
};

void DoGetShardLocationsRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TGetShardLocationsRPC(std::move(p)));
}

} // namespace NKikimr
} // namespace NGRpcService
