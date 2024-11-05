#include "service_discovery.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/mind/node_broker.h>
#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NKikimrNodeBroker;
using namespace NNodeBroker;

using TEvNodeRegistrationRequest = TGrpcRequestOperationCall<Ydb::Discovery::NodeRegistrationRequest,
    Ydb::Discovery::NodeRegistrationResponse>;

class TNodeRegistrationRPC : public TActorBootstrapped<TNodeRegistrationRPC> {
    using TActorBase = TActorBootstrapped<TNodeRegistrationRPC>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TNodeRegistrationRPC(IRequestOpCtx* request)
        : Request(request)
    {}

    void Bootstrap(const TActorContext& ctx) {
        auto req = dynamic_cast<TEvNodeRegistrationRequest*>(Request.get());
        Y_ABORT_UNLESS(req, "Unexpected request type for TNodeRegistrationRPC");

        if (!CheckAccess()) {
            Status = Ydb::StatusIds::UNAUTHORIZED;
            Request->RaiseIssue(NYql::TIssue("Cannot authorize node. Access denied"));
            SendReplyAndDie(ctx);
            return;
        }

        auto dinfo = AppData(ctx)->DomainsInfo;
        auto request = TEvNodeRegistrationRequest::GetProtoRequest(Request);
        const TString& domainPath = request->domain_path();
        if (domainPath && dinfo->GetDomain()->Name != domainPath) {
            auto error = Sprintf("Unknown domain %s", domainPath.data());
            ReplyWithErrorAndDie(error, ctx);
            return;
        }

        NTabletPipe::TClientConfig pipeConfig;
        pipeConfig.RetryPolicy = {.RetryLimitCount = 10};
        auto pipe = NTabletPipe::CreateClient(SelfId(), MakeNodeBrokerID(), pipeConfig);
        NodeBrokerPipe = ctx.RegisterWithSameMailbox(pipe);

        TAutoPtr<TEvNodeBroker::TEvRegistrationRequest> nodeBrokerRequest
            = new TEvNodeBroker::TEvRegistrationRequest;

        nodeBrokerRequest->Record.SetHost(request->host());
        nodeBrokerRequest->Record.SetPort(request->port());
        nodeBrokerRequest->Record.SetResolveHost(request->resolve_host());
        nodeBrokerRequest->Record.SetAddress(request->address());
        CopyNodeLocation(nodeBrokerRequest->Record.MutableLocation(), request->location());
        nodeBrokerRequest->Record.SetFixedNodeId(request->fixed_node_id());
        if (request->has_path()) {
            nodeBrokerRequest->Record.SetPath(request->path());
        }
        nodeBrokerRequest->Record.SetAuthorizedByCertificate(IsNodeAuthorizedByCertificate);

        NTabletPipe::SendData(ctx, NodeBrokerPipe, nodeBrokerRequest.Release());

        Become(&TNodeRegistrationRPC::MainState);
    }

    void Handle(TEvNodeBroker::TEvRegistrationResponse::TPtr &ev, const TActorContext &ctx) {
        auto &rec = ev->Get()->Record;

        if (rec.GetStatus().GetCode() != TStatus::OK) {
            ReplyWithErrorAndDie(rec.GetStatus().GetReason(), ctx);
            return;
        }

        auto request = TEvNodeRegistrationRequest::GetProtoRequest(Request);
        Result.set_node_id(rec.GetNode().GetNodeId());
        if (rec.GetNode().HasName()) {
            Result.set_node_name(rec.GetNode().GetName());
        }
        Result.set_expire(rec.GetNode().GetExpire());
        Result.set_domain_path(request->domain_path());
        CopyNodeInfo(Result.add_nodes(), rec.GetNode());

        if (rec.HasScopeTabletId()) {
            Result.set_scope_tablet_id(rec.GetScopeTabletId());
        }
        if (rec.HasScopePathId()) {
            Result.set_scope_path_id(rec.GetScopePathId());
        }

        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        auto config = AppData()->DynamicNameserviceConfig;

        for (const auto &node : ev->Get()->Nodes) {
            // Copy static nodes only.
            if (!config || node.NodeId <= config->MaxStaticNodeId) {
                auto &info = *Result.add_nodes();
                info.set_node_id(node.NodeId);
                info.set_host(node.Host);
                info.set_address(node.Address);
                info.set_resolve_host(node.ResolveHost);
                info.set_port(node.Port);
                NActorsInterconnect::TNodeLocation location;
                node.Location.Serialize(&location, true);
                CopyNodeLocation(info.mutable_location(), location);
            }
        }

        Status = Ydb::StatusIds::SUCCESS;
        SendReplyAndDie(ctx);
    }

    void Undelivered(const TActorContext &ctx) {
        ReplyWithErrorAndDie("Node Broker is unavailable", ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) noexcept
    {
        if (ev->Get()->Status != NKikimrProto::OK)
            Undelivered(ctx);
    }

    void Die(const TActorContext &ctx)
    {
        if (NodeBrokerPipe) {
            NTabletPipe::CloseClient(ctx, NodeBrokerPipe);
        }
        TActorBase::Die(ctx);
    }

    void SendReplyAndDie(const TActorContext &ctx)
    {
        Request->SendResult(Result, Status);
        Die(ctx);
    }

    void ReplyWithErrorAndDie(const TString &error, const TActorContext &ctx)
    {
        auto issue = NYql::TIssue(error);
        Request->RaiseIssue(issue);
        Status = Ydb::StatusIds::GENERIC_ERROR;
        SendReplyAndDie(ctx);
    }

    STFUNC(MainState) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            HFunc(TEvNodeBroker::TEvRegistrationResponse, Handle);
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            CFunc(TEvTabletPipe::EvClientDestroyed, Undelivered);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        }
    }

private:
    bool CheckAccess() {
        const auto& serializedToken = Request->GetSerializedToken();
        // Empty serializedToken means token is not required. Checked in secure_request.h
        if (!serializedToken.empty() && !AppData()->RegisterDynamicNodeAllowedSIDs.empty()) {
            NACLib::TUserToken token(serializedToken);
            for (const auto& sid : AppData()->RegisterDynamicNodeAllowedSIDs) {
                if (token.IsExist(sid)) {
                    IsNodeAuthorizedByCertificate = true;
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    static void CopyNodeInfo(Ydb::Discovery::NodeInfo* dst, const NKikimrNodeBroker::TNodeInfo& src) {
        dst->set_node_id(src.GetNodeId());
        dst->set_host(src.GetHost());
        dst->set_port(src.GetPort());
        dst->set_resolve_host(src.GetResolveHost());
        dst->set_address(src.GetAddress());
        CopyNodeLocation(dst->mutable_location(), src.GetLocation());
        dst->set_expire(src.GetExpire());
    }

    static void CopyNodeLocation(NActorsInterconnect::TNodeLocation* dst, const Ydb::Discovery::NodeLocation& src) {
        if (src.has_data_center_num()) {
            dst->SetDataCenterNum(src.data_center_num());
        }
        if (src.has_room_num()) {
            dst->SetRoomNum(src.room_num());
        }
        if (src.has_rack_num()) {
            dst->SetRackNum(src.rack_num());
        }
        if (src.has_body_num()) {
            dst->SetBodyNum(src.body_num());
        }
        if (src.has_body()) {
            dst->SetBody(src.body());
        }
        if (src.has_data_center()) {
            dst->SetDataCenter(src.data_center());
        }
        if (src.has_module()) {
            dst->SetModule(src.module());
        }
        if (src.has_rack()) {
            dst->SetRack(src.rack());
        }
        if (src.has_unit()) {
            dst->SetUnit(src.unit());
        }
    }

    static void CopyNodeLocation(Ydb::Discovery::NodeLocation* dst, const NActorsInterconnect::TNodeLocation& src) {
        if (src.HasDataCenterNum()) {
            dst->set_data_center_num(src.GetDataCenterNum());
        }
        if (src.HasRoomNum()) {
            dst->set_room_num(src.GetRoomNum());
        }
        if (src.HasRackNum()) {
            dst->set_rack_num(src.GetRackNum());
        }
        if (src.HasBodyNum()) {
            dst->set_body_num(src.GetBodyNum());
        }
        if (src.HasBody()) {
            dst->set_body(src.GetBody());
        }
        if (src.HasDataCenter()) {
            dst->set_data_center(src.GetDataCenter());
        }
        if (src.HasModule()) {
            dst->set_module(src.GetModule());
        }
        if (src.HasRack()) {
            dst->set_rack(src.GetRack());
        }
        if (src.HasUnit()) {
            dst->set_unit(src.GetUnit());
        }
    }

    std::unique_ptr<IRequestOpCtx> Request;
    Ydb::Discovery::NodeRegistrationResult Result;
    Ydb::StatusIds_StatusCode Status = Ydb::StatusIds::SUCCESS;
    TActorId NodeBrokerPipe;
    bool IsNodeAuthorizedByCertificate = false;
};

void DoNodeRegistrationRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TNodeRegistrationRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
