#include "msgbus_servicereq.h"
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr {
namespace NMsgBusProxy {

class TMessageBusResolveNode : public TMessageBusLocalServiceRequest<TMessageBusResolveNode, NKikimrServices::TActivity::MSGBUS_COMMON> {
    using TBase = TMessageBusLocalServiceRequest<TMessageBusResolveNode, NKikimrServices::TActivity::MSGBUS_COMMON>;
    NKikimrClient::TResolveNodeRequest ResolveRequest;

public:
    TMessageBusResolveNode(TBusMessageContext& msg)
        : TBase(msg, TDuration::Seconds(600))
        , ResolveRequest(static_cast<TBusResolveNode*>(msg.GetMessage())->Record)
    {}

    static TActorId MakeServiceID(const TActorContext&) {
        return GetNameserviceActorId();
    }

    TEvInterconnect::TEvListNodes* MakeReq(const TActorContext&) {
        return new TEvInterconnect::TEvListNodes();
    }

    void HandleTimeout(const TActorContext& ctx) {
        TBase::SendReplyAndDie(new TBusResponseStatus(MSTATUS_TIMEOUT), ctx);
    }

    static bool CheckName(const TString& request, const TString& name) {
        return name.StartsWith(request) && (name.size() == request.size() || (name.size() > request.size() && name[request.size()] == '.'));
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        const TEvInterconnect::TEvNodesInfo& nodesInfo = *ev->Get();
        THolder<TBusResponse> response(new TBusResponse());
        TVector<TVector<TEvInterconnect::TNodeInfo>::const_iterator> items;
        if (ResolveRequest.GetResolveLocalNode() || ResolveRequest.GetHost() == ".") {
            ui32 localNodeId = SelfId().NodeId();
            for (auto it = nodesInfo.Nodes.begin(); it != nodesInfo.Nodes.end(); ++it) {
                if (it->NodeId == localNodeId) {
                    items.push_back(it);
                    break;
                }
            }
        } else {
            for (auto it = nodesInfo.Nodes.begin(); it != nodesInfo.Nodes.end(); ++it) {
                if (ResolveRequest.HasHost()) {
                    const TString& host(ResolveRequest.GetHost());
                    if (CheckName(host, it->Host) || CheckName(host, it->ResolveHost)) {
                        items.push_back(it);
                        continue;
                    }
                }
                if (ResolveRequest.HasNodeId()) {
                    if (it->NodeId == ResolveRequest.GetNodeId()) {
                        items.push_back(it);
                        continue;
                    }
                }
            }
        }
        if (items.size() != 1) {
            response->Record.SetStatus(MSTATUS_ERROR);
            if (items.empty()) {
                response->Record.SetErrorReason("Could not resolve the node");
            } else {
                response->Record.SetErrorReason("More than one node resolved");
            }
        } else {
            response->Record.SetStatus(MSTATUS_OK);
            response->Record.MutableResolveNodeResponse()->SetHost(items.front()->Host);
            response->Record.MutableResolveNodeResponse()->SetNodeId(items.front()->NodeId);
        }

        TBase::SendReplyAndDie(response.Release(), ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

IActor* CreateMessageBusResolveNode(TBusMessageContext &msg) {
    return new TMessageBusResolveNode(msg);
}

}
}
