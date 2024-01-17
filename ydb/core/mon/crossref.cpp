#include "crossref.h"
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NCrossRef {

using namespace NNodeWhiteboard;

class TCrossRefActor : public TActor<TCrossRefActor> {
    struct TEndpoint {
        TString Address;
        ui16 Port;
        bool Valid = false;
    };
    std::unordered_map<ui32, TEndpoint> Endpoints;

    struct TEndpointRequest {
        std::deque<ui64> PendingIds;
    };
    std::unordered_map<ui32, TEndpointRequest> EndpointRequests;

    std::unordered_map<ui64, TEvGenerateCrossRef::TPtr> RequestsInProgress;
    ui64 NextRequestId = 1;

public:
    TCrossRefActor()
        : TActor(&TThis::StateFunc)
    {}

    void Handle(TEvGenerateCrossRef::TPtr ev) {
        const ui32 nodeId = ev->Get()->TargetNodeId;
        if (const auto it = Endpoints.find(nodeId); it != Endpoints.end() && it->second.Valid) {
            IssueReply(ev, &it->second);
        } else {
            TEndpointRequest& request = EndpointRequests[nodeId];

            if (request.PendingIds.empty()) {
                Send(MakeNodeWhiteboardServiceId(nodeId), new TEvWhiteboard::TEvSystemStateRequest,
                    IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
            }

            const ui64 id = NextRequestId++;
            request.PendingIds.push_back(id);
            TActivationContext::Schedule(ev->Get()->Deadline, new IEventHandle(NActors::TEvents::TSystem::Wakeup, 0, SelfId(), TActorId(), nullptr, id));
            RequestsInProgress.emplace(id, ev);
        }
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr ev) {
        const ui32 nodeId = ev->Sender.NodeId();

        const auto& record = ev->Get()->Record;
        for (const auto& e : record.GetSystemStateInfo(0).GetEndpoints()) {
            if (e.GetName() == "http-mon") {
                const TString& address = e.GetAddress();
                const char *p = address.data();
                if (const char *x = strchr(p, ':')) {
                    TEndpoint& endpoint = Endpoints[nodeId];
                    endpoint.Port = atoi(x + 1);
                    endpoint.Address = TString(p, x);
                    if (endpoint.Address) {
                        endpoint.Valid = true;
                        ProcessEndpointRequests(nodeId, &endpoint); // fulfill pending requests
                    } else {
                        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(nodeId));
                    }
                    return;
                }
            }
        }

        ProcessEndpointRequests(nodeId, nullptr);
    }

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        if (const auto it = Endpoints.find(nodeId); it != Endpoints.end()) {
            if (const auto& info = ev->Get()->Node) {
                TEndpoint& endpoint = it->second;
                endpoint.Address = info->Host;
                endpoint.Valid = true;
                return ProcessEndpointRequests(nodeId, &endpoint); // fulfill pending requests
            }
        }
        ProcessEndpointRequests(nodeId, nullptr);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr ev) {
        ProcessEndpointRequests(ev->Sender.NodeId(), nullptr);
    }

    void Handle(TEvInterconnect::TEvNodeConnected::TPtr) {}

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        ProcessEndpointRequests(ev->Get()->NodeId, nullptr); // drop any pending requests
        Endpoints.erase(ev->Get()->NodeId); // terminate existing entries
    }

    void ProcessEndpointRequests(ui32 nodeId, const TEndpoint *endpoint) {
        if (const auto it = EndpointRequests.find(nodeId); it != EndpointRequests.end()) {
            for (ui64 id : it->second.PendingIds) {
                if (const auto it = RequestsInProgress.find(id); it != RequestsInProgress.end()) {
                    IssueReply(it->second, endpoint);
                    RequestsInProgress.erase(it);
                }
            }
            EndpointRequests.erase(it);
        }
    }

    void IssueReply(TEvGenerateCrossRef::TPtr ev, const TEndpoint *endpoint) {
        TString url = endpoint && endpoint->Valid // TODO: bastion, viewer
            ? Sprintf("http://%s:%u/%s", endpoint->Address.data(), endpoint->Port, ev->Get()->Path.data())
            : TString();
        Send(ev->Sender, new TEvCrossRef(url), 0, ev->Cookie);
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr ev) {
        if (const auto it = RequestsInProgress.find(ev->Cookie); it != RequestsInProgress.end()) {
            IssueReply(it->second, nullptr);
            RequestsInProgress.erase(it);
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvGenerateCrossRef, Handle);
        hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeInfo, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle);
    )
};

IActor *CreateCrossRefActor() {
    return new TCrossRefActor;
}

} // NKikimr::NCrossRef
