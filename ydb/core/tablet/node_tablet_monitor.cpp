
#include "node_tablet_monitor.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/core/mon/mon.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/appdata.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/core/interconnect.h>
#include <util/generic/algorithm.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/tablet.h>

namespace NKikimr {
namespace NNodeTabletMonitor {

using namespace NNodeWhiteboard;

class TNodeList : public TActorBootstrapped<TNodeList> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_FORWARDING_ACTOR;
    }

    TNodeList(const TActorId &sender)
        : Sender(sender)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        ctx.Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
        Become(&TThis::StateRequestedBrowse);
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        Nodes = ev->Get()->Nodes;
        if (!Nodes.empty()) {
            RenderResponse(ctx);
        } else {
            NoData(ctx);
        }
    }

    void RenderResponse(const TActorContext &ctx) {
        Sort(Nodes.begin(), Nodes.end());
        TStringStream str;
        HTML(str) {
            TAG(TH3) {
                str << "Nodes";
            }
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                      TABLEH() {str << "NodeId";}
                      TABLEH() {str << "Host";}
                      TABLEH() {str << "Address";}
                      TABLEH() {str << "Port";}
                    }
                }
                TABLEBODY() {
                for (const auto& ni : Nodes) {
                    const TEvInterconnect::TNodeInfo &nodeInfo = ni;
                    TABLER() {
                        TABLED() {str << "<a href=\"nodetabmon?action=browse_tablets&node_id=" << nodeInfo.NodeId << "\">"
                                << nodeInfo.NodeId << "</a>";}
                        TABLED() {str << "<a href=\"nodetabmon?action=browse_tablets&node_id=" << nodeInfo.NodeId << "\">"
                                << nodeInfo.Host << "</a>";}
                        TABLED() {str << nodeInfo.Address;}
                        TABLED() {str << nodeInfo.Port;}
                    }
                }
                }
            }
            HTML_TAG() {str << "<a href=\"nodetabmon?action=browse_tablets\">All tablets of the cluster</a>";}
        }
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(str.Str()));
        Die(ctx);
    }

    void Notify(const TActorContext &ctx, const TString& html) {
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(html));
    }

    void NoData(const TActorContext &ctx) {
        Notify(ctx, "No data to display");
        Die(ctx);
    }

    void Timeout(const TActorContext &ctx) {
        Notify(ctx, "Timeout");
        Die(ctx);
    }

protected:
    TActorId Sender;
    TVector<TEvInterconnect::TNodeInfo> Nodes;
};

class TTabletList : public TActorBootstrapped<TTabletList> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_FORWARDING_ACTOR;
    }

    TTabletList(const TActorId &sender, ui32 filterNodeId,
                const TIntrusivePtr<ITabletStateClassifier>& stateClassifier,
                const TIntrusivePtr<ITabletListRenderer>& renderer)
        : Sender(sender)
        , NodesRequested(0)
        , NodesReceived(0)
        , FilterNodeId(filterNodeId)
        , StateClassifier(stateClassifier)
        , Renderer(renderer)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        ctx.Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
        Become(&TThis::StateRequestedBrowse);
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    STFUNC(StateRequestedTabletInfo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        Nodes = ev->Get()->Nodes;
        if (!Nodes.empty()) {
            if (FilterNodeId) {
                TActorId tabletStateActorId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(FilterNodeId);
                ctx.Send(tabletStateActorId, new TEvWhiteboard::TEvTabletStateRequest(), IEventHandle::FlagTrackDelivery, FilterNodeId);
                ++NodesRequested;
            } else {
                for (const auto& ni : Nodes) {
                    TActorId tabletStateActorId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(ni.NodeId);
                    ctx.Send(tabletStateActorId, new TEvWhiteboard::TEvTabletStateRequest(), IEventHandle::FlagTrackDelivery, ni.NodeId);
                    ++NodesRequested;
                }
            }
            Become(&TThis::StateRequestedTabletInfo);
        } else {
            NoData(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        PerNodeTabletInfo[nodeId]; // nullptr
        NodeTabletInfoReceived(ctx);
    }

    void Handle(TEvWhiteboard::TEvTabletStateResponse::TPtr &ev, const TActorContext &ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        PerNodeTabletInfo[nodeId] = ev->Release();
        NodeTabletInfoReceived(ctx);
    }

    void NodeTabletInfoReceived(const TActorContext &ctx) {
        ++NodesReceived;
        if (NodesReceived >= NodesRequested) {
            RenderResponse(ctx);
        }
    }

    void BuildTabletList(std::function<bool(const NKikimrWhiteboard::TTabletStateInfo&)> filter,
                        TVector<TTabletListElement>& tabletsToRender) {
        TVector<ui64> tabletIdIndex;

        tabletsToRender.clear();
        for (const auto& ni : PerNodeTabletInfo) {
            if (FilterNodeId != 0 && FilterNodeId != ni.first)
                continue;
            auto eq_it = EqualRange(Nodes.begin(), Nodes.end(), ni.first);
            if (eq_it.first != Nodes.end() && ni.second) {
                for (const auto& ti : ni.second->Record.GetTabletStateInfo()) {
                    if (filter(ti)) {
                        tabletIdIndex.push_back(ti.GetTabletId());
                    }
                }
            }
        }

        Sort(tabletIdIndex);

        for (const auto& ni : PerNodeTabletInfo) {
            if (FilterNodeId != 0 && FilterNodeId != ni.first)
                continue;
            auto eq_it = EqualRange(Nodes.begin(), Nodes.end(), ni.first);
            if (eq_it.first != Nodes.end() && ni.second) {
                const TEvInterconnect::TNodeInfo& nodeInfo = *eq_it.first;
                for (const auto& ti : ni.second->Record.GetTabletStateInfo()) {
                    if (filter(ti)) {
                        ui64 index = EqualRange(tabletIdIndex.begin(), tabletIdIndex.end(), ti.GetTabletId()).first - tabletIdIndex.begin();
                        tabletsToRender.push_back({ &nodeInfo, index, &ti });
                    }
                }
            }
        }
    }

    void RenderResponse(const TActorContext &ctx) {
        Sort(Nodes.begin(), Nodes.end());
        TString filterNodeHost;
        if (FilterNodeId != 0) {
            auto eq_it = EqualRange(Nodes.begin(), Nodes.end(), FilterNodeId);
            if (eq_it.first != Nodes.end()) {
                filterNodeHost = eq_it.first->Host;
            }
        }
        TStringStream str;
        HTML(str) {
            Renderer->RenderPageHeader(str);
            for(ui32 cls = 0; cls < StateClassifier->GetMaxTabletStateClass(); cls++) {
                auto filter = StateClassifier->GetTabletStateClassFilter(cls);
                TVector<TTabletListElement> tablets;
                BuildTabletList(filter, tablets);
                auto listName = StateClassifier->GetTabletStateClassName(cls);
                Renderer->RenderTabletList(str, listName, tablets, {FilterNodeId, filterNodeHost});
            }
            Renderer->RenderPageFooter(str);
        }
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(str.Str()));
        Die(ctx);
    }

    void Notify(const TActorContext &ctx, const TString& html) {
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(html));
    }

    void NoData(const TActorContext &ctx) {
        Notify(ctx, "No data to display");
        Die(ctx);
    }

    void Timeout(const TActorContext &ctx) {
        Notify(ctx, "Timeout");
        Die(ctx);
    }

protected:
    TActorId Sender;
    TVector<TEvInterconnect::TNodeInfo> Nodes;
    TMap<ui64, TAutoPtr<TEvWhiteboard::TEvTabletStateResponse>> PerNodeTabletInfo;
    size_t NodesRequested;
    size_t NodesReceived;
    ui32 FilterNodeId;
    TIntrusivePtr<ITabletStateClassifier> StateClassifier;
    TIntrusivePtr<ITabletListRenderer> Renderer;

};

class TStateStorageTabletList : public TActorBootstrapped<TStateStorageTabletList> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_FORWARDING_ACTOR;
    }

    TStateStorageTabletList(const TActorId &sender)
        : Sender(sender)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        ctx.Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
        Become(&TThis::StateRequestedBrowse);
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, Handle);
            HFunc(TEvStateStorage::TEvResponseReplicasDumps, Handle);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx) {
        Nodes = ev->Get()->Nodes;
        const TActorId proxyActorID = MakeStateStorageProxyID();
        ctx.Send(proxyActorID, new TEvStateStorage::TEvRequestReplicasDumps());
    }

    void Handle(TEvStateStorage::TEvResponseReplicasDumps::TPtr &ev, const TActorContext &ctx) {
        Sort(Nodes.begin(), Nodes.end());
        TEvStateStorage::TEvResponseReplicasDumps &event = *ev->Get();
        TMap<ui64, TVector<std::pair<ui32, const NKikimrStateStorage::TEvInfo*>>> indexByTabletId;
        for (const auto& rdi : event.ReplicasDumps) {
            const TEvStateStorage::TEvReplicaDump& replicaDump = *rdi.second;
            for (const NKikimrStateStorage::TEvInfo& ei : replicaDump.Record.GetInfo()) {
                indexByTabletId[ei.GetTabletID()].push_back(std::make_pair(rdi.first.NodeId(), &ei));
            }
        }
        TStringStream str;
        HTML(str) {
            TAG(TH3) {
                str << "Tablets of StateStorage";
            }
            TABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                         HTML_TAG() {str << "<tr style='background-color:#eee'>";
                         TABLEH() {str << "<span data-toggle='tooltip' title='Tablet Identifier'>TabletID</span>";}
                         TABLEH() {str << "<span data-toggle='tooltip' title='Node from which SS Replica has responded'>Replica Node ID / Hostname</span>";}
                         TABLEH() {str << "<span data-toggle='tooltip' title='Tablet Generation'>Gen</span>";}
                         TABLEH() {str << "<span data-toggle='tooltip' title='Amount of time tablet has been locked in SS (seconds)'>Locked For</span>";}
                         TABLEH() {str << "<span data-toggle='tooltip' title='Node where tablet has been started'>Tablet Node ID / Hostname</span>";}
                         TABLEH() {str << "<span data-toggle='tooltip' title='Is user part (actor) of the tablet started?'>Active</span>";}
                         str << "</tr>";
                    }
                }
                TABLEBODY() {
                         for (auto iit = indexByTabletId.begin(); iit != indexByTabletId.end(); ++iit) {
                             for (auto iptr = iit->second.begin(); iptr != iit->second.end(); ++iptr) {
                                 const NKikimrStateStorage::TEvInfo &ei = *iptr->second;
                                 ui32 replicaNodeId = iptr->first;
                                 TABLER() {
                                     if (iptr == iit->second.begin()) {
                                         HTML_TAG() {str << "<td rowspan='" << iit->second.size() << "'>"
                                                   << "<a href='tablets?TabletID=" << ei.GetTabletID() << "'>" << ei.GetTabletID() << "</a></td>";}
                                     }
                                     TABLED() {
                                         str << replicaNodeId;
                                         auto eq_it = EqualRange(Nodes.begin(), Nodes.end(), replicaNodeId);
                                         if (eq_it.first != Nodes.end() && eq_it.first->Host) str << " / " << eq_it.first->Host;
                                     }
                                     TABLED() {str << ei.GetCurrentGeneration();}
                                     TABLED() {if (ei.HasLockedFor()) str << TDuration::MicroSeconds(ei.GetLockedFor()).Seconds();}
                                     TABLED() {
                                         ui32 nodeId = ActorIdFromProto(ei.GetCurrentLeader()).NodeId();
                                         str << nodeId;
                                         auto eq_it = EqualRange(Nodes.begin(), Nodes.end(), nodeId);
                                         if (eq_it.first != Nodes.end() && eq_it.first->Host) str << " / " << eq_it.first->Host;
                                     }
                                     TABLED() {if (ActorIdFromProto(ei.GetCurrentLeaderTablet())) str << "<span class='glyphicon glyphicon-ok' title='User Actor present'/>";}
                                 }
                             }
                         }
                }
            }
            HTML_TAG() {str << "<script>$(document).ready(function(){$('[data-toggle=\"tooltip\"]').tooltip();}</script>";}
        }
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(str.Str()));
        Die(ctx);
    }

    void Notify(const TActorContext &ctx, const TString& html) {
        ctx.Send(Sender, new NMon::TEvHttpInfoRes(html));
    }

    void NoData(const TActorContext &ctx) {
        Notify(ctx, "No data to display");
        Die(ctx);
    }

    void Timeout(const TActorContext &ctx) {
        Notify(ctx, "Timeout");
        Die(ctx);
    }

protected:
    TActorId Sender;
    TVector<TEvInterconnect::TNodeInfo> Nodes;
};

class TNodeTabletMonitor : public TActorBootstrapped<TNodeTabletMonitor> {
private:
    TIntrusivePtr<ITabletStateClassifier> StateClassifier;
    TIntrusivePtr<ITabletListRenderer> TableRenderer;

public:

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TABLET_MONITORING_PROXY;
    }

    TNodeTabletMonitor(const TIntrusivePtr<ITabletStateClassifier>& stateClassifier,
                       const TIntrusivePtr<ITabletListRenderer>& renderer)
        : StateClassifier(stateClassifier)
        , TableRenderer(renderer)
    {
    }

    void Bootstrap(const TActorContext &ctx) {
        Become(&TThis::StateWork);
        NActors::TMon* mon = AppData(ctx)->Mon;

        if (mon) {
            mon->RegisterActorPage(nullptr, "nodetabmon", "Node Tablet Monitor", false, ctx.ExecutorThread.ActorSystem, ctx.SelfID);
        }
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfo, Handle);
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        NMon::TEvHttpInfo* msg = ev->Get();
        const TCgiParameters& cgi = msg->Request.GetParams();
        if (cgi.Has("action")) {
            const TString &actionParam = cgi.Get("action");
            if (actionParam == "browse_nodes") {
                ctx.ExecutorThread.RegisterActor(new TNodeList(ev->Sender));
                return;
            } else if (actionParam == "kill_tablet") {
                if (cgi.Has("tablet_id")) {
                    ui64 tabletId = FromStringWithDefault<ui64>(cgi.Get("tablet_id"));
                    ctx.Register(CreateTabletKiller(tabletId));
                    if (cgi.Has("filter_node_id"))
                        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("<meta http-equiv=\"refresh\" content=\"0; nodetabmon?action=browse_tablets&node_id=" + cgi.Get("filter_node_id") + "\" />"));
                    else
                        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes("<meta http-equiv=\"refresh\" content=\"0; nodetabmon?action=browse_tablets\" />"));
                    return;
                }
            } else if (actionParam == "browse_tablets") {
                ui32 filterNodeId = 0;
                if (cgi.Has("node_id"))
                    filterNodeId = FromStringWithDefault<ui32>(cgi.Get("node_id"));
                ctx.ExecutorThread.RegisterActor(new TTabletList(ev->Sender, filterNodeId, StateClassifier, TableRenderer));
                return;
            } else if (actionParam == "browse_ss") {
                ctx.ExecutorThread.RegisterActor(new TStateStorageTabletList(ev->Sender));
                return;
            }
        }
        ctx.ExecutorThread.RegisterActor(new TNodeList(ev->Sender));
    }
};


IActor* CreateNodeTabletMonitor(const TIntrusivePtr<ITabletStateClassifier>& stateClassifier,
                                const TIntrusivePtr<ITabletListRenderer>& renderer)
{
    return new TNodeTabletMonitor(stateClassifier, renderer);
}

} // NNodeTabletMonitor
} // NKikimr
