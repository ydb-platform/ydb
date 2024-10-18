#pragma once
#include "json_handlers.h"
#include "viewer.h"
#include "viewer_bsgroupinfo.h"
#include "viewer_pdiskinfo.h"
#include "viewer_tabletinfo.h"
#include "viewer_vdiskinfo.h"

namespace NKikimr::NViewer {

using namespace NActors;
using ::google::protobuf::FieldDescriptor;

class TJsonCounters : public TActorBootstrapped<TJsonCounters> {
    using TThis = TJsonCounters;
    using TBase = TActorBootstrapped<TJsonCounters>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    ui32 Requested;
    ui32 Received;
    THolder<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TMap<ui32, NKikimrWhiteboard::TEvVDiskStateResponse> VDiskInfo;
    TMap<ui32, NKikimrWhiteboard::TEvPDiskStateResponse> PDiskInfo;
    TMap<ui32, NKikimrWhiteboard::TEvTabletStateResponse> TabletInfo;
    TMap<ui32, NKikimrWhiteboard::TEvBSGroupStateResponse> BSGroupInfo;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonCounters(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
        , Requested(0)
        , Received(0)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const TActorId nameserviceId = GetNameserviceActorId();
        ctx.Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        TBase::Become(&TThis::StateRequestedBrowse);
        ctx.Schedule(TDuration::Seconds(60), new TEvents::TEvWakeup());
    }

    void Die(const TActorContext& ctx) override {
        if (NodesInfo != nullptr) {
            for (const auto& ni : NodesInfo->Nodes) {
                ctx.Send(TActivationContext::InterconnectProxy(ni.NodeId), new TEvents::TEvUnsubscribe());
            }
        }
        TBase::Die(ctx);
    }

    void SendRequest(ui32 nodeId, const TActorContext& ctx) {
        TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
        ctx.Send(whiteboardServiceId, new TEvWhiteboard::TEvVDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        ++Requested;
        ctx.Send(whiteboardServiceId, new TEvWhiteboard::TEvPDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        ++Requested;
        ctx.Send(whiteboardServiceId, new TEvWhiteboard::TEvTabletStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        ++Requested;
        ctx.Send(whiteboardServiceId, new TEvWhiteboard::TEvBSGroupStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        ++Requested;
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext& ctx) {
        NodesInfo = ev->Release();
        for (const auto& ni : NodesInfo->Nodes) {
            SendRequest(ni.NodeId, ctx);
        }
        if (Requested > 0) {
            TBase::Become(&TThis::StateRequestedNodeInfo);
        } else {
            ReplyAndDie(ctx);
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev.Get()->Cookie;
        switch (ev->Get()->SourceType) {
        case TEvWhiteboard::EvVDiskStateRequest:
            if (VDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvVDiskStateResponse{}).second) {
                NodeStateInfoReceived(ctx);
            }
            break;
        case TEvWhiteboard::EvPDiskStateRequest:
            if (PDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvPDiskStateResponse{}).second) {
                NodeStateInfoReceived(ctx);
            }
            break;
        case TEvWhiteboard::EvTabletStateRequest:
            if (TabletInfo.emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
                NodeStateInfoReceived(ctx);
            }
            break;
        case TEvWhiteboard::EvBSGroupStateRequest:
            if (BSGroupInfo.emplace(nodeId, NKikimrWhiteboard::TEvBSGroupStateResponse{}).second) {
                NodeStateInfoReceived(ctx);
            }
            break;
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
        ui32 nodeId = ev->Get()->NodeId;
        if (VDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvVDiskStateResponse{}).second) {
            NodeStateInfoReceived(ctx);
        }
        if (PDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvPDiskStateResponse{}).second) {
            NodeStateInfoReceived(ctx);
        }
        if (TabletInfo.emplace(nodeId, NKikimrWhiteboard::TEvTabletStateResponse{}).second) {
            NodeStateInfoReceived(ctx);
        }
        if (BSGroupInfo.emplace(nodeId, NKikimrWhiteboard::TEvBSGroupStateResponse{}).second) {
            NodeStateInfoReceived(ctx);
        }
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        VDiskInfo[nodeId] = std::move(ev->Get()->Record);
        NodeStateInfoReceived(ctx);
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        PDiskInfo[nodeId] = std::move(ev->Get()->Record);
        NodeStateInfoReceived(ctx);
    }

    void Handle(TEvWhiteboard::TEvTabletStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        TabletInfo[nodeId] = std::move(ev->Get()->Record);
        NodeStateInfoReceived(ctx);
    }

    void Handle(TEvWhiteboard::TEvBSGroupStateResponse::TPtr& ev, const TActorContext& ctx) {
        ui64 nodeId = ev.Get()->Cookie;
        BSGroupInfo[nodeId] = std::move(ev->Get()->Record);
        NodeStateInfoReceived(ctx);
    }

    void NodeStateInfoReceived(const TActorContext& ctx) {
        ++Received;
        if (Received == Requested) {
            ReplyAndDie(ctx);
        }
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    STFUNC(StateRequestedNodeInfo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            HFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            HFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            HFunc(TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            HFunc(TEvents::TEvUndelivered, Undelivered);
            HFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            CFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    template<typename ResponseType>
    void RenderStats(TStringStream& json,
                     ResponseType& response,
                     const TEvInterconnect::TNodeInfo& nodeInfo,
                     const TString& subsystem,
                     const TVector<const FieldDescriptor*>& groupFields) {

        TWhiteboardGrouper<ResponseType>::GroupResponse(response, groupFields, true);
        auto& stateInfo = TWhiteboardInfo<ResponseType>::GetElementsField(response);
        TStringBuf host(nodeInfo.Host);
        size_t pos = host.find('.');
        if (pos != TString::npos) {
            host = host.substr(0, pos);
        }
        for (typename TWhiteboardInfo<ResponseType>::TElementType& info : stateInfo) {
            const Reflection& reflectionFrom = *info.GetReflection();
            json << ",{\"labels\":{";
            if (nodeInfo.NodeId != 0) {
                json << "\"node\":" << nodeInfo.NodeId << ",";
            }
            json << "\"host\":\"" << host << "\",";
            if (nodeInfo.Port != 0) {
                json << "\"port\":" << nodeInfo.Port << ",";
            }
            json << "\"subsystem\":\"" << subsystem << "\",";
            json << "\"" << groupFields.front()->name() << "\":\"";
            json << reflectionFrom.GetEnum(info, groupFields.front())->name();
            json << "\"";
            json << "},\"value\":";
            json << info.GetCount();
            json << '}';
        }
    }

    void RenderStats(TStringStream& json,
                     NKikimrWhiteboard::TEvVDiskStateResponse& response,
                     const TEvInterconnect::TNodeInfo& nodeInfo) {
        if (response.VDiskStateInfoSize() == 0)
            return;
        static TVector<const FieldDescriptor*> groupFields
                = TWhiteboardGrouper<NKikimrWhiteboard::TEvVDiskStateResponse>::GetProtoFields("VDiskState");
        RenderStats(json, response, nodeInfo, "VDisk", groupFields);
    }

    void RenderStats(TStringStream& json,
                     NKikimrWhiteboard::TEvPDiskStateResponse& response,
                     const TEvInterconnect::TNodeInfo& nodeInfo) {
        if (response.PDiskStateInfoSize() == 0)
            return;
        static TVector<const FieldDescriptor*> groupFields
                = TWhiteboardGrouper<NKikimrWhiteboard::TEvPDiskStateResponse>::GetProtoFields("State");
        RenderStats(json, response, nodeInfo, "PDisk", groupFields);
    }

    void RenderStats(TStringStream& json,
                     NKikimrWhiteboard::TEvTabletStateResponse& response,
                     const TEvInterconnect::TNodeInfo& nodeInfo) {
        if (response.TabletStateInfoSize() == 0)
            return;
        static TVector<const FieldDescriptor*> groupFields
                = TWhiteboardGrouper<NKikimrWhiteboard::TEvTabletStateResponse>::GetProtoFields("State");
        RenderStats(json, response, nodeInfo, "Tablet", groupFields);
    }

    void ReplyAndDie(const TActorContext& ctx) {
        TStringStream json;

        json << '{';
        json << "\"sensors\":[";

        Sort(NodesInfo->Nodes, [](
             const TEvInterconnect::TNodeInfo& a,
             const TEvInterconnect::TNodeInfo& b) -> bool {
            return a.NodeId < b.NodeId;
        });

        ui32 nodesResponded = 0;
        for (const std::pair<const ui32, NKikimrWhiteboard::TEvVDiskStateResponse>& value : VDiskInfo) {
            if (value.second.HasResponseTime()) {
                ++nodesResponded;
            }
        }

        json << "{\"labels\":{";
        json << "\"subsystem\":\"Viewer\",";
        json << "\"host\":\"cluster\",";
        json << "\"sensor\":\"NodesResponded\"";
        json << "},\"value\":" << nodesResponded;
        json << '}';

        NKikimrWhiteboard::TEvTabletStateResponse mergedTabletInfo;
        MergeWhiteboardResponses(mergedTabletInfo, TabletInfo);
        TabletInfo.clear();
        for (const auto& tabletInfo : mergedTabletInfo.GetTabletStateInfo()) {
            if (!tabletInfo.HasNodeId()) {
                continue;
            }
            auto it = TabletInfo.find(tabletInfo.GetNodeId());
            if (it == TabletInfo.end()) {
                it = TabletInfo.emplace(tabletInfo.GetNodeId(), NKikimrWhiteboard::TEvTabletStateResponse{}).first;
            }
            it->second.AddTabletStateInfo()->CopyFrom(tabletInfo);
        }

        std::array<int, 20> pDiskUserSpaceHistogram = {};

        auto itVDiskInfo = VDiskInfo.begin();
        auto itPDiskInfo = PDiskInfo.begin();
        auto itTabletInfo = TabletInfo.begin();

        for (const auto& nodeInfo : NodesInfo->Nodes) {
            while (itVDiskInfo != VDiskInfo.end() && itVDiskInfo->first < nodeInfo.NodeId)
                ++itVDiskInfo;
            if (itVDiskInfo != VDiskInfo.end() && itVDiskInfo->first == nodeInfo.NodeId) {
                RenderStats(json, itVDiskInfo->second, nodeInfo);
            }
            while (itPDiskInfo != PDiskInfo.end() && itPDiskInfo->first < nodeInfo.NodeId)
                ++itPDiskInfo;
            if (itPDiskInfo != PDiskInfo.end() && itPDiskInfo->first == nodeInfo.NodeId) {
                RenderStats(json, itPDiskInfo->second, nodeInfo);
                auto& stateInfo = TWhiteboardInfo<NKikimrWhiteboard::TEvPDiskStateResponse>::GetElementsField(itPDiskInfo->second);
                for (const typename TWhiteboardInfo<NKikimrWhiteboard::TEvPDiskStateResponse>::TElementType& info : stateInfo) {
                    if (info.GetTotalSize() > 0 && info.GetAvailableSize() > 0) {
                        ++pDiskUserSpaceHistogram[std::min((info.GetTotalSize() - info.GetAvailableSize()) * pDiskUserSpaceHistogram.size() / info.GetTotalSize(), pDiskUserSpaceHistogram.size() - 1)];
                    }
                }
            }
            while (itTabletInfo != TabletInfo.end() && itTabletInfo->first < nodeInfo.NodeId)
                ++itTabletInfo;
            if (itTabletInfo != TabletInfo.end() && itTabletInfo->first == nodeInfo.NodeId) {
                RenderStats(json, itTabletInfo->second, nodeInfo);
            }
        }

        static TEvInterconnect::TNodeInfo totals(0, "", "cluster", "", 0, TNodeLocation());

        for (size_t p = 0; p < pDiskUserSpaceHistogram.size(); ++p) {
            json << ",{\"labels\":{";
            json << "\"bin\":\"" << ((p + 1) * 100 / pDiskUserSpaceHistogram.size()) << "%\",";
            json << "\"subsystem\":\"PDisk\",";
            json << "\"host\":\"cluster\",";
            json << "\"sensor\":\"UsedSpace\"";
            json << "},\"value\":";
            json << pDiskUserSpaceHistogram[p];
            json << '}';
        }

        NKikimrWhiteboard::TEvVDiskStateResponse mergedVDiskInfo;
        MergeWhiteboardResponses(mergedVDiskInfo, VDiskInfo);
        RenderStats(json, mergedVDiskInfo, totals);
        NKikimrWhiteboard::TEvPDiskStateResponse mergedPDiskInfo;
        MergeWhiteboardResponses(mergedPDiskInfo, PDiskInfo);
        RenderStats(json, mergedPDiskInfo, totals);
        RenderStats(json, mergedTabletInfo, totals);
        NKikimrWhiteboard::TEvBSGroupStateResponse mergedBSGroupInfo;
        MergeWhiteboardResponses(mergedBSGroupInfo, BSGroupInfo);

        std::array<int, 9> bsGroupUnavaiableHistogram = {};
        std::array<int, 9> bsGroupGreenHistogram = {};
        std::array<int, 9> bsGroupNotGreenHistogram = {};
        std::unordered_map<ui64, int> bsGroupVDisks;
        std::unordered_map<ui64, int> bsGroupGreenVDisks;
        std::unordered_map<ui64, int> bsGroupNotGreenVDisks;
        {
            auto& stateInfo = TWhiteboardInfo<NKikimrWhiteboard::TEvBSGroupStateResponse>::GetElementsField(mergedBSGroupInfo);
            for (const typename TWhiteboardInfo<NKikimrWhiteboard::TEvBSGroupStateResponse>::TElementType& info : stateInfo) {
                bsGroupVDisks[info.GetGroupID()] = info.VDiskIdsSize();
            }
        }
        {
            auto& stateInfo = TWhiteboardInfo<NKikimrWhiteboard::TEvVDiskStateResponse>::GetElementsField(mergedVDiskInfo);
            for (const typename TWhiteboardInfo<NKikimrWhiteboard::TEvVDiskStateResponse>::TElementType& info : stateInfo) {
                auto groupId = info.GetVDiskId().GetGroupID();
                bsGroupVDisks[groupId]--;
                auto flag = GetVDiskOverallFlag(info);
                if (flag == NKikimrViewer::EFlag::Green && info.GetReplicated()) {
                    bsGroupGreenVDisks[groupId]++;
                } else {
                    bsGroupNotGreenVDisks[groupId]++;
                }
            }
        }
        {
            for (auto it = bsGroupVDisks.begin(); it != bsGroupVDisks.end(); ++it) {
                int idx = it->second;
                if (idx < 0) {
                    idx = 0;
                }
                if (idx >= (int)bsGroupUnavaiableHistogram.size()) {
                    idx = bsGroupUnavaiableHistogram.size() - 1;
                }
                bsGroupUnavaiableHistogram[idx]++;
            }
        }
        {
            for (auto it = bsGroupGreenVDisks.begin(); it != bsGroupGreenVDisks.end(); ++it) {
                int idx = it->second;
                if (idx < 0) {
                    idx = 0;
                }
                if (idx >= (int)bsGroupGreenHistogram.size()) {
                    idx = bsGroupGreenHistogram.size() - 1;
                }
                bsGroupGreenHistogram[idx]++;
            }
        }
        {
            for (auto it = bsGroupNotGreenVDisks.begin(); it != bsGroupNotGreenVDisks.end(); ++it) {
                int idx = it->second;
                if (idx < 0) {
                    idx = 0;
                }
                if (idx >= (int)bsGroupNotGreenHistogram.size()) {
                    idx = bsGroupNotGreenHistogram.size() - 1;
                }
                bsGroupNotGreenHistogram[idx]++;
            }
        }

        for (size_t p = 0; p < bsGroupUnavaiableHistogram.size(); ++p) {
            json << ",{\"labels\":{";
            json << "\"bin\":\"" << p << "\",";
            json << "\"subsystem\":\"BSGroups\",";
            json << "\"host\":\"cluster\",";
            json << "\"sensor\":\"UnavailableVDisks\"";
            json << "},\"value\":";
            json << bsGroupUnavaiableHistogram[p];
            json << '}';
        }

        for (size_t p = 0; p < bsGroupGreenHistogram.size(); ++p) {
            json << ",{\"labels\":{";
            json << "\"bin\":\"" << p << "\",";
            json << "\"subsystem\":\"BSGroups\",";
            json << "\"host\":\"cluster\",";
            json << "\"sensor\":\"GreenVDisks\"";
            json << "},\"value\":";
            json << bsGroupGreenHistogram[p];
            json << '}';
        }

        for (size_t p = 0; p < bsGroupNotGreenHistogram.size(); ++p) {
            json << ",{\"labels\":{";
            json << "\"bin\":\"" << p << "\",";
            json << "\"subsystem\":\"BSGroups\",";
            json << "\"host\":\"cluster\",";
            json << "\"sensor\":\"NotGreenVDisks\"";
            json << "},\"value\":";
            json << bsGroupNotGreenHistogram[p];
            json << '}';
        }

        json << ']';
        json << '}';

        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void Timeout(const TActorContext& ctx) {
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "counters",
            .Description = "counters",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TStorageUsageStats>());
        return yaml;
    }
};

}
