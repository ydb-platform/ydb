#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/subdomain.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include <ydb/core/viewer/json/json.h>
#include "viewer.h"
#include "json_pipe_req.h"
#include "wb_aggregate.h"
#include "wb_merge.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonNetInfo : public TViewerPipeClient {
    using TThis = TJsonNetInfo;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    std::unordered_map<TString, NKikimrViewer::TTenant> TenantByPath;
    std::unordered_map<TPathId, NKikimrViewer::TTenant> TenantBySubDomainKey;
    std::unordered_map<TString, std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> NavigateResult;
    std::unique_ptr<TEvHive::TEvResponseHiveDomainStats> HiveStats;
    NMon::TEvHttpInfo::TPtr Event;
    std::vector<TNodeId> NodeIds;
    std::unordered_map<TNodeId, std::unique_ptr<TEvWhiteboard::TEvSystemStateResponse>> NodeSysInfo;
    std::unordered_map<TNodeId, std::unique_ptr<TEvWhiteboard::TEvNodeStateResponse>> NodeNetInfo;
    std::unique_ptr<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TString User;
    TString Path;

public:
    TJsonNetInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Path = params.Get("path");

        SendRequest(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());

        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto *domain = domains->GetDomain();
        ui64 consoleId = MakeConsoleID();

        if (consoleId != 0) {
            RequestConsoleListTenants();
        }

        ui64 hiveId = domains->GetHive();
        if (hiveId != TDomainsInfo::BadTabletId) {
            RequestHiveDomainStats(hiveId);
        }

        TString domainPath = "/" + domain->Name;
        if (Path.empty() || domainPath == Path) {
            NKikimrViewer::TTenant& tenant = TenantByPath[domainPath];
            tenant.SetName(domainPath);
            tenant.SetState(Ydb::Cms::GetDatabaseStatusResult::State::GetDatabaseStatusResult_State_RUNNING);
            RequestSchemeCacheNavigate(domainPath);
        }

        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void PassAway() override {
        for (const TNodeId nodeId : NodeIds) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(TEvHive::TEvResponseHiveDomainStats, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvNodeStateResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        NodesInfo.reset(ev->Release().Release());
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            if (!Path.empty() && path != Path) {
                continue;
            }
            TenantByPath[path];
            RequestSchemeCacheNavigate(path);
        }
        RequestDone();
    }

    void Handle(TEvHive::TEvResponseHiveDomainStats::TPtr& ev) {
        HiveStats.reset(ev->Release().Release());
        for (const NKikimrHive::THiveDomainStats& hiveStat : HiveStats->Record.GetDomainStats()) {
            TPathId subDomainKey(hiveStat.GetShardId(), hiveStat.GetPathId());
            NKikimrViewer::TTenant& tenant = TenantBySubDomainKey[subDomainKey];
            tenant.SetId(TStringBuilder() << hiveStat.GetShardId() << '-' << hiveStat.GetPathId());
            tenant.MutableNodeIds()->MergeFrom(hiveStat.GetNodeIds());
            for (TNodeId nodeId : hiveStat.GetNodeIds()) {
                NodeIds.emplace_back(nodeId);
            }
        }
        for (TNodeId nodeId : NodeIds) {
            TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
            SendRequest(
                whiteboardServiceId,
                new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateRequest(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                nodeId);
            SendRequest(
                whiteboardServiceId,
                new NNodeWhiteboard::TEvWhiteboard::TEvNodeStateRequest(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
                nodeId);

        }
        RequestDone();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Get()->Request->ResultSet.size() == 1 && ev->Get()->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            TString path = CanonizePath(ev->Get()->Request->ResultSet.begin()->Path);
            NavigateResult[path].reset(ev->Release().Release());
        }
        RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        NodeSysInfo[nodeId].reset(ev->Release().Release());
        RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvNodeStateResponse::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        NodeNetInfo[nodeId].reset(ev->Release().Release());
        RequestDone();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        ui32 nodeId = ev.Get()->Cookie;
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvSystemStateRequest) {
            if (NodeSysInfo.emplace(nodeId, nullptr).second) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == NNodeWhiteboard::TEvWhiteboard::EvNodeStateRequest) {
            if (NodeNetInfo.emplace(nodeId, nullptr).second) {
                RequestDone();
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        ui32 nodeId = ev->Get()->NodeId;
        if (NodeSysInfo.emplace(nodeId, nullptr).second) {
            RequestDone();
        }
        if (NodeNetInfo.emplace(nodeId, nullptr).second) {
            RequestDone();
        }
    }

    void ReplyAndPassAway() override {
        THashMap<TNodeId, const TEvInterconnect::TNodeInfo*> nodeInfoIndex;
        if (NodesInfo) {
            for (const TEvInterconnect::TNodeInfo& nodeInfo : NodesInfo->Nodes) {
                nodeInfoIndex[nodeInfo.NodeId] = &nodeInfo;
            }
        }
        TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
        NKikimrViewer::TNetInfo result;
        for (const std::pair<const TString, NKikimrViewer::TTenant>& prTenant : TenantByPath) {
            const TString& path = prTenant.first;
            //const NKikimrViewer::TTenant& tenantByPath(prTenant.second);
            NKikimrViewer::TNetTenantInfo& netTenantInfo = *result.AddTenants();
            netTenantInfo.SetName(path);
            auto itNavigate = NavigateResult.find(path);
            if (itNavigate != NavigateResult.end()) {
                auto domainInfo = itNavigate->second->Request->ResultSet.begin()->DomainInfo;
                TPathId subDomainKey(domainInfo->DomainKey);
                const NKikimrViewer::TTenant& tenantBySubDomainKey(TenantBySubDomainKey[subDomainKey]);
                for (TNodeId nodeId : tenantBySubDomainKey.GetNodeIds()) {
                    NKikimrViewer::TNetNodeInfo& netNodeInfo = *netTenantInfo.AddNodes();
                    netNodeInfo.SetNodeId(nodeId);
                    auto itSysInfo = NodeSysInfo.find(nodeId);
                    if (itSysInfo != NodeSysInfo.end()) {
                        if (itSysInfo->second != nullptr && itSysInfo->second->Record.SystemStateInfoSize() == 1) {
                            const NKikimrWhiteboard::TSystemStateInfo& sysInfo = itSysInfo->second->Record.GetSystemStateInfo(0);
                            if (sysInfo.HasDataCenter()) {
                                netNodeInfo.SetDataCenter(sysInfo.GetDataCenter());
                            }
                            if (sysInfo.HasRack()) {
                                netNodeInfo.SetRack(sysInfo.GetRack());
                            }
                        }
                    }
                    if (dynamicNameserviceConfig) {
                        netNodeInfo.SetNodeType(nodeId <= dynamicNameserviceConfig->MaxStaticNodeId ? NKikimrViewer::ENodeType::Static : NKikimrViewer::ENodeType::Dynamic);
                    }
                    auto itNodeInfo = nodeInfoIndex.find(nodeId);
                    if (itNodeInfo != nodeInfoIndex.end()) {
                        netNodeInfo.SetHost(itNodeInfo->second->Host);
                        netNodeInfo.SetPort(itNodeInfo->second->Port);
                    }
                    auto itNetInfo = NodeNetInfo.find(nodeId);
                    if (itNetInfo != NodeNetInfo.end()) {
                        if (itNetInfo->second != nullptr) {
                            for (const NKikimrWhiteboard::TNodeStateInfo& netInfo : itNetInfo->second->Record.GetNodeStateInfo()) {
                                TString peerName(netInfo.GetPeerName());
                                TNodeId nodeId = FromStringWithDefault<TNodeId>(TStringBuf(peerName).Before(':'));
                                if (nodeInfoIndex.find(nodeId) == nodeInfoIndex.end()) {
                                    continue;
                                }
                                NKikimrViewer::TNetNodePeerInfo& netNodePeerInfo = *netNodeInfo.AddPeers();
                                netNodePeerInfo.SetNodeId(nodeId);
                                netNodePeerInfo.SetPeerName(peerName);
                                netNodePeerInfo.SetConnected(netInfo.GetConnected());
                                netNodePeerInfo.SetConnectStatus(GetViewerFlag(netInfo.GetConnectStatus()));
                                netNodePeerInfo.SetChangeTime(netInfo.GetChangeTime());
                                if (dynamicNameserviceConfig) {
                                    netNodePeerInfo.SetNodeType(nodeId <= dynamicNameserviceConfig->MaxStaticNodeId ? NKikimrViewer::ENodeType::Static : NKikimrViewer::ENodeType::Dynamic);
                                }
                                auto itSysInfo = NodeSysInfo.find(nodeId);
                                if (itSysInfo != NodeSysInfo.end()) {
                                    if (itSysInfo->second != nullptr && itSysInfo->second->Record.SystemStateInfoSize() == 1) {
                                        const NKikimrWhiteboard::TSystemStateInfo& sysInfo = itSysInfo->second->Record.GetSystemStateInfo(0);
                                        if (sysInfo.HasDataCenter()) {
                                            netNodePeerInfo.SetDataCenter(sysInfo.GetDataCenter());
                                        }
                                        if (sysInfo.HasRack()) {
                                            netNodePeerInfo.SetRack(sysInfo.GetRack());
                                        }
                                    }
                                }
                                auto itNodeInfo = nodeInfoIndex.find(nodeId);
                                if (itNodeInfo != nodeInfoIndex.end()) {
                                    netNodePeerInfo.SetHost(itNodeInfo->second->Host);
                                    netNodePeerInfo.SetPort(itNodeInfo->second->Port);
                                }
                            }
                        }
                    }

                    // TODO(xenoxeno)
                    netNodeInfo.SetOverall(NKikimrViewer::EFlag::Green);
                }
            }

            // TODO(xenoxeno)
            netTenantInfo.SetOverall(NKikimrViewer::EFlag::Green);
        }

        // TODO(xenoxeno)
        result.SetOverall(NKikimrViewer::EFlag::Green);
        TStringStream json;
        TProtoToJson::ProtoToJson(json, result, JsonSettings);
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonNetInfo> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TNetInfo>();
    }
};

template <>
struct TJsonRequestParameters<TJsonNetInfo> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
              - name: path
                in: query
                description: schema path
                required: false
                type: string
              - name: hive_id
                in: query
                description: hive identifier (tablet id)
                required: false
                type: string
              - name: enums
                in: query
                description: convert enums to strings
                required: false
                type: boolean
              - name: ui64
                in: query
                description: return ui64 as number
                required: false
                type: boolean
              - name: timeout
                in: query
                description: timeout in ms
                required: false
                type: integer
            )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonNetInfo> {
    static TString GetSummary() {
        return "Network information";
    }
};

template <>
struct TJsonRequestDescription<TJsonNetInfo> {
    static TString GetDescription() {
        return "Returns network information";
    }
};

}
}
