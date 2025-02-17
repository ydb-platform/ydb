#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include "viewer_bsgroupinfo.h"
#include "viewer_vdiskinfo.h"
#include "viewer_pdiskinfo.h"
#include "viewer_helper.h"
#include "wb_merge.h"

template<>
struct std::hash<NKikimrBlobStorage::TVSlotId> {
    std::size_t operator()(const NKikimrBlobStorage::TVSlotId& vSlotId) const {
        return std::hash<ui32>()(vSlotId.GetNodeId())
            ^ (std::hash<ui32>()(vSlotId.GetPDiskId()) << 1)
            ^ (std::hash<ui32>()(vSlotId.GetVSlotId()) << 2);
    }
};

template<>
struct std::equal_to<NKikimrBlobStorage::TVSlotId> {
    bool operator()(const NKikimrBlobStorage::TVSlotId& lhs, const NKikimrBlobStorage::TVSlotId& rhs) const {
        return lhs.GetNodeId() == rhs.GetNodeId()
            && lhs.GetPDiskId() == rhs.GetPDiskId()
            && lhs.GetVSlotId() == rhs.GetVSlotId();
    }
};

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

using ::google::protobuf::FieldDescriptor;

class TJsonStorageBase : public TViewerPipeClient {
protected:
    using TBase = TViewerPipeClient;
    using TThis = TJsonStorageBase;

    using TNodeId = ui32;
    IViewer* Viewer;
    TActorId Initiator;
    NMon::TEvHttpInfo::TPtr Event;
    THolder<TEvInterconnect::TEvNodesInfo> NodesInfo;
    TMap<ui32, NKikimrWhiteboard::TEvVDiskStateResponse> VDiskInfo;
    TMap<ui32, NKikimrWhiteboard::TEvPDiskStateResponse> PDiskInfo;
    TMap<ui32, NKikimrWhiteboard::TEvBSGroupStateResponse> BSGroupInfo;
    THashMap<TString, THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>> DescribeResult;
    THashMap<TTabletId, THolder<TEvHive::TEvResponseHiveStorageStats>> HiveStorageStats;
    THolder<TEvBlobStorage::TEvControllerConfigResponse> BaseConfig;

    // indexes
    THashMap<TVDiskID, NKikimrWhiteboard::TVDiskStateInfo*> VDiskId2vDiskStateInfo;
    THashMap<ui32, std::vector<TNodeId>> Group2NodeId;

    struct TStoragePoolInfo {
        TString Kind;
        TString MediaType;
        TSet<TString> Groups;
        NKikimrViewer::EFlag Overall = NKikimrViewer::EFlag::Grey;
    };

    THashMap<TString, TStoragePoolInfo> StoragePoolInfo;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TString FilterTenant;
    THashSet<TString> FilterStoragePools;
    TString Filter;
    std::unordered_set<TString> FilterGroupIds;
    std::unordered_set<TNodeId> FilterNodeIds;
    std::unordered_set<ui32> FilterPDiskIds;
    THashSet<TString> EffectiveGroupFilter;
    std::unordered_set<TNodeId> NodeIds;
    bool NeedAdditionalNodesRequests;

    enum class EWith {
        Everything,
        MissingDisks,
        SpaceProblems,
    };

    enum ETimeoutTag {
        TimeoutBSC,
        TimeoutFinal,
    };

    EWith With = EWith::Everything;

    ui32 UsagePace = 5;
    TVector<ui32> UsageBuckets;

    struct TGroupRow {
        TString PoolName;
        TString GroupId;
        TString Kind;
        TString MediaType;
        TString Erasure;
        ui32 Degraded;
        float Usage;
        ui64 Used;
        ui64 Limit;
        ui64 Read;
        ui64 Write;

        TGroupRow()
            : Used(0)
            , Limit(0)
            , Read(0)
            , Write(0)
        {}
    };
    THashMap<TString, TGroupRow> GroupRowsByGroupId;

    TJsonStorageBase(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Initiator(ev->Sender)
        , Event(std::move(ev))
    {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        FilterTenant = params.Get("tenant");
        TString filterStoragePool = params.Get("pool");
        if (!filterStoragePool.empty()) {
            FilterStoragePools.emplace(filterStoragePool);
        }
        SplitIds(params.Get("node_id"), ',', FilterNodeIds);
        SplitIds(params.Get("pdisk_id"), ',', FilterPDiskIds);
        NeedAdditionalNodesRequests = !FilterNodeIds.empty();
        SplitIds(params.Get("group_id"), ',', FilterGroupIds);
        Filter = params.Get("filter");
        if (params.Get("with") == "missing") {
            With = EWith::MissingDisks;
        } if (params.Get("with") == "space") {
            With = EWith::SpaceProblems;
        }
    }

public:
    void Bootstrap() override {
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;

        if (FilterTenant.empty()) {
            RequestConsoleListTenants();
        } else {
            RequestSchemeCacheNavigate(FilterTenant);
        }
        auto itZero = FilterNodeIds.find(0);
        if (itZero != FilterNodeIds.end()) {
            FilterNodeIds.erase(itZero);
            FilterNodeIds.insert(TlsActivationContext->ActorSystem()->NodeId);
        }
        if (FilterNodeIds.empty()) {
            SendRequest(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        } else {
            for (ui32 nodeId : FilterNodeIds) {
                SendNodeRequests(nodeId);
            }
        }
        if (Requests == 0) {
            ReplyAndPassAway();
            return;
        }

        RequestBSControllerConfigWithStoragePools();

        TBase::Become(&TThis::StateWork);
        Schedule(TDuration::MilliSeconds(Timeout / 100 * 70), new TEvents::TEvWakeup(TimeoutBSC)); // 70% timeout (for bsc)
        Schedule(TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup(TimeoutFinal)); // timeout for the rest
    }

    void PassAway() override {
        for (const TNodeId nodeId : NodeIds) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
    }

    void SendNodeRequests(ui32 nodeId) {
        if (NodeIds.insert(nodeId).second) {
            TActorId whiteboardServiceId = MakeNodeWhiteboardServiceId(nodeId);
            SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvVDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
            SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvPDiskStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
            SendRequest(whiteboardServiceId, new TEvWhiteboard::TEvBSGroupStateRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId);
        }
    }

    void Handle(TEvBlobStorage::TEvControllerSelectGroupsResult::TPtr& ev) {
        for (const auto& matchingGroups : ev->Get()->Record.GetMatchingGroups()) {
            for (const auto& group : matchingGroups.GetGroups()) {
                TString storagePoolName = group.GetStoragePoolName();
                StoragePoolInfo[storagePoolName].Groups.emplace(ToString(group.GetGroupID()));
            }
        }
        RequestDone();
    }

    TString GetMediaType(const NKikimrBlobStorage::TDefineStoragePool& pool) const {
        for (const NKikimrBlobStorage::TPDiskFilter& filter : pool.GetPDiskFilter()) {
            for (const NKikimrBlobStorage::TPDiskFilter::TRequiredProperty& property : filter.GetProperty()) {
                if (property.HasType()) {
                    return ToString(property.GetType());
                }
            }
        }
        return TString();
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(ev->Get()->Record);

        if (pbRecord.HasResponse() && pbRecord.GetResponse().StatusSize() > 1) {
            const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
            if (pbStatus.HasBaseConfig()) {
                BaseConfig = ev->Release();
                if (!FilterNodeIds.empty()) {
                    std::vector<TNodeId> additionalNodeIds;
                    const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(BaseConfig->Record);
                    const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
                    const NKikimrBlobStorage::TBaseConfig& pbConfig(pbStatus.GetBaseConfig());
                    for (const NKikimrBlobStorage::TBaseConfig::TGroup& group : pbConfig.GetGroup()) {
                        for (const NKikimrBlobStorage::TVSlotId& vslot : group.GetVSlotId()) {
                            if (FilterNodeIds.count(vslot.GetNodeId()) != 0) {
                                for (const NKikimrBlobStorage::TVSlotId& vslot : group.GetVSlotId()) {
                                    additionalNodeIds.push_back(vslot.GetNodeId());
                                }
                                break;
                            }
                        }
                    }
                    for (TNodeId nodeId : additionalNodeIds) {
                        SendNodeRequests(nodeId);
                    }
                }
            }
            const NKikimrBlobStorage::TConfigResponse::TStatus& spStatus(pbRecord.GetResponse().GetStatus(1));
            for (const NKikimrBlobStorage::TDefineStoragePool& pool : spStatus.GetStoragePool()) {
                StoragePoolInfo[pool.GetName()].MediaType = GetMediaType(pool);
            }
        }
        RequestDone();
    }

    void Handle(NConsole::TEvConsole::TEvListTenantsResponse::TPtr& ev) {
        Ydb::Cms::ListDatabasesResult listTenantsResult;
        ev->Get()->Record.GetResponse().operation().result().UnpackTo(&listTenantsResult);
        for (const TString& path : listTenantsResult.paths()) {
            RequestSchemeCacheNavigate(path);
        }
        RequestDone();
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        ui32 maxAllowedNodeId = std::numeric_limits<ui32>::max();
        TIntrusivePtr<TDynamicNameserviceConfig> dynamicNameserviceConfig = AppData()->DynamicNameserviceConfig;
        if (dynamicNameserviceConfig) {
            maxAllowedNodeId = dynamicNameserviceConfig->MaxStaticNodeId;
        }
        NodesInfo = ev->Release();
        for (const auto& ni : NodesInfo->Nodes) {
            if (ni.NodeId <= maxAllowedNodeId) {
                SendNodeRequests(ni.NodeId);
            }
        }
        RequestDone();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, bool requestHiveStorageStats = true) {
        if (ev->Get()->Request->ResultSet.size() == 1 && ev->Get()->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            TString path = CanonizePath(ev->Get()->Request->ResultSet.begin()->Path);
            TIntrusiveConstPtr<TSchemeCacheNavigate::TDomainDescription> domainDescription = ev->Get()->Request->ResultSet.begin()->DomainDescription;
            TIntrusiveConstPtr<NSchemeCache::TDomainInfo> domainInfo = ev->Get()->Request->ResultSet.begin()->DomainInfo;

            if (domainInfo != nullptr && domainDescription != nullptr) {
                if (requestHiveStorageStats) {
                    TTabletId hiveId = domainInfo->Params.GetHive();
                    if (hiveId != 0) {
                        RequestHiveStorageStats(hiveId);
                    }
                }

                for (const auto& storagePool : domainDescription->Description.GetStoragePools()) {
                    TString storagePoolName = storagePool.GetName();
                    if (!FilterTenant.empty()) {
                        FilterStoragePools.emplace(storagePoolName);
                    }
                    auto& storagePoolInfo(StoragePoolInfo[storagePoolName]);
                    if (!storagePoolInfo.Kind.empty()) {
                        continue;
                    }
                    storagePoolInfo.Kind = storagePool.GetKind();
                    THolder<TEvBlobStorage::TEvControllerSelectGroups> request = MakeHolder<TEvBlobStorage::TEvControllerSelectGroups>();
                    request->Record.SetReturnAllMatchingGroups(true);
                    request->Record.AddGroupParameters()->MutableStoragePoolSpecifier()->SetName(storagePoolName);
                    RequestBSControllerSelectGroups(std::move(request));
                }
            }
        }
        RequestDone();
    }

    void Handle(TEvHive::TEvResponseHiveStorageStats::TPtr& ev) {
        HiveStorageStats[ev->Cookie] = ev->Release();
        RequestDone();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 nodeId = ev.Get()->Cookie;
        switch (ev->Get()->SourceType) {
        case TEvWhiteboard::EvVDiskStateRequest:
            if (VDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvVDiskStateResponse{}).second) {
                RequestDone();
            }
            break;
        case TEvWhiteboard::EvPDiskStateRequest:
            if (PDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvPDiskStateResponse{}).second) {
                RequestDone();
            }
            break;
        case TEvWhiteboard::EvBSGroupStateRequest:
            if (BSGroupInfo.emplace(nodeId, NKikimrWhiteboard::TEvBSGroupStateResponse{}).second) {
                RequestDone();
            }
            break;
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        ui32 nodeId = ev->Get()->NodeId;
        if (VDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvVDiskStateResponse{}).second) {
            RequestDone();
        }
        if (PDiskInfo.emplace(nodeId, NKikimrWhiteboard::TEvPDiskStateResponse{}).second) {
            RequestDone();
        }
        if (BSGroupInfo.emplace(nodeId, NKikimrWhiteboard::TEvBSGroupStateResponse{}).second) {
            RequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        auto& vDiskInfo = VDiskInfo[nodeId] = std::move(ev->Get()->Record);
        for (auto& vDiskStateInfo : *(vDiskInfo.MutableVDiskStateInfo())) {
            vDiskStateInfo.SetNodeId(nodeId);
            VDiskId2vDiskStateInfo[VDiskIDFromVDiskID(vDiskStateInfo.GetVDiskId())] = &vDiskStateInfo;

            bool isNodeIdValid = FilterNodeIds.empty() || FilterNodeIds.contains(nodeId);
            bool isPDiskIdValid = FilterNodeIds.empty() || FilterPDiskIds.empty() || FilterPDiskIds.contains(vDiskStateInfo.GetPDiskId());
            bool isGroupIdValid = FilterGroupIds.empty() || FilterGroupIds.contains(ToString(vDiskStateInfo.GetVDiskId().GetGroupID()));

            if (isNodeIdValid && isPDiskIdValid && isGroupIdValid) {
                EffectiveGroupFilter.insert(ToString(vDiskStateInfo.GetVDiskId().GetGroupID()));
            }
        }
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        PDiskInfo[nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvBSGroupStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        for (const auto& info : ev->Get()->Record.GetBSGroupStateInfo()) {
            TString storagePoolName = info.GetStoragePoolName();
            if (storagePoolName.empty()) {
                continue;
            }
            if (FilterNodeIds.empty() || FilterNodeIds.contains(info.GetNodeId())) {
                StoragePoolInfo[storagePoolName].Groups.emplace(ToString(info.GetGroupID()));
            }
            for (const auto& vDiskNodeId : info.GetVDiskNodeIds()) {
                Group2NodeId[info.GetGroupID()].push_back(vDiskNodeId);
            }
        }
        BSGroupInfo[nodeId] = std::move(ev->Get()->Record);
        RequestDone();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            hFunc(NConsole::TEvConsole::TEvListTenantsResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvBlobStorage::TEvControllerSelectGroupsResult, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            hFunc(TEvHive::TEvResponseHiveStorageStats, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            hFunc(TEvents::TEvWakeup, HandleTimeout);
        }
    }

    NKikimrViewer::TStorageInfo StorageInfo;
    NKikimrWhiteboard::TEvBSGroupStateResponse MergedBSGroupInfo;
    NKikimrWhiteboard::TEvVDiskStateResponse MergedVDiskInfo;
    NKikimrWhiteboard::TEvPDiskStateResponse MergedPDiskInfo;
    TMap<TString, const NKikimrWhiteboard::TBSGroupStateInfo&> BSGroupIndex;
    TMap<TString, NKikimrHive::THiveStorageGroupStats> BSGroupHiveIndex;
    TMap<NKikimrBlobStorage::TVDiskID, const NKikimrWhiteboard::TVDiskStateInfo&> VDisksIndex;
    std::unordered_map<NKikimrBlobStorage::TVSlotId, const NKikimrWhiteboard::TVDiskStateInfo&> VSlotsIndex;
    TMap<std::pair<ui32, ui32>, const NKikimrWhiteboard::TPDiskStateInfo&> PDisksIndex;
    TMap<TString, TString> BSGroupOverall;
    THashSet<TString> BSGroupWithMissingDisks;
    THashSet<TString> BSGroupWithSpaceProblems;
    TMap<NKikimrBlobStorage::TVDiskID, TString> VDisksOverall;
    TMap<std::pair<ui32, ui32>, TString> PDisksOverall;

    TList<NKikimrWhiteboard::TPDiskStateInfo> PDisksAppended;
    TList<NKikimrWhiteboard::TVDiskStateInfo> VDisksAppended;

    bool CheckAdditionalNodesInfoNeeded() {
        if (NeedAdditionalNodesRequests) {
            NeedAdditionalNodesRequests = false;
            for (const auto& [nodeId, vDiskInfo] : VDiskInfo) {
                if (FilterNodeIds.count(nodeId) == 0) {
                    continue;
                }
                THashSet<ui32> additionalNodes;
                for (const auto& vDiskStateInfo : vDiskInfo.GetVDiskStateInfo()) {
                    ui32 groupId = vDiskStateInfo.GetVDiskId().GetGroupID();
                    auto itNodes = Group2NodeId.find(groupId);
                    if (itNodes != Group2NodeId.end()) {
                        for (TNodeId groupNodeId : itNodes->second) {
                            if (groupNodeId != nodeId && additionalNodes.insert(groupNodeId).second) {
                                SendNodeRequests(groupNodeId);
                            }
                        }
                    }
                }
            }
        }
        return Requests != 0;
    }

    void CollectDiskInfo(bool needDonors) {
        MergeWhiteboardResponses(MergedBSGroupInfo, BSGroupInfo);
        MergeWhiteboardResponses(MergedVDiskInfo, VDiskInfo);
        MergeWhiteboardResponses(MergedPDiskInfo, PDiskInfo);
        for (auto& element : TWhiteboardInfo<NKikimrWhiteboard::TEvPDiskStateResponse>::GetElementsField(MergedPDiskInfo)) {
            element.SetStateFlag(GetWhiteboardFlag(GetPDiskStateFlag(element)));
            auto overall = NKikimrViewer::EFlag_Name(GetPDiskOverallFlag(element));
            auto key = TWhiteboardInfo<NKikimrWhiteboard::TEvPDiskStateResponse>::GetElementKey(element);
            element.ClearOverall();
            PDisksOverall.emplace(key, overall);
            PDisksIndex.emplace(key, element);
        }
        for (auto& element : TWhiteboardInfo<NKikimrWhiteboard::TEvVDiskStateResponse>::GetElementsField(MergedVDiskInfo)) {
            auto overall = NKikimrViewer::EFlag_Name(GetVDiskOverallFlag(element));
            auto key = TWhiteboardInfo<NKikimrWhiteboard::TEvVDiskStateResponse>::GetElementKey(element);
            element.ClearOverall();
            element.ClearStoragePoolName();
            VDisksOverall.emplace(key, overall);
            VDisksIndex.emplace(key, element);
            if (needDonors) {
                NKikimrBlobStorage::TVSlotId slotId;
                slotId.SetNodeId(element.GetNodeId());
                slotId.SetPDiskId(element.GetPDiskId());
                slotId.SetVSlotId(element.GetVDiskSlotId());
                VSlotsIndex.emplace(std::move(slotId), element);
            }
        }
        for (auto& element : TWhiteboardInfo<NKikimrWhiteboard::TEvBSGroupStateResponse>::GetElementsField(MergedBSGroupInfo)) {
            auto state = GetBSGroupOverallState(element, VDisksIndex, PDisksIndex);
            auto key = ToString(TWhiteboardInfo<NKikimrWhiteboard::TEvBSGroupStateResponse>::GetElementKey(element));
            if (state.MissingDisks > 0) {
                BSGroupWithMissingDisks.insert(key);
            }
            if (state.SpaceProblems > 0) {
                BSGroupWithSpaceProblems.insert(key);
            }
            auto& sp = StoragePoolInfo[element.GetStoragePoolName()];
            sp.Overall = Max(sp.Overall, state.Overall);
            element.ClearOverall();
            element.ClearNodeId();
            element.ClearStoragePoolName();
            BSGroupOverall.emplace(key, NKikimrViewer::EFlag_Name(state.Overall));
            BSGroupIndex.emplace(key, element);
        }
    }

    void ParsePDisksFromBaseConfig() {
        if (BaseConfig) {
            const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(BaseConfig->Record);
            const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
            const NKikimrBlobStorage::TBaseConfig& pbConfig(pbStatus.GetBaseConfig());
            for (const NKikimrBlobStorage::TBaseConfig::TPDisk& pDisk : pbConfig.GetPDisk()) {
                std::pair<ui32, ui32> pDiskKey(pDisk.GetNodeId(), pDisk.GetPDiskId());
                auto itPDisk = PDisksIndex.find(pDiskKey);
                if (itPDisk == PDisksIndex.end()) {
                    PDisksAppended.emplace_back();
                    NKikimrWhiteboard::TPDiskStateInfo& pbPDisk = PDisksAppended.back();
                    itPDisk = PDisksIndex.emplace(pDiskKey, pbPDisk).first;
                    pbPDisk.SetNodeId(pDisk.GetNodeId());
                    pbPDisk.SetPDiskId(pDisk.GetPDiskId());
                    pbPDisk.SetPath(pDisk.GetPath());
                    pbPDisk.SetGuid(pDisk.GetGuid());
                    pbPDisk.SetCategory(static_cast<ui64>(pDisk.GetType()));
                    pbPDisk.SetTotalSize(pDisk.GetPDiskMetrics().GetTotalSize());
                    pbPDisk.SetAvailableSize(pDisk.GetPDiskMetrics().GetAvailableSize());
                }
            }
        }
    }

    void ParseVDisksFromBaseConfig() {
        if (BaseConfig) {
            const NKikimrBlobStorage::TEvControllerConfigResponse& pbRecord(BaseConfig->Record);
            const NKikimrBlobStorage::TConfigResponse::TStatus& pbStatus(pbRecord.GetResponse().GetStatus(0));
            const NKikimrBlobStorage::TBaseConfig& pbConfig(pbStatus.GetBaseConfig());
            for (const NKikimrBlobStorage::TBaseConfig::TVSlot& vDisk : pbConfig.GetVSlot()) {
                NKikimrBlobStorage::TVDiskID vDiskKey;
                vDiskKey.SetGroupID(vDisk.GetGroupId());
                vDiskKey.SetGroupGeneration(vDisk.GetGroupGeneration());
                vDiskKey.SetRing(vDisk.GetFailRealmIdx());
                vDiskKey.SetDomain(vDisk.GetFailDomainIdx());
                vDiskKey.SetVDisk(vDisk.GetVDiskIdx());

                auto itVDisk = VDisksIndex.find(vDiskKey);
                if (itVDisk == VDisksIndex.end()) {
                    VDisksAppended.emplace_back();
                    NKikimrWhiteboard::TVDiskStateInfo& pbVDisk = VDisksAppended.back();
                    itVDisk = VDisksIndex.emplace(vDiskKey, pbVDisk).first;
                    pbVDisk.MutableVDiskId()->CopyFrom(vDiskKey);
                    pbVDisk.SetNodeId(vDisk.GetVSlotId().GetNodeId());
                    pbVDisk.SetPDiskId(vDisk.GetVSlotId().GetPDiskId());
                    pbVDisk.SetAllocatedSize(vDisk.GetVDiskMetrics().GetAllocatedSize());
                }
            }
        }
    }

    void ReplyAndPassAway() override {}

    void HandleTimeout(TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
            case TimeoutBSC:
                break;
            case TimeoutFinal:
                FilterNodeIds.clear();
                break;
        }
        ReplyAndPassAway();
    }
};

}
