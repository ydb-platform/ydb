#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include "viewer.h"
#include "viewer_helper.h"
#include "viewer_tabletinfo.h"
#include "wb_group.h"
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NViewer {

using namespace NProtobufJson;
using namespace NActors;
using namespace NNodeWhiteboard;

enum class ENodeFields : ui8 {
    NodeId,
    NodeInfo,
    SystemState,
    PDisks,
    VDisks,
    Tablets,
    Peers,
    ReversePeers,
    HostName,
    NodeName,
    DC,
    Rack,
    Version,
    Uptime,
    Memory,
    CPU,
    LoadAverage,
    Missing,
    DiskSpaceUsage,
    SubDomainKey,
    DisconnectTime,
    Database,
    HasDisks,
    Connections,
    ConnectStatus,
    SendThroughput,
    ReceiveThroughput,
    NetworkUtilization,
    ClockSkew,
    PingTime,
    COUNT
};

constexpr ui8 operator +(ENodeFields e) {
    return static_cast<ui8>(e);
}

bool operator ==(const NActorsInterconnect::TScopeId& x, const NActorsInterconnect::TScopeId& y) {
    return x.GetX1() == y.GetX1() && x.GetX2() == y.GetX2();
}

class TJsonNodes : public TViewerPipeClient {
    using TThis = TJsonNodes;
    using TBase = TViewerPipeClient;
    using TNodeId = ui32;
    using TPDiskId = std::pair<TNodeId, ui32>;
    using TFieldsType = std::bitset<+ENodeFields::COUNT>;

    enum ENavigateRequest {
        ENavigateRequestDatabase,
        ENavigateRequestResource,
        ENavigateRequestPath,
    };

    enum EBoardInfoRequest {
        EBoardInfoRequestDatabase,
        EBoardInfoRequestResource,
    };

    std::optional<TRequestResponse<TEvInterconnect::TEvNodesInfo>> NodesInfoResponse;
    std::optional<TRequestResponse<TEvWhiteboard::TEvNodeStateResponse>> NodeStateResponse;
    std::optional<TRequestResponse<TEvStateStorage::TEvBoardInfo>> DatabaseBoardInfoResponse;
    std::optional<TRequestResponse<TEvStateStorage::TEvBoardInfo>> ResourceBoardInfoResponse;
    std::optional<TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult>> PathNavigateResponse;
    std::unordered_map<TTabletId, TRequestResponse<TEvHive::TEvResponseHiveNodeStats>> HiveNodeStats;
    bool HiveNodeStatsProcessed = false;
    std::vector<TTabletId> HivesToAsk;
    bool AskHiveAboutPaths = false;

    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse>> StoragePoolsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetGroupsResponse>> GroupsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse>> VSlotsResponse;
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse>> PDisksResponse;

    int WhiteboardStateRequestsInFlight = 0;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvSystemStateResponse>> SystemStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvVDiskStateResponse>> VDiskStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvPDiskStateResponse>> PDiskStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvTabletStateResponse>> TabletStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvNodeStateResponse>> PeersStateResponse;
    std::unordered_map<TNodeId, std::unordered_set<TNodeId>> SystemViewerRequest;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> SystemViewerResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> VDiskViewerResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> PDiskViewerResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> TabletViewerResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> PeersViewerResponse;

    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

    enum ETimeoutTag : ui64 {
        NoTimeout,
        TimeoutTablets,
        TimeoutFinal,
    };

    ETimeoutTag CurrentTimeoutState = NoTimeout;

    TString SharedDatabase;
    bool FilterDatabase = false;
    bool HasDatabaseNodes = false;
    TPathId FilterPathId;
    TSubDomainKey SubDomainKey;
    TSubDomainKey SharedSubDomainKey;
    bool FilterSubDomainKey = false;
    TString FilterPath;
    TString DomainPath;
    std::vector<TString> FilterStoragePools;
    std::vector<std::pair<ui64, ui64>> FilterStoragePoolsIds;
    std::unordered_set<TNodeId> FilterNodeIds;
    std::unordered_set<ui32> FilterGroupIds;
    std::optional<std::size_t> Offset;
    std::optional<std::size_t> Limit;
    int UptimeSeconds = 0;
    bool ProblemNodesOnly = false;
    TString Filter;
    bool AllWhiteboardFields = false;

    enum class EWith {
        Everything,
        MissingDisks,
        SpaceProblems,
    };
    EWith With = EWith::Everything;

    enum class EType {
        Any,
        Static,
        Dynamic,
        Storage,
    };
    EType Type = EType::Any;

    enum class EFilterStorageStage {
        None,
        Pools,
        Groups,
        VSlots,
    };

    EFilterStorageStage FilterStorageStage = EFilterStorageStage::None;
    TNodeId MinAllowedNodeId = std::numeric_limits<TNodeId>::min();
    TNodeId MaxAllowedNodeId = std::numeric_limits<TNodeId>::max();
    std::optional<std::size_t> MaximumDisksPerNode;
    std::optional<std::size_t> MaximumSlotsPerDisk;
    ui32 SpaceUsageProblem = 90; // %
    bool OffloadMerge = true;
    size_t OffloadMergeAttempts = 2;

    using TGroupSortKey = std::variant<TString, ui64, float, int>;

    struct TNode {
        TEvInterconnect::TNodeInfo NodeInfo;
        NKikimrWhiteboard::TSystemStateInfo SystemState;
        std::vector<NKikimrWhiteboard::TPDiskStateInfo> PDisks;
        std::vector<NKikimrSysView::TPDiskEntry> SysViewPDisks;
        std::vector<NKikimrWhiteboard::TVDiskStateInfo> VDisks;
        std::vector<NKikimrSysView::TVSlotEntry> SysViewVDisks;
        std::vector<NKikimrViewer::TTabletStateInfo> Tablets;
        std::vector<NKikimrWhiteboard::TNodeStateInfo> Peers; // information about sessions from this node
        std::vector<NKikimrWhiteboard::TNodeStateInfo> ReversePeers; // information about sessions to this node
        TSubDomainKey SubDomainKey;
        TString Database;
        ui32 MissingDisks = 0;
        float DiskSpaceUsage = 0; // the highest
        float CpuUsage = 0; // total, normalized
        float LoadAverage = 0; // normalized
        bool Problems = false;
        NKikimrWhiteboard::TNodeStateInfo NetworkStateInfo;
        bool Disconnected = false;
        bool HasDisks = false;
        bool GotDatabaseFromDatabaseBoardInfo = false;
        bool GotDatabaseFromResourceBoardInfo = false;
        int UptimeSeconds = 0;
        ui32 Connections = 0;
        ui64 SendThroughput = 0;
        ui64 ReceiveThroughput = 0;
        NKikimrWhiteboard::EFlag ConnectStatus = NKikimrWhiteboard::EFlag::Grey;
        float NetworkUtilization = 0; // Sum
        float NetworkUtilizationMin = 0;
        float NetworkUtilizationMax = 0;
        int64 ClockSkewUs = 0; // Avg
        int64 ClockSkewMinUs = 0;
        int64 ClockSkewMaxUs = 0;
        int64 ReverseClockSkewUs = 0; // Avg
        uint64 PingTimeUs = 0; // Avg
        uint64 PingTimeMinUs = 0;
        uint64 PingTimeMaxUs = 0;
        uint64 ReversePingTimeUs = 0; // Avg

        TNodeId GetNodeId() const {
            return NodeInfo.NodeId;
        }

        TString GetHostName() const {
            if (NodeInfo.Host) {
                return NodeInfo.Host;
            }
            if (SystemState.GetHost()) {
                return SystemState.GetHost();
            }
            if (NodeInfo.ResolveHost) {
                return NodeInfo.ResolveHost;
            }
            return {};
        }

        TString GetNodeName() const {
            return SystemState.GetNodeName();
        }

        TString GetDataCenter() const {
            if (NodeInfo.Location.GetDataCenterId()) {
                return NodeInfo.Location.GetDataCenterId();
            }
            return SystemState.GetLocation().GetDataCenter();
        }

        TString GetRack() const {
            if (NodeInfo.Location.GetRackId()) {
                return NodeInfo.Location.GetRackId();
            }
            return SystemState.GetLocation().GetRack();
        }

        void Cleanup() {
            if (SystemState.HasSystemLocation()) {
                SystemState.ClearSystemLocation();
            }
            if (SystemState.HasLocation()) {
                if (SystemState.GetLocation().GetDataCenter().empty()) {
                    SystemState.MutableLocation()->ClearDataCenter();
                }
                if (SystemState.GetLocation().GetRack().empty()) {
                    SystemState.MutableLocation()->ClearRack();
                }
                if (SystemState.GetLocation().GetUnit().empty() || SystemState.GetLocation().GetUnit() == "0") {
                    SystemState.MutableLocation()->ClearUnit();
                }
            }
        }

        void CalcDatabase() {
            if (SystemState.TenantsSize() == 1) {
                Database = SystemState.GetTenants(0);
            }
        }

        void CalcDisks() {
            MissingDisks = 0;
            DiskSpaceUsage = 0;
            if (!PDisks.empty()) {
                for (const auto& pdisk : PDisks) {
                    float diskSpaceUsage = pdisk.GetTotalSize() ? 100.0 * (pdisk.GetTotalSize() - pdisk.GetAvailableSize()) / pdisk.GetTotalSize() : 0;
                    DiskSpaceUsage = std::max(DiskSpaceUsage, diskSpaceUsage);
                    if (pdisk.state() == NKikimrBlobStorage::TPDiskState::Normal) {
                        continue;
                    }
                    ++MissingDisks;
                }
            } else {
                for (const auto& entry : SysViewPDisks) {
                    const auto& pdisk(entry.GetInfo());
                    float diskSpaceUsage = pdisk.GetTotalSize() ? 100.0 * (pdisk.GetTotalSize() - pdisk.GetAvailableSize()) / pdisk.GetTotalSize() : 0;
                    DiskSpaceUsage = std::max(DiskSpaceUsage, diskSpaceUsage);
                    NKikimrBlobStorage::EDriveStatus driveStatus = NKikimrBlobStorage::EDriveStatus::UNKNOWN;
                    if (NKikimrBlobStorage::EDriveStatus_Parse(pdisk.GetStatusV2(), &driveStatus)) {
                        switch (driveStatus) {
                            case NKikimrBlobStorage::EDriveStatus::ACTIVE:
                            case NKikimrBlobStorage::EDriveStatus::INACTIVE:
                                continue;
                            default:
                                ++MissingDisks;
                                break;
                        }
                    }
                }
            }
        }

        void CalcCpuUsage() {
            float usage = SystemState.GetCoresUsed();
            int threads = SystemState.GetCoresTotal();
            if (threads == 0) {
                for (const auto& pool : SystemState.GetPoolStats()) {
                    ui32 usageThreads = pool.GetLimit() ? pool.GetLimit() : pool.GetThreads();
                    usage += pool.GetUsage() * usageThreads;
                    if (pool.GetName() != "IO") {
                        threads += pool.GetThreads();
                    }
                }
                SystemState.SetCoresUsed(usage);
                SystemState.SetCoresTotal(threads);
            }
            CpuUsage = usage / threads;
        }

        void CalcLoadAverage() {
            if (SystemState.GetNumberOfCpus() && SystemState.LoadAverageSize() > 0) {
                LoadAverage = SystemState.GetLoadAverage(0) / SystemState.GetNumberOfCpus();
            }
        }

        void DisconnectNode() {
            Problems = true;
            Disconnected = true;
            if (!SystemState.HasDisconnectTime()) {
                TInstant disconnectTime;
                for (const auto& entry : SysViewPDisks) {
                    const auto& pdisk(entry.GetInfo());
                    disconnectTime = std::max(disconnectTime, TInstant::MicroSeconds(pdisk.GetStatusChangeTimestamp()));
                }
                if (disconnectTime) {
                    SystemState.SetDisconnectTime(disconnectTime.MilliSeconds());
                }
            }
            CalcUptimeSeconds(TInstant::Now());
        }

        void RemapDisks() {
            if (PDisks.empty() && !SysViewPDisks.empty()) {
                for (const auto& entry : SysViewPDisks) {
                    const auto& pdisk(entry.GetInfo());
                    auto& pDiskState = PDisks.emplace_back();
                    NKikimrBlobStorage::EDriveStatus driveStatus = NKikimrBlobStorage::EDriveStatus::UNKNOWN;
                    if (NKikimrBlobStorage::EDriveStatus_Parse(pdisk.GetStatusV2(), &driveStatus)) {
                        switch (driveStatus) {
                            case NKikimrBlobStorage::EDriveStatus::ACTIVE:
                            case NKikimrBlobStorage::EDriveStatus::INACTIVE:
                                pDiskState.SetState(NKikimrBlobStorage::TPDiskState::Normal);
                                break;
                            default:
                                break;
                        }
                    }

                    pDiskState.SetPDiskId(entry.GetKey().GetPDiskId());
                    pDiskState.SetNodeId(entry.GetKey().GetNodeId());
                    pDiskState.SetPath(pdisk.GetPath());
                    pDiskState.SetGuid(pdisk.GetGuid());
                    pDiskState.SetTotalSize(pdisk.GetTotalSize());
                    pDiskState.SetAvailableSize(pdisk.GetAvailableSize());
                    pDiskState.SetExpectedSlotCount(pdisk.GetExpectedSlotCount());
                }
            }
            if (VDisks.empty() && !SysViewVDisks.empty()) {
                for (const auto& entry : SysViewVDisks) {
                    const auto& vdisk(entry.GetInfo());
                    auto& vDiskState = VDisks.emplace_back();
                    vDiskState.MutableVDiskId()->SetGroupID(vdisk.GetGroupId());
                    vDiskState.MutableVDiskId()->SetGroupGeneration(vdisk.GetGroupGeneration());
                    vDiskState.MutableVDiskId()->SetRing(vdisk.GetFailRealm());
                    vDiskState.MutableVDiskId()->SetDomain(vdisk.GetFailDomain());
                    vDiskState.MutableVDiskId()->SetVDisk(vdisk.GetVDisk());
                    vDiskState.SetNodeId(entry.GetKey().GetNodeId());
                    vDiskState.SetPDiskId(entry.GetKey().GetPDiskId());
                    vDiskState.SetAllocatedSize(vdisk.GetAllocatedSize());
                    vDiskState.SetAvailableSize(vdisk.GetAvailableSize());
                    vDiskState.SetVDiskSlotId(entry.GetKey().GetVSlotId());
                    NKikimrBlobStorage::EVDiskStatus vDiskStatus;
                    if (NKikimrBlobStorage::EVDiskStatus_Parse(vdisk.GetStatusV2(), &vDiskStatus)) {
                        switch(vDiskStatus) {
                            case NKikimrBlobStorage::EVDiskStatus::ERROR:
                                vDiskState.SetVDiskState(NKikimrWhiteboard::EVDiskState::LocalRecoveryError);
                                break;
                            case NKikimrBlobStorage::EVDiskStatus::INIT_PENDING:
                                vDiskState.SetVDiskState(NKikimrWhiteboard::EVDiskState::Initial);
                                break;
                            case NKikimrBlobStorage::EVDiskStatus::REPLICATING:
                                vDiskState.SetVDiskState(NKikimrWhiteboard::EVDiskState::OK);
                                vDiskState.SetReplicated(false);
                                break;
                            case NKikimrBlobStorage::EVDiskStatus::READY:
                                vDiskState.SetVDiskState(NKikimrWhiteboard::EVDiskState::OK);
                                break;
                        }
                    }
                }
            }
        }

        bool IsStatic() const {
            return NodeInfo.IsStatic;
        }

        NKikimrWhiteboard::EFlag GetOverall() const {
            return SystemState.GetSystemState();
        }

        int GetCandidateScore() const {
            int score = 0;
            if (NetworkStateInfo.GetConnected() && NetworkStateInfo.GetConnectStatus() != NKikimrWhiteboard::EFlag::Red) {
                score += 10000; // because already connected node is always preferable
            }
            if (NetworkStateInfo.GetConnectStatus() != NKikimrWhiteboard::EFlag::Grey && NetworkStateInfo.GetConnectStatus() != NKikimrWhiteboard::EFlag::Green) {
                score -= 3000 * static_cast<int>(NetworkStateInfo.GetConnectStatus()); // connection state is important
            }
            score -= NetworkStateInfo.GetPingTimeUs(); // lower ping is better
            if (IsStatic()) {
                score += 10000; // static nodes are always preferable too
            }
            if (NetworkStateInfo.GetSessionState() == NKikimrWhiteboard::TNodeStateInfo_ESessionState_PENDING_CONNECTION) {
                score -= 100000; // avoid pending connections
            }
            return score;
        }

        TString GetDiskUsageForGroup() const {
            //return TStringBuilder() << std::ceil(std::clamp<float>(DiskSpaceUsage, 0, 100) / 5) * 5 << '%';
            // we want 0%-95% groups instead of 5%-100% groups
            return TStringBuilder() << std::floor(std::clamp<float>(DiskSpaceUsage, 0, 100) / 5) * 5 << '%';
        }

        TString GetNetworkUtilizationForGroup() const {
            //return TStringBuilder() << std::ceil(std::clamp<float>(NetworkUtilization, 0, 100) / 5) * 5 << '%';
            // we want 0%-95% groups instead of 5%-100% groups
            return TStringBuilder() << std::floor(std::clamp<float>(NetworkUtilization, 0, 100) / 5) * 5 << '%';
        }

        TInstant GetStartTime() const {
            return TInstant::MilliSeconds(SystemState.GetStartTime());
        }

        TInstant GetDisconnectTime() const {
            return TInstant::MilliSeconds(SystemState.GetDisconnectTime());
        }

        int GetUptimeSeconds(TInstant now) const {
            if (Disconnected) {
                return static_cast<int>(GetDisconnectTime().Seconds()) - static_cast<int>(now.Seconds()); // negative for disconnected nodes
            } else {
                return static_cast<int>(now.Seconds()) - static_cast<int>(GetStartTime().Seconds());
            }
        }

        void CalcUptimeSeconds(TInstant now) {
            UptimeSeconds = GetUptimeSeconds(now);
        }

        void CalcPeers() {
            Connections = 0;
            SendThroughput = 0;
            ReceiveThroughput = 0;
            std::array<int, NKikimrWhiteboard::EFlag_ARRAYSIZE> connectStatuses = {};
            ConnectStatus = NKikimrWhiteboard::EFlag::Grey;
            NetworkUtilization = 0;
            NetworkUtilizationMin = 0;
            NetworkUtilizationMax = 0;
            ClockSkewUs = 0;
            ClockSkewMinUs = 0;
            ClockSkewMaxUs = 0;
            ReverseClockSkewUs = 0;
            PingTimeUs = 0;
            PingTimeMinUs = 0;
            PingTimeMaxUs = 0;
            ReversePingTimeUs = 0;
            if (!Peers.empty()) {
                NetworkUtilizationMin = NetworkUtilizationMax = Peers.front().GetUtilization();
                ClockSkewMinUs = ClockSkewMaxUs = Peers.front().GetClockSkewUs();
                PingTimeMinUs = PingTimeMaxUs = Peers.front().GetPingTimeUs();
            }
            for (const auto& peer : Peers) {
                NKikimrWhiteboard::EFlag connectStatus = peer.GetConnected() ? peer.GetConnectStatus() : NKikimrWhiteboard::EFlag::Grey;
                connectStatuses[connectStatus]++;
                if (peer.GetConnected() && peer.GetConnectStatus() != NKikimrWhiteboard::EFlag::Red && peer.GetSessionState() != NKikimrWhiteboard::TNodeStateInfo_ESessionState_PENDING_CONNECTION) {
                    ++Connections;
                }
                SendThroughput += peer.GetWriteThroughput();
                NetworkUtilization += peer.GetUtilization();
                NetworkUtilizationMin = std::min(NetworkUtilizationMin, peer.GetUtilization());
                NetworkUtilizationMax = std::max(NetworkUtilizationMax, peer.GetUtilization());
                ClockSkewUs += peer.GetClockSkewUs();
                ClockSkewMinUs = std::min(ClockSkewMinUs, peer.GetClockSkewUs());
                ClockSkewMaxUs = std::max(ClockSkewMaxUs, peer.GetClockSkewUs());
                PingTimeUs += peer.GetPingTimeUs();
                PingTimeMinUs = std::min(PingTimeMinUs, peer.GetPingTimeUs());
                PingTimeMaxUs = std::max(PingTimeMaxUs, peer.GetPingTimeUs());
            }
            if (!Peers.empty()) {
                // NetworkUtilization /= Peers.size(); // alexvru suggests to use sum instead of average
                ClockSkewUs = ClockSkewUs / static_cast<i64>(Peers.size());
                PingTimeUs = PingTimeUs / Peers.size();
            }
            int percent5 = Peers.size() / 20;
            for (int i = 0; i < NKikimrWhiteboard::EFlag_ARRAYSIZE; ++i) {
                if (connectStatuses[i] > percent5) {
                    ConnectStatus = static_cast<NKikimrWhiteboard::EFlag>(i);
                }
            }
            for (const auto& peer : ReversePeers) {
                ReceiveThroughput += peer.GetWriteThroughput();
                ReverseClockSkewUs += peer.GetClockSkewUs();
                ReversePingTimeUs += peer.GetPingTimeUs();
            }
            if (!ReversePeers.empty()) {
                ReverseClockSkewUs = ReverseClockSkewUs / static_cast<i64>(ReversePeers.size());
                ReversePingTimeUs = ReversePingTimeUs / ReversePeers.size();
            }
        }

        TString GetUptimeForGroup() const {
            if (!Disconnected && UptimeSeconds >= 0) {
                if (UptimeSeconds < 60 * 10) {
                    return "up <10m";
                }
                if (UptimeSeconds < 60 * 60) {
                    return "up <1h";
                }
                if (UptimeSeconds < 60 * 60 * 24) {
                    return "up <24h";
                }
                if (UptimeSeconds < 60 * 60 * 24 * 7) {
                    return "up 24h+";
                }
                return "up 1 week+";
            } else {
                if (SystemState.HasDisconnectTime()) {
                    if (UptimeSeconds > -60 * 10) {
                        return "down <10m";
                    }
                    if (UptimeSeconds > -60 * 60) {
                        return "down <1h";
                    }
                    if (UptimeSeconds > -60 * 60 * 24) {
                        return "down <24h";
                    }
                    if (UptimeSeconds > -60 * 60 * 24 * 7) {
                        return "down 24h+";
                    }
                    return "down 1 week+";
                } else {
                    return "disconnected";
                }
            }
        }

        TString GetVersionForGroup() const {
            if (SystemState.HasVersion()) {
                return SystemState.GetVersion();
            } else {
                return "unknown";
            }
        }

        TString GetClockSkewForGroup() const {
            auto clockSkew = abs(ClockSkewUs) / 1000;
            if (clockSkew < 1) {
                return "<1ms";
            }
            if (clockSkew < 10) {
                return "1ms..10ms";
            }
            return "10ms+";
        }

        TString GetPingTimeForGroup() const {
            if (PingTimeUs < 1000) {
                return "<1ms";
            }
            if (PingTimeUs < 10000) {
                return "1ms..10ms";
            }
            return "10ms+";
        }

        bool HasDatabase(const TString& database) const {
            return Database == database;
        }

        bool HasSubDomainKey(const TSubDomainKey& subDomainKey) const {
            return SubDomainKey == subDomainKey;
        }

        TString GetGroupName(ENodeFields groupBy) const {
            TString groupName;
            switch (groupBy) {
                case ENodeFields::NodeId:
                    groupName = ToString(GetNodeId());
                    break;
                case ENodeFields::HostName:
                    groupName = GetHostName();
                    break;
                case ENodeFields::NodeName:
                    groupName = GetNodeName();
                    break;
                case ENodeFields::Database:
                    groupName = Database;
                    break;
                case ENodeFields::DiskSpaceUsage:
                    groupName = GetDiskUsageForGroup();
                    break;
                case ENodeFields::DC:
                    groupName = GetDataCenter();
                    break;
                case ENodeFields::Rack:
                    groupName = GetRack();
                    break;
                case ENodeFields::Missing:
                    groupName = ToString(MissingDisks);
                    break;
                case ENodeFields::Uptime:
                    groupName = GetUptimeForGroup();
                    break;
                case ENodeFields::Version:
                    groupName = GetVersionForGroup();
                    break;
                case ENodeFields::SystemState:
                    groupName = NKikimrWhiteboard::EFlag_Name(GetOverall());
                    break;
                case ENodeFields::ConnectStatus:
                    groupName = NKikimrWhiteboard::EFlag_Name(ConnectStatus);
                    break;
                case ENodeFields::NetworkUtilization:
                    groupName = GetNetworkUtilizationForGroup();
                    break;
                case ENodeFields::ClockSkew:
                    groupName = GetClockSkewForGroup();
                    break;
                case ENodeFields::PingTime:
                    groupName = GetPingTimeForGroup();
                    break;
                default:
                    break;
            }
            if (groupName.empty()) {
                groupName = "unknown";
            }
            return groupName;
        }

        TGroupSortKey GetGroupSortKey(ENodeFields groupBy) const {
            switch (groupBy) {
                case ENodeFields::NodeId:
                case ENodeFields::HostName:
                case ENodeFields::NodeName:
                case ENodeFields::Database:
                case ENodeFields::DC:
                case ENodeFields::Rack:
                case ENodeFields::Version:
                    return GetGroupName(groupBy);
                case ENodeFields::DiskSpaceUsage:
                    return DiskSpaceUsage;
                case ENodeFields::Missing:
                    return MissingDisks;
                case ENodeFields::Uptime:
                    return UptimeSeconds;
                case ENodeFields::SystemState:
                    return static_cast<int>(GetOverall());
                case ENodeFields::ConnectStatus:
                    return static_cast<int>(ConnectStatus);
                case ENodeFields::NetworkUtilization:
                    return NetworkUtilization;
                case ENodeFields::ClockSkew:
                    return static_cast<int>(abs(ClockSkewUs) / 1000);
                case ENodeFields::PingTime:
                    return PingTimeUs;
                default:
                    return TString();
            }
        }

        void MergeFrom(const NKikimrWhiteboard::TSystemStateInfo& systemState, TInstant now) {
            SystemState.MergeFrom(systemState);
            Cleanup();
            CalcDatabase();
            CalcCpuUsage();
            CalcLoadAverage();
            CalcUptimeSeconds(now);
        }
    };

    struct TNodeBatch {
        std::vector<TNode*> NodesToAskFor;
        std::vector<TNode*> NodesToAskAbout;
        size_t Offset = 0;
        bool HasStaticNodes = false;

        TNodeId ChooseNodeId() {
            if (Offset >= NodesToAskFor.size()) {
                return 0;
            }
            return NodesToAskFor[Offset++]->GetNodeId();
        }
    };

    using TNodeData = std::vector<TNode>;
    using TNodeView = std::deque<TNode*>;

    struct TNodeGroup {
        TString Name;
        TGroupSortKey SortKey;
        TNodeView Nodes;
    };

    TNodeData NodeData;
    TNodeView NodeView;
    std::vector<TNodeGroup> NodeGroups;
    std::unordered_map<TNodeId, TNode*> NodesByNodeId;
    std::unordered_map<TNodeId, TNodeBatch> NodeBatches;

    TFieldsType FieldsRequired;
    TFieldsType FieldsAvailable;
    const TFieldsType FieldsAll = TFieldsType().set();
    const TFieldsType FieldsNodeInfo = TFieldsType().set(+ENodeFields::NodeInfo)
                                                    .set(+ENodeFields::NodeId)
                                                    .set(+ENodeFields::HostName)
                                                    .set(+ENodeFields::DC)
                                                    .set(+ENodeFields::Rack);
    const TFieldsType FieldsSystemState = TFieldsType().set(+ENodeFields::SystemState)
                                                       .set(+ENodeFields::Database)
                                                       .set(+ENodeFields::NodeName)
                                                       .set(+ENodeFields::Version)
                                                       .set(+ENodeFields::Uptime)
                                                       .set(+ENodeFields::Memory)
                                                       .set(+ENodeFields::CPU)
                                                       .set(+ENodeFields::LoadAverage);
    const TFieldsType FieldsPDisks = TFieldsType().set(+ENodeFields::PDisks)
                                                  .set(+ENodeFields::Missing)
                                                  .set(+ENodeFields::DiskSpaceUsage);
    const TFieldsType FieldsVDisks = TFieldsType().set(+ENodeFields::VDisks);
    const TFieldsType FieldsTablets = TFieldsType().set(+ENodeFields::Tablets);
    const TFieldsType FieldsHiveNodeStat = TFieldsType().set(+ENodeFields::SubDomainKey)
                                                        .set(+ENodeFields::DisconnectTime);

    const TFieldsType FieldsPeers = TFieldsType().set(+ENodeFields::Peers)
                                                 .set(+ENodeFields::SendThroughput)
                                                 .set(+ENodeFields::Connections)
                                                 .set(+ENodeFields::ConnectStatus)
                                                 .set(+ENodeFields::NetworkUtilization)
                                                 .set(+ENodeFields::PingTime)
                                                 .set(+ENodeFields::ClockSkew);

    const TFieldsType FieldsReversePeers = TFieldsType().set(+ENodeFields::ReversePeers)
                                                        .set(+ENodeFields::ReceiveThroughput);

    const std::unordered_map<ENodeFields, TFieldsType> DependentFields = {
        { ENodeFields::DC, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Rack, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Uptime, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Version, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::NodeName, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::CPU, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Memory, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::LoadAverage, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Database, TFieldsType().set(+ENodeFields::SystemState) },
        { ENodeFields::Missing, TFieldsType().set(+ENodeFields::PDisks) },
        { ENodeFields::SendThroughput, TFieldsType().set(+ENodeFields::Peers) },
        { ENodeFields::ReceiveThroughput, TFieldsType().set(+ENodeFields::ReversePeers) },
        { ENodeFields::ReversePeers, TFieldsType().set(+ENodeFields::Peers) },
        { ENodeFields::Connections, TFieldsType().set(+ENodeFields::Peers) },
        { ENodeFields::ConnectStatus, TFieldsType().set(+ENodeFields::Peers) },
        { ENodeFields::NetworkUtilization, TFieldsType().set(+ENodeFields::Peers) },
        { ENodeFields::PingTime, TFieldsType().set(+ENodeFields::Peers) },
        { ENodeFields::ClockSkew, TFieldsType().set(+ENodeFields::Peers) },
    };

    bool FieldsNeeded(TFieldsType fields) const {
        return (FieldsRequired & (fields & ~FieldsAvailable)).any();
    }

    ENodeFields SortBy = ENodeFields::NodeId;
    bool ReverseSort = false;
    ENodeFields GroupBy = ENodeFields::NodeId;
    ENodeFields FilterGroupBy = ENodeFields::NodeId;
    TString FilterGroup;
    bool NeedFilter = false;
    bool NeedGroup = false;
    bool NeedSort = false;
    bool NeedLimit = false;
    ui64 TotalNodes = 0;
    ui64 FoundNodes = 0;
    bool NoRack = false;
    bool NoDC = false;
    std::vector<TString> Problems;

    void AddProblem(const TString& problem) {
        for (const auto& p : Problems) {
            if (p == problem) {
                return;
            }
        }
        Problems.push_back(problem);
    }

    static ENodeFields ParseENodeFields(TStringBuf field) {
        ENodeFields result = ENodeFields::COUNT;
        if (field == "NodeId" || field == "Id") {
            result = ENodeFields::NodeId;
        } else if (field == "Host" || field == "HostName") {
            result = ENodeFields::HostName;
        } else if (field == "NodeName") {
            result = ENodeFields::NodeName;
        } else if (field == "DC") {
            result = ENodeFields::DC;
        } else if (field == "Rack") {
            result = ENodeFields::Rack;
        } else if (field == "Version") {
            result = ENodeFields::Version;
        } else if (field == "Uptime") {
            result = ENodeFields::Uptime;
        } else if (field == "Memory") {
            result = ENodeFields::Memory;
        } else if (field == "CPU") {
            result = ENodeFields::CPU;
        } else if (field == "LoadAverage") {
            result = ENodeFields::LoadAverage;
        } else if (field == "Missing") {
            result = ENodeFields::Missing;
        } else if (field == "DiskSpaceUsage") {
            result = ENodeFields::DiskSpaceUsage;
        } else if (field == "DisconnectTime") {
            result = ENodeFields::DisconnectTime;
        } else if (field == "Database") {
            result = ENodeFields::Database;
        } else if (field == "SubDomainKey") {
            result = ENodeFields::SubDomainKey;
        } else if (field == "SystemState") {
            result = ENodeFields::SystemState;
        } else if (field == "PDisks") {
            result = ENodeFields::PDisks;
        } else if (field == "VDisks") {
            result = ENodeFields::VDisks;
        } else if (field == "Tablets") {
            result = ENodeFields::Tablets;
        } else if (field == "Peers") {
            result = ENodeFields::Peers;
        } else if (field == "Connections") {
            result = ENodeFields::Connections;
        } else if (field == "SendThroughput") {
            result = ENodeFields::SendThroughput;
        } else if (field == "ReceiveThroughput") {
            result = ENodeFields::ReceiveThroughput;
        } else if (field == "ReversePeers") {
            result = ENodeFields::ReversePeers;
        } else if (field == "ConnectStatus") {
            result = ENodeFields::ConnectStatus;
        } else if (field == "NetworkUtilization") {
            result = ENodeFields::NetworkUtilization;
        } else if (field == "PingTime") {
            result = ENodeFields::PingTime;
        } else if (field == "ClockSkew") {
            result = ENodeFields::ClockSkew;
        }
        return result;
    }

public:
    TJsonNodes(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev, "/viewer/nodes")
    {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        InitConfig(params);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        FieldsRequired.set(+ENodeFields::NodeId);
        UptimeSeconds = FromStringWithDefault<int>(params.Get("uptime"), 0);
        ProblemNodesOnly = FromStringWithDefault<bool>(params.Get("problems_only"), ProblemNodesOnly);
        Filter = params.Get("filter");
        if (UptimeSeconds || ProblemNodesOnly || !Filter.empty()) {
            FieldsRequired.set(+ENodeFields::SystemState);
        }
        FilterPath = params.Get("path");
        if (FilterPath && !Database) {
            Database = FilterPath;
        }
        if (Database) {
            FilterDatabase = true;
        }
        if (FilterPath == Database) {
            FilterPath.clear();
        }
        if (params.Has("filter_group") && params.Has("filter_group_by")) {
            FilterGroup = params.Get("filter_group");
            FilterGroupBy = ParseENodeFields(params.Get("filter_group_by"));
            FieldsRequired.set(+FilterGroupBy);
        }

        OffloadMerge = FromStringWithDefault<bool>(params.Get("offload_merge"), OffloadMerge);
        OffloadMergeAttempts = FromStringWithDefault<bool>(params.Get("offload_merge_attempts"), OffloadMergeAttempts);
        Direct = FromStringWithDefault<bool>(params.Get("direct"), Direct);
        TString filterStoragePool = params.Get("pool");
        if (filterStoragePool.empty()) {
            filterStoragePool = params.Get("storage_pool");
        }
        if (!filterStoragePool.empty()) {
            FilterStoragePools.emplace_back(filterStoragePool);
        }
        if (params.Has("group_id")) {
            FilterGroupIds.insert(FromStringWithDefault<ui32>(params.Get("group_id"), -1));
        }
        SplitIds(params.Get("node_id"), ',', FilterNodeIds);
        auto itZero = FilterNodeIds.find(0);
        if (itZero != FilterNodeIds.end()) {
            FilterNodeIds.erase(itZero);
            FilterNodeIds.insert(TlsActivationContext->ActorSystem()->NodeId);
        }
        if (params.Get("with") == "missing") {
            With = EWith::MissingDisks;
            FieldsRequired.set(+ENodeFields::Missing);
        } else if (params.Get("with") == "space") {
            With = EWith::SpaceProblems;
            FieldsRequired.set(+ENodeFields::DiskSpaceUsage);
        }
        if (params.Get("type") == "static") {
            Type = EType::Static;
            FieldsRequired.set(+ENodeFields::NodeInfo);
        } else if (params.Get("type") == "dynamic") {
            Type = EType::Dynamic;
            FieldsRequired.set(+ENodeFields::NodeInfo);
        } else if (params.Get("type") == "storage") {
            Type = EType::Storage;
            FieldsRequired.set(+ENodeFields::NodeInfo);
        } else if (params.Get("type") == "any") {
            Type = EType::Any;
        }
        NeedFilter = (With != EWith::Everything) || (Type != EType::Any) || !Filter.empty() || !FilterNodeIds.empty() || ProblemNodesOnly || UptimeSeconds > 0 || !FilterGroup.empty();
        if (params.Has("offset")) {
            Offset = FromStringWithDefault<ui32>(params.Get("offset"), 0);
            NeedLimit = true;
        }
        if (params.Has("limit")) {
            Limit = FromStringWithDefault<ui32>(params.Get("limit"), std::numeric_limits<ui32>::max());
            NeedLimit = true;
        }
        if (FromStringWithDefault<bool>(params.Get("storage"))) {
            FieldsRequired.set(+ENodeFields::PDisks);
            FieldsRequired.set(+ENodeFields::VDisks);
        }
        if (FromStringWithDefault<bool>(params.Get("tablets"))) {
            FieldsRequired.set(+ENodeFields::Tablets);
        }
        TStringBuf sort = params.Get("sort");
        if (sort) {
            NeedSort = true;
            if (sort.StartsWith("-") || sort.StartsWith("+")) {
                ReverseSort = (sort[0] == '-');
                sort.Skip(1);
            }
            SortBy = ParseENodeFields(sort);
            FieldsRequired.set(+SortBy);
        }
        TString fieldsRequired = params.Get("fields_required");
        if (!fieldsRequired.empty()) {
            if (fieldsRequired == "all") {
                FieldsRequired = FieldsAll;
            } else {
                TStringBuf source = fieldsRequired;
                for (TStringBuf value = source.NextTok(','); !value.empty(); value = source.NextTok(',')) {
                    ENodeFields field = ParseENodeFields(value);
                    if (field != ENodeFields::COUNT) {
                        FieldsRequired.set(+field);
                    }
                }
            }
        } else {
            FieldsRequired.set(+ENodeFields::SystemState);
        }
        TStringBuf group = params.Get("group");
        if (group) {
            NeedGroup = true;
            GroupBy = ParseENodeFields(group);
            FieldsRequired.set(+GroupBy);
            NeedSort = false;
            NeedLimit = false;
        }
        for (auto field = +ENodeFields::NodeId; field != +ENodeFields::COUNT; ++field) {
            if (FieldsRequired.test(field)) {
                auto itDependentFields = DependentFields.find(static_cast<ENodeFields>(field));
                if (itDependentFields != DependentFields.end()) {
                    FieldsRequired |= itDependentFields->second;
                }
            }
        }
        if (FromStringWithDefault<bool>(params.Get("all_whiteboard_fields"), false)) {
            AllWhiteboardFields = true;
        }
    }

    void Bootstrap() override {
        if (TBase::NeedToRedirect()) {
            return;
        }

        NodesInfoResponse = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        {
            auto request = std::make_unique<TEvWhiteboard::TEvNodeStateRequest>();
            request->Record.AddFieldsRequired(-1);
            NodeStateResponse = MakeWhiteboardRequest(TActivationContext::ActorSystem()->NodeId, request.release());
        }
        if (!FilterStoragePools.empty() || !FilterGroupIds.empty()) {
            FilterDatabase = false; // we disable database filter if we're filtering by pool or group
        }
        if (FilterDatabase) {
            if (!DatabaseNavigateResponse) {
                DatabaseNavigateResponse = MakeRequestSchemeCacheNavigate(Database, ENavigateRequestDatabase);
            }
            if (!FieldsNeeded(FieldsHiveNodeStat) && !(FilterPath && FieldsNeeded(FieldsTablets))) {
                DatabaseBoardInfoResponse = MakeRequestStateStorageEndpointsLookup(Database, EBoardInfoRequestDatabase);
            }
            if ((Type == EType::Storage || Type == EType::Static) && FilterStoragePools.empty() && FilterGroupIds.empty()) {
                FilterStorageStage = EFilterStorageStage::Pools;
                FilterDatabase = false;
            }
        }
        if (FilterPath && FieldsNeeded(FieldsTablets)) {
            PathNavigateResponse = MakeRequestSchemeCacheNavigate(FilterPath, ENavigateRequestPath);
        }
        if (!FilterStoragePools.empty()) {
            StoragePoolsResponse = RequestBSControllerPools();
            GroupsResponse = RequestBSControllerGroups();
            VSlotsResponse = RequestBSControllerVSlots();
            FilterStorageStage = EFilterStorageStage::Pools;
        } else if (!FilterGroupIds.empty()) {
            VSlotsResponse = RequestBSControllerVSlots();
            FilterStorageStage = EFilterStorageStage::VSlots;
        }
        if (With != EWith::Everything) {
            PDisksResponse = RequestBSControllerPDisks();
        }
        TIntrusivePtr<TDomainsInfo> domains = AppData()->DomainsInfo;
        auto* domain = domains->GetDomain();
        DomainPath = "/" + domain->Name;
        if (ProblemNodesOnly || GroupBy == ENodeFields::Uptime) {
            FieldsRequired.set(+ENodeFields::SystemState);
            TTabletId rootHiveId = domains->GetHive();
            HivesToAsk.push_back(rootHiveId);
            if (!PDisksResponse) {
                PDisksResponse = RequestBSControllerPDisks();
            }
        }
        if (FieldsRequired.test(+ENodeFields::PDisks)) {
            if (!PDisksResponse) {
                PDisksResponse = RequestBSControllerPDisks();
            }
        }
        if (FieldsRequired.test(+ENodeFields::VDisks)) {
            if (!VSlotsResponse) {
                VSlotsResponse = RequestBSControllerVSlots();
            }
        }
        if (FieldsNeeded(FieldsHiveNodeStat) && !FilterDatabase && !FilterPath) {
            TTabletId rootHiveId = domains->GetHive();
            HivesToAsk.push_back(rootHiveId);
        }
        Schedule(TDuration::MilliSeconds(Timeout * 50 / 100), new TEvents::TEvWakeup(TimeoutTablets)); // 50% timeout (for tablets)
        TBase::Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup(TimeoutFinal));
    }

    void InvalidateNodes() {
        NodesByNodeId.clear();
    }

    void RebuildNodesByNodeId() {
        NodesByNodeId.clear();
        for (TNode* node : NodeView) {
            NodesByNodeId.emplace(node->GetNodeId(), node);
        }
    }

    TNode* FindNode(TNodeId nodeId) {
        if (NodesByNodeId.empty()) {
            RebuildNodesByNodeId();
        }
        auto itNode = NodesByNodeId.find(nodeId);
        if (itNode != NodesByNodeId.end()) {
            return itNode->second;
        }
        return nullptr;
    }

    bool PreFilterDone() const {
        return !FilterDatabase && FilterStorageStage == EFilterStorageStage::None;
    }

    bool FilterDone() const {
        return PreFilterDone() && !NeedFilter;
    }

    void ApplyFilter() {
        // database pre-filter, affects TotalNodes count
        if (FilterDatabase) {
            if (FilterSubDomainKey && FieldsAvailable.test(+ENodeFields::SubDomainKey)) {
                TNodeView nodeView;
                if (HasDatabaseNodes) {
                    for (TNode* node : NodeView) {
                        if (node->HasSubDomainKey(SubDomainKey)) {
                            nodeView.push_back(node);
                        }
                    }
                } else {
                    for (TNode* node : NodeView) {
                        if (node->HasSubDomainKey(SharedSubDomainKey)) {
                            nodeView.push_back(node);
                        }
                    }
                }
                NodeView.swap(nodeView);
                FoundNodes = TotalNodes = NodeView.size();
                InvalidateNodes();
                FilterDatabase = false;
                AddEvent("PreFilter Applied");
            } else if (FieldsAvailable.test(+ENodeFields::Database)) {
                TNodeView nodeView;
                if (HasDatabaseNodes) {
                    for (TNode* node : NodeView) {
                        if (node->HasDatabase(Database)) {
                            nodeView.push_back(node);
                        }
                    }
                } else {
                    for (TNode* node : NodeView) {
                        if (node->HasDatabase(SharedDatabase)) {
                            nodeView.push_back(node);
                        }
                    }
                }
                NodeView.swap(nodeView);
                FoundNodes = TotalNodes = NodeView.size();
                InvalidateNodes();
                FilterDatabase = false;
                AddEvent("PreFilter Applied");
            } else {
                return;
            }
        }
        // storage/nodes pre-filter, affects TotalNodes count
        if (FilterStorageStage != EFilterStorageStage::None) {
            return;
        }
        if (((Type == EType::Static || Type == EType::Dynamic) && FieldsAvailable.test(+ENodeFields::NodeInfo)) || (Type == EType::Storage && FieldsAvailable.test(+ENodeFields::HasDisks))) {
            TNodeView nodeView;
            switch (Type) {
                case EType::Static:
                    for (TNode* node : NodeView) {
                        if (node->IsStatic()) {
                            nodeView.push_back(node);
                        }
                    }
                    break;
                case EType::Dynamic:
                    for (TNode* node : NodeView) {
                        if (!node->IsStatic()) {
                            nodeView.push_back(node);
                        }
                    }
                    break;
                case EType::Storage:
                    for (TNode* node : NodeView) {
                        if (node->HasDisks) {
                            nodeView.push_back(node);
                        }
                    }
                    break;
                case EType::Any:
                    break;
            }
            NodeView.swap(nodeView);
            FoundNodes = TotalNodes = NodeView.size();
            Type = EType::Any;
            InvalidateNodes();
            AddEvent("Type Filter Applied");
        }
        // storage/nodes pre-filter, affects TotalNodes count
        if (Type != EType::Any) {
            return;
        }
        if (!FilterNodeIds.empty() && FieldsAvailable.test(+ENodeFields::NodeId)) {
            TNodeView nodeView;
            for (TNode* node : NodeView) {
                if (FilterNodeIds.count(node->GetNodeId()) > 0) {
                    nodeView.push_back(node);
                }
            }
            NodeView.swap(nodeView);
            FoundNodes = TotalNodes = NodeView.size();
            InvalidateNodes();
            FilterNodeIds.clear();
            AddEvent("Id Filter Applied");
        }
        if (NeedFilter) {
            if (With == EWith::MissingDisks && FieldsAvailable.test(+ENodeFields::Missing)) {
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if (node->MissingDisks != 0) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                With = EWith::Everything;
                InvalidateNodes();
                AddEvent("Missing Filter Applied");
            }
            if (With == EWith::SpaceProblems && FieldsAvailable.test(+ENodeFields::DiskSpaceUsage)) {
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if (node->DiskSpaceUsage >= SpaceUsageProblem) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                With = EWith::Everything;
                InvalidateNodes();
                AddEvent("Space Filter Applied");
            }
            if (ProblemNodesOnly && FieldsAvailable.test(+ENodeFields::SystemState)) {
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if (node->GetOverall() != NKikimrWhiteboard::EFlag::Green) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                ProblemNodesOnly = false;
                InvalidateNodes();
                AddEvent("Problem Filter Applied");
            }
            if (UptimeSeconds > 0 && FieldsAvailable.test(+ENodeFields::SystemState)) {
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if (node->UptimeSeconds < UptimeSeconds) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                UptimeSeconds = 0;
                InvalidateNodes();
                AddEvent("Uptime Filter Applied");
            }
            if (!Filter.empty()) {
                bool allFieldsPresent =
                    (!FieldsRequired.test(+ENodeFields::NodeId) || FieldsAvailable.test(+ENodeFields::NodeId)) &&
                    (!FieldsRequired.test(+ENodeFields::HostName) || FieldsAvailable.test(+ENodeFields::HostName)) &&
                    (!FieldsRequired.test(+ENodeFields::NodeName) || FieldsAvailable.test(+ENodeFields::NodeName));
                if (allFieldsPresent) {
                    TVector<TString> filterWords = SplitString(Filter, " ");
                    TNodeView nodeView;
                    for (TNode* node : NodeView) {
                        for (const TString& word : filterWords) {
                            if (FieldsRequired.test(+ENodeFields::NodeId) && ::ToString(node->GetNodeId()).Contains(word)) {
                                nodeView.push_back(node);
                                break;
                            }
                            if (FieldsRequired.test(+ENodeFields::HostName) && node->GetHostName().Contains(word)) {
                                nodeView.push_back(node);
                                break;
                            }
                            if (FieldsRequired.test(+ENodeFields::NodeName) && node->GetNodeName().Contains(word)) {
                                nodeView.push_back(node);
                                break;
                            }
                        }
                    }
                    NodeView.swap(nodeView);
                    Filter.clear();
                    InvalidateNodes();
                    AddEvent("Search Filter Applied");
                }
            }
            if (!FilterGroup.empty() && FieldsAvailable.test(+FilterGroupBy)) {
                TNodeView nodeView;
                for (TNode* node : NodeView) {
                    if (node->GetGroupName(FilterGroupBy) == FilterGroup) {
                        nodeView.push_back(node);
                    }
                }
                NodeView.swap(nodeView);
                FilterGroup.clear();
                InvalidateNodes();
                AddEvent("Group Filter Applied");
            }
            NeedFilter = (With != EWith::Everything) || (Type != EType::Any) || !Filter.empty() || !FilterNodeIds.empty() || ProblemNodesOnly || UptimeSeconds > 0 || !FilterGroup.empty();
            FoundNodes = NodeView.size();
        }
    }

    void GroupCollection() {
        std::unordered_map<TString, size_t> nodeGroups;
        NodeGroups.clear();
        for (TNode* node : NodeView) {
            auto gb = node->GetGroupName(GroupBy);
            TNodeGroup* nodeGroup = nullptr;
            auto it = nodeGroups.find(gb);
            if (it == nodeGroups.end()) {
                nodeGroups.emplace(gb, NodeGroups.size());
                nodeGroup = &NodeGroups.emplace_back();
                nodeGroup->Name = gb;
                nodeGroup->SortKey = node->GetGroupSortKey(GroupBy);
            } else {
                nodeGroup = &NodeGroups[it->second];
            }
            nodeGroup->Nodes.push_back(node);
        }
    }

    void ApplyGroup() {
        if (FilterDone() && NeedGroup && FieldsAvailable.test(+GroupBy)) {
            switch (GroupBy) {
                case ENodeFields::NodeId:
                case ENodeFields::HostName:
                case ENodeFields::NodeName:
                case ENodeFields::Database:
                case ENodeFields::DC:
                case ENodeFields::Rack:
                case ENodeFields::Uptime:
                    GroupCollection();
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.SortKey; });
                    NeedGroup = false;
                    break;
                case ENodeFields::DiskSpaceUsage:
                case ENodeFields::Missing:
                case ENodeFields::Version:
                case ENodeFields::SystemState:
                case ENodeFields::ConnectStatus:
                case ENodeFields::NetworkUtilization:
                case ENodeFields::ClockSkew:
                case ENodeFields::PingTime:
                    GroupCollection();
                    SortCollection(NodeGroups, [](const TNodeGroup& nodeGroup) { return nodeGroup.SortKey; }, true);
                    NeedGroup = false;
                    break;
                case ENodeFields::NodeInfo:
                case ENodeFields::PDisks:
                case ENodeFields::VDisks:
                case ENodeFields::Tablets:
                case ENodeFields::Peers:
                case ENodeFields::SubDomainKey:
                case ENodeFields::COUNT:
                case ENodeFields::Memory:
                case ENodeFields::CPU:
                case ENodeFields::LoadAverage:
                case ENodeFields::DisconnectTime:
                case ENodeFields::HasDisks:
                case ENodeFields::Connections:
                case ENodeFields::ReceiveThroughput:
                case ENodeFields::SendThroughput:
                case ENodeFields::ReversePeers:
                    break;
            }
            AddEvent("Group Applied");
        }
    }

    void ApplySort() {
        if (FilterDone() && NeedSort && FieldsAvailable.test(+SortBy)) {
            switch (SortBy) {
                case ENodeFields::NodeId:
                    SortCollection(NodeView, [](const TNode* node) { return node->GetNodeId(); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::HostName:
                    SortCollection(NodeView, [](const TNode* node) { return node->GetHostName(); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::NodeName:
                    SortCollection(NodeView, [](const TNode* node) { return node->GetNodeName(); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::DC:
                    SortCollection(NodeView, [](const TNode* node) { return node->NodeInfo.Location.GetDataCenterId(); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::Rack:
                    SortCollection(NodeView, [](const TNode* node) { return node->NodeInfo.Location.GetRackId(); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::Version:
                    SortCollection(NodeView, [](const TNode* node) { return node->SystemState.GetVersion(); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::Uptime:
                    SortCollection(NodeView, [](const TNode* node) { return node->UptimeSeconds; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::Memory:
                    SortCollection(NodeView, [](const TNode* node) { return node->SystemState.GetMemoryUsed(); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::CPU:
                    SortCollection(NodeView, [](const TNode* node) { return node->CpuUsage; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::LoadAverage:
                    SortCollection(NodeView, [](const TNode* node) { return node->LoadAverage; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::Missing:
                    SortCollection(NodeView, [](const TNode* node) { return node->MissingDisks; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::DiskSpaceUsage:
                    SortCollection(NodeView, [](const TNode* node) { return node->DiskSpaceUsage; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::Database:
                    SortCollection(NodeView, [](const TNode* node) { return node->Database; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::SystemState:
                    SortCollection(NodeView, [](const TNode* node) { return static_cast<int>(node->GetOverall()); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::Connections:
                    SortCollection(NodeView, [](const TNode* node) { return node->Connections; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::SendThroughput:
                    SortCollection(NodeView, [](const TNode* node) { return node->SendThroughput; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::ReceiveThroughput:
                    SortCollection(NodeView, [](const TNode* node) { return node->ReceiveThroughput; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::NetworkUtilization:
                    SortCollection(NodeView, [](const TNode* node) { return node->NetworkUtilization; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::ConnectStatus:
                    SortCollection(NodeView, [](const TNode* node) { return static_cast<int>(node->ConnectStatus); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::PingTime:
                    SortCollection(NodeView, [](const TNode* node) { return node->PingTimeUs; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::ClockSkew:
                    SortCollection(NodeView, [](const TNode* node) { return node->ClockSkewUs; }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::Peers:
                    SortCollection(NodeView, [](const TNode* node) { return node->Peers.size(); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::ReversePeers:
                    SortCollection(NodeView, [](const TNode* node) { return node->ReversePeers.size(); }, ReverseSort);
                    NeedSort = false;
                    break;
                case ENodeFields::NodeInfo:
                case ENodeFields::PDisks:
                case ENodeFields::VDisks:
                case ENodeFields::Tablets:
                case ENodeFields::SubDomainKey:
                case ENodeFields::DisconnectTime:
                case ENodeFields::HasDisks:
                case ENodeFields::COUNT:
                    break;
            }
            if (!NeedSort) {
                InvalidateNodes();
            }
            AddEvent("Sort Applied");
        }
    }

    void ApplyLimit() {
        if (FilterDone() && !NeedSort && !NeedGroup && NeedLimit) {
            if (Offset) {
                NodeView.erase(NodeView.begin(), NodeView.begin() + std::min(*Offset, NodeView.size()));
                InvalidateNodes();
            }
            if (Limit) {
                NodeView.resize(std::min(*Limit, NodeView.size()));
                InvalidateNodes();
            }
            NeedLimit = false;
            AddEvent("Limit Applied");
        }
    }

    void ApplyEverything() {
        AddEvent("ApplyEverything");
        ApplyFilter();
        ApplyGroup();
        ApplySort();
        ApplyLimit();
    }

    static constexpr size_t BATCH_SIZE = 200;

    void BuildCandidates(TNodeBatch& batch, std::vector<TNode*>& candidates) {
        auto itCandidate = candidates.begin();
        for (; itCandidate != candidates.end() && batch.NodesToAskFor.size() < OffloadMergeAttempts; ++itCandidate) {
            batch.NodesToAskFor.push_back(*itCandidate);
        }
        candidates.erase(candidates.begin(), itCandidate);
        for (TNode* node : batch.NodesToAskAbout) {
            if (node->IsStatic()) {
                batch.HasStaticNodes = true;
            }
        }
    }

    void SplitBatch(TNodeBatch& nodeBatch, std::vector<TNodeBatch>& batches) {
        std::vector<TNode*> candidates = nodeBatch.NodesToAskAbout;
        std::sort(candidates.begin(), candidates.end(), [](TNode* a, TNode* b) {
            return a->GetCandidateScore() > b->GetCandidateScore();
        });
        while (nodeBatch.NodesToAskAbout.size() > BATCH_SIZE) {
            TNodeBatch newBatch;
            size_t splitSize = std::min(BATCH_SIZE, nodeBatch.NodesToAskAbout.size() / 2);
            newBatch.NodesToAskAbout.reserve(splitSize);
            for (size_t i = 0; i < splitSize; ++i) {
                newBatch.NodesToAskAbout.push_back(nodeBatch.NodesToAskAbout.back());
                nodeBatch.NodesToAskAbout.pop_back();
            }
            BuildCandidates(newBatch, candidates);
            batches.emplace_back(std::move(newBatch));
        }
        if (!nodeBatch.NodesToAskAbout.empty()) {
            BuildCandidates(nodeBatch, candidates);
            batches.emplace_back(std::move(nodeBatch));
        }
    }

    std::vector<TNodeBatch> BatchNodes(const TNodeView& nodeView) {
        std::vector<TNodeBatch> batches;
        if (OffloadMerge) {
            std::unordered_map<TSubDomainKey, TNodeBatch> batchSubDomain;
            std::unordered_map<TString, TNodeBatch> batchDataCenters;
            for (TNode* node : nodeView) {
                if (node->IsStatic()) {
                    batchDataCenters[node->GetDataCenter()].NodesToAskAbout.push_back(node);
                } else {
                    batchSubDomain[node->SubDomainKey].NodesToAskAbout.push_back(node);
                }
            }
            for (auto& [subDomainKey, nodeBatch] : batchSubDomain) {
                if (nodeBatch.NodesToAskAbout.size() == 1) {
                    TNode* node = nodeBatch.NodesToAskAbout.front();
                    batchDataCenters[node->GetDataCenter()].NodesToAskAbout.push_back(node);
                } else {
                    SplitBatch(nodeBatch, batches);
                }
            }
            for (auto& [dataCenter, nodeBatch] : batchDataCenters) {
                SplitBatch(nodeBatch, batches);
            }
        } else {
            TNodeBatch nodeBatch;
            for (TNode* node : nodeView) {
                nodeBatch.NodesToAskAbout.push_back(node);
            }
            SplitBatch(nodeBatch, batches);
        }
        return batches;
    }

    bool HiveResponsesDone() const {
        for (const auto& [hiveId, hiveNodeStats] : HiveNodeStats) {
            if (!hiveNodeStats.IsDone()) {
                return false;
            }
        }
        return HivesToAsk.empty();
    }

    bool TimeToAskHive() {
        if (NodesInfoResponse && !NodesInfoResponse->IsDone()) {
            return false;
        }
        if (DatabaseNavigateResponse && !DatabaseNavigateResponse->IsDone()) {
            return false;
        }
        if (ResourceNavigateResponse && !ResourceNavigateResponse->IsDone()) {
            return false;
        }
        if (PathNavigateResponse && !PathNavigateResponse->IsDone()) {
            return false;
        }
        return CurrentTimeoutState < TimeoutTablets;
    }

    bool TimeToAskWhiteboard() {
        if (NodesInfoResponse && !NodesInfoResponse->IsDone()) {
            return false;
        }
        if (NodeStateResponse && !NodeStateResponse->IsDone()) {
            return false;
        }
        if (DatabaseNavigateResponse && !DatabaseNavigateResponse->IsDone()) {
            return false;
        }
        if (ResourceNavigateResponse && !ResourceNavigateResponse->IsDone()) {
            return false;
        }
        if (PathNavigateResponse && !PathNavigateResponse->IsDone()) {
            return false;
        }
        if (DatabaseBoardInfoResponse && !DatabaseBoardInfoResponse->IsDone()) {
            return false;
        }
        if (ResourceBoardInfoResponse && !ResourceBoardInfoResponse->IsDone()) {
            return false;
        }
        if (!HiveResponsesDone() || !HiveNodeStatsProcessed) {
            return false;
        }
        if (StoragePoolsResponse && !StoragePoolsResponse->IsDone()) {
            return false;
        }
        if (GroupsResponse && !GroupsResponse->IsDone()) {
            return false;
        }
        if (VSlotsResponse && !VSlotsResponse->IsDone()) {
            return false;
        }
        if (PDisksResponse && !PDisksResponse->IsDone()) {
            return false;
        }
        if (!SystemStateResponse.empty() || !SystemViewerResponse.empty()
            || !TabletStateResponse.empty() || !TabletViewerResponse.empty()
            || !PDiskStateResponse.empty() || !PDiskViewerResponse.empty()
            || !VDiskStateResponse.empty() || !VDiskViewerResponse.empty()
            || !PeersStateResponse.empty() || !PeersViewerResponse.empty()) {
            return false;
        }
        return CurrentTimeoutState < TimeoutFinal;
    }

    static TString GetDatabaseFromEndpointsBoardPath(const TString& path) {
        TStringBuf db(path);
        db.SkipPrefix("gpc+");
        return TString(db);
    }

    void CheckAndFillStoragePoolFilter(const TSchemeCacheNavigate::TEntry& entry) {
        if ((Type == EType::Storage || Type == EType::Static) && FilterStorageStage == EFilterStorageStage::Pools && FilterStoragePools.empty()) {
            auto domainDescription = entry.DomainDescription;
            if (domainDescription) {
                for (const auto& storagePool : domainDescription->Description.GetStoragePools()) {
                    FilterStoragePools.emplace_back(storagePool.GetName());
                }
                if (!FilterStoragePools.empty()) {
                    if (!StoragePoolsResponse) {
                        StoragePoolsResponse = RequestBSControllerPools();
                    }
                    if (!GroupsResponse) {
                        GroupsResponse = RequestBSControllerGroups();
                    }
                    if (!VSlotsResponse) {
                        VSlotsResponse = RequestBSControllerVSlots();
                    }
                }
            }
            FilterDatabase = false; // switching filter from database to storage pools
        }
    }

    void ProcessResponses() {
        AddEvent("ProcessResponses");
        if (NodesInfoResponse) {
            if (NodesInfoResponse->IsDone()) {
                if (NodesInfoResponse->IsOk()) {
                    bool seenDC = false;
                    bool seenRack = false;
                    for (const auto& ni : NodesInfoResponse->Get()->Nodes) {
                        TNode& node = NodeData.emplace_back();
                        node.NodeInfo = ni;
                        if (ni.Host && !node.SystemState.GetHost()) {
                            node.SystemState.SetHost(ni.Host);
                        }
                        if (ni.Location.GetDataCenterId() != 0) {
                            seenDC = true;
                        }
                        if (ni.Location.GetRackId() != 0) {
                            seenRack = true;
                        }
                    }
                    for (TNode& node : NodeData) {
                        NodeView.emplace_back(&node);
                    }
                    InvalidateNodes();
                    FieldsAvailable |= FieldsNodeInfo;
                    FoundNodes = TotalNodes = NodeView.size();
                    NoDC = !seenDC;
                    NoRack = !seenRack;
                    ApplyEverything();
                } else {
                    AddProblem("no-nodes-info");
                }
                NodesInfoResponse.reset();
            } else {
                return; // no further processing until we get node list
            }
        }

        if (NodeStateResponse && NodeStateResponse->IsDone() && TotalNodes > 0) {
            if (NodeStateResponse->IsOk()) {
                for (const auto& nodeStateInfo : NodeStateResponse->Get()->Record.GetNodeStateInfo()) {
                    TNodeId nodeId = nodeStateInfo.GetPeerNodeId()
                        ? nodeStateInfo.GetPeerNodeId()
                        : FromStringWithDefault(TStringBuf(nodeStateInfo.GetPeerName()).Before(':'), 0);
                    if (nodeId) {
                        TNode* node = FindNode(nodeId);
                        if (node) {
                            node->NetworkStateInfo = nodeStateInfo;
                        }
                    }
                }
                TNode* node = FindNode(TActivationContext::ActorSystem()->NodeId);
                if (node) {
                    node->NetworkStateInfo.MutableScopeId()->SetX1(AppData()->LocalScopeId.GetInterconnectScopeId().first);
                    node->NetworkStateInfo.MutableScopeId()->SetX2(AppData()->LocalScopeId.GetInterconnectScopeId().second);
                }
            } else {
                AddProblem("no-node-state-info");
            }
            NodeStateResponse.reset();
        }

        if (DatabaseNavigateResponse && DatabaseNavigateResponse->IsDone()) { // database hive and subdomain key
            if (DatabaseNavigateResponse->IsOk()) {
                auto* ev = DatabaseNavigateResponse->Get();
                TSchemeCacheNavigate::TEntry& entry(ev->Request->ResultSet.front());
                if (entry.DomainInfo) {
                    if (entry.DomainInfo->ResourcesDomainKey && entry.DomainInfo->DomainKey != entry.DomainInfo->ResourcesDomainKey) {
                        TPathId resourceDomainKey(entry.DomainInfo->ResourcesDomainKey);
                        ResourceNavigateResponse = MakeRequestSchemeCacheNavigate(resourceDomainKey, ENavigateRequestResource);
                    } else {
                        CheckAndFillStoragePoolFilter(entry);
                    }
                    if (FieldsNeeded(FieldsHiveNodeStat) || (FilterPath && FieldsNeeded(FieldsTablets))) {
                        const auto ownerId = entry.DomainInfo->DomainKey.OwnerId;
                        const auto localPathId = entry.DomainInfo->DomainKey.LocalPathId;
                        SubDomainKey = TSubDomainKey(ownerId, localPathId);
                        if (FilterDatabase) {
                            FilterSubDomainKey = true;
                        }
                        if (FieldsNeeded(FieldsHiveNodeStat) || (FilterPath && FieldsNeeded(FieldsTablets))) {
                            const auto ownerId = entry.DomainInfo->DomainKey.OwnerId;
                            const auto localPathId = entry.DomainInfo->DomainKey.LocalPathId;
                            SubDomainKey = TSubDomainKey(ownerId, localPathId);
                            if (FilterDatabase) {
                                FilterSubDomainKey = true;
                            }
                            HivesToAsk.push_back(AppData()->DomainsInfo->GetHive());
                            if (entry.DomainInfo->Params.HasHive()) {
                                HivesToAsk.push_back(entry.DomainInfo->Params.GetHive());
                            }
                        }
                    }
                }
            } else {
                NodeView.clear();
                AddProblem("no-database-info");
            }
            DatabaseNavigateResponse.reset();
        }

        if (ResourceNavigateResponse && ResourceNavigateResponse->IsDone()) { // database hive and subdomain key
            if (ResourceNavigateResponse->IsOk()) {
                auto* ev = ResourceNavigateResponse->Get();
                if (ev->Request->ResultSet.size() == 1 && ev->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                    TSchemeCacheNavigate::TEntry& entry(ev->Request->ResultSet.front());
                    auto path = CanonizePath(entry.Path);
                    SharedDatabase = path;
                    CheckAndFillStoragePoolFilter(entry);
                    if (FieldsNeeded(FieldsHiveNodeStat) || (FilterPath && FieldsNeeded(FieldsTablets))) {
                        HivesToAsk.push_back(AppData()->DomainsInfo->GetHive());
                        if (entry.DomainInfo) {
                            const auto ownerId = entry.DomainInfo->DomainKey.OwnerId;
                            const auto localPathId = entry.DomainInfo->DomainKey.LocalPathId;
                            SharedSubDomainKey = TSubDomainKey(ownerId, localPathId);
                            if (FilterDatabase) {
                                FilterSubDomainKey = true;
                            }
                            if (entry.DomainInfo->Params.HasHive()) {
                                HivesToAsk.push_back(entry.DomainInfo->Params.GetHive());
                            }
                        }
                    } else {
                        ResourceBoardInfoResponse = MakeRequestStateStorageEndpointsLookup(path, EBoardInfoRequestResource);
                    }
                }
            } else {
                NodeView.clear();
                AddProblem("no-shared-database-info");
            }
            ResourceNavigateResponse.reset();
        }

        if (PathNavigateResponse && PathNavigateResponse->IsDone()) { // filter path id
            if (PathNavigateResponse->IsOk()) {
                auto* ev = PathNavigateResponse->Get();
                if (ev->Request->ResultSet.size() == 1 && ev->Request->ResultSet.begin()->Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
                    TSchemeCacheNavigate::TEntry& entry(ev->Request->ResultSet.front());
                    if (entry.Self) {
                        FilterPathId = TPathId(entry.Self->Info.GetSchemeshardId(), entry.Self->Info.GetPathId());
                        AskHiveAboutPaths = true;
                        HivesToAsk.push_back(AppData()->DomainsInfo->GetHive());
                        if (entry.DomainInfo) {
                            const auto ownerId = entry.DomainInfo->DomainKey.OwnerId;
                            const auto localPathId = entry.DomainInfo->DomainKey.LocalPathId;
                            SubDomainKey = TSubDomainKey(ownerId, localPathId);
                            if (FilterDatabase) {
                                FilterSubDomainKey = true;
                            }
                            if (entry.DomainInfo->Params.HasHive()) {
                                HivesToAsk.push_back(entry.DomainInfo->Params.GetHive());
                            }
                        }
                    }
                }
            } else {
                AddProblem("no-path-info");
            }
            PathNavigateResponse.reset();
        }

        if (DatabaseBoardInfoResponse && DatabaseBoardInfoResponse->IsDone() && TotalNodes > 0) {
            if (DatabaseBoardInfoResponse->IsOk() && DatabaseBoardInfoResponse->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
                TString database = GetDatabaseFromEndpointsBoardPath(DatabaseBoardInfoResponse->Get()->Path);
                for (const auto& entry : DatabaseBoardInfoResponse->Get()->InfoEntries) {
                    if (!entry.second.Dropped) {
                        TNode* node = FindNode(entry.first.NodeId());
                        if (node) {
                            node->Database = database;
                            node->GotDatabaseFromDatabaseBoardInfo = true;
                            HasDatabaseNodes = true;
                        }
                    }
                }
                FieldsAvailable.set(+ENodeFields::Database);
            } else {
                AddProblem("no-database-board-info");
            }
            DatabaseBoardInfoResponse.reset();
        }

        if (ResourceBoardInfoResponse && ResourceBoardInfoResponse->IsDone() && TotalNodes > 0) {
            if (ResourceBoardInfoResponse->IsOk() && ResourceBoardInfoResponse->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
                TString database = GetDatabaseFromEndpointsBoardPath(ResourceBoardInfoResponse->Get()->Path);
                for (const auto& entry : ResourceBoardInfoResponse->Get()->InfoEntries) {
                    if (!entry.second.Dropped) {
                        TNode* node = FindNode(entry.first.NodeId());
                        if (node) {
                            node->Database = database;
                            node->GotDatabaseFromResourceBoardInfo = true;
                        }
                    }
                }
                FieldsAvailable.set(+ENodeFields::Database);
            } else {
                AddProblem("no-shared-database-board-info");
            }
            ResourceBoardInfoResponse.reset();
        }

        if (!TimeToAskHive()) {
            return;
        }

        AddEvent("TimeToAskHive");

        if (!HivesToAsk.empty()) {
            AddEvent("HivesTokHive");
            std::sort(HivesToAsk.begin(), HivesToAsk.end());
            HivesToAsk.erase(std::unique(HivesToAsk.begin(), HivesToAsk.end()), HivesToAsk.end());
            for (TTabletId hiveId : HivesToAsk) {
                auto request = std::make_unique<TEvHive::TEvRequestHiveNodeStats>();
                request->Record.SetReturnMetrics(true);
                if (Database) { // it's better to ask hive about tablets only if we're filtering by database
                    request->Record.SetReturnExtendedTabletInfo(true);
                }
                if (AskHiveAboutPaths) {
                    request->Record.SetFilterTabletsBySchemeShardId(FilterPathId.OwnerId);
                    request->Record.SetFilterTabletsByPathId(FilterPathId.LocalPathId);
                }
                HiveNodeStats.emplace(hiveId, MakeRequestHiveNodeStats(hiveId, request.release()));
            }
            HivesToAsk.clear();
        }

        if (HiveResponsesDone() && !HiveNodeStatsProcessed) {
            AddEvent("HiveResponsesDone");
            for (const auto& [hiveId, nodeStats] : HiveNodeStats) {
                if (nodeStats.IsDone()) {
                    if (nodeStats.IsOk()) {
                        TInstant now = TInstant::Now();
                        for (const NKikimrHive::THiveNodeStats& nodeStats : nodeStats.Get()->Record.GetNodeStats()) {
                            ui32 nodeId = nodeStats.GetNodeId();
                            TNode* node = FindNode(nodeId);
                            if (node) {
                                if (Database) { // it's better to ask hive about tablets only if we're filtering by database
                                    for (const NKikimrHive::THiveDomainStatsStateCount& stateStats : nodeStats.GetStateStats()) {
                                        NKikimrViewer::TTabletStateInfo& viewerTablet(node->Tablets.emplace_back());
                                        viewerTablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(stateStats.GetTabletType()));
                                        viewerTablet.SetCount(stateStats.GetCount());
                                        viewerTablet.SetState(GetFlagFromTabletState(stateStats.GetVolatileState()));
                                        FieldsAvailable.set(+ENodeFields::Tablets);
                                    }
                                }
                                if (nodeStats.HasLastAliveTimestamp()) {
                                    node->SystemState.SetDisconnectTime(std::max(node->SystemState.GetDisconnectTime(), nodeStats.GetLastAliveTimestamp())); // milliseconds
                                    node->CalcUptimeSeconds(now);
                                    FieldsAvailable.set(+ENodeFields::DisconnectTime);
                                }
                                if (nodeStats.HasNodeDomain()) {
                                    node->SubDomainKey = TSubDomainKey(nodeStats.GetNodeDomain());
                                    FieldsAvailable.set(+ENodeFields::SubDomainKey);
                                    if (node->SubDomainKey == SubDomainKey) {
                                        HasDatabaseNodes = true;
                                    }
                                }
                            }
                        }
                    } else {
                        AddProblem("hive-no-data");
                    }
                }
            }
            HiveNodeStatsProcessed = true;
        }

        if (FilterStorageStage == EFilterStorageStage::Pools && StoragePoolsResponse && StoragePoolsResponse->IsDone()) {
            if (StoragePoolsResponse->IsOk()) {
                for (const auto& storagePoolEntry : StoragePoolsResponse->Get()->Record.GetEntries()) {
                    auto itFilterStoragePool = std::ranges::find(FilterStoragePools, storagePoolEntry.GetInfo().GetName());
                    if (itFilterStoragePool != FilterStoragePools.end()) {
                        FilterStoragePoolsIds.emplace_back(std::make_pair(storagePoolEntry.GetKey().GetBoxId(), storagePoolEntry.GetKey().GetStoragePoolId()));
                        FilterStoragePools.erase(itFilterStoragePool);
                        if (FilterStoragePools.empty()) {
                            break;
                        }
                    }
                }
                FilterStorageStage = EFilterStorageStage::Groups;
            } else {
                AddProblem("bsc-storage-pools-no-data");
            }
            StoragePoolsResponse.reset();
        }
        if (FilterStorageStage == EFilterStorageStage::Groups && GroupsResponse && GroupsResponse->IsDone()) {
            if (GroupsResponse->IsOk()) {
                for (const auto& groupEntry : GroupsResponse->Get()->Record.GetEntries()) {
                    auto itFilterStoragePoolId = std::ranges::find(FilterStoragePoolsIds,
                        std::pair<ui64, ui64>(groupEntry.GetInfo().GetBoxId(),
                            groupEntry.GetInfo().GetStoragePoolId()));
                    if (itFilterStoragePoolId != FilterStoragePoolsIds.end()) {
                        FilterGroupIds.insert(groupEntry.GetKey().GetGroupId());
                    }
                }
                FilterStorageStage = EFilterStorageStage::VSlots;
            } else {
                AddProblem("bsc-storage-groups-no-data");
            }
            GroupsResponse.reset();
        }
        if ((FilterStorageStage == EFilterStorageStage::VSlots || FilterStorageStage == EFilterStorageStage::None) && VSlotsResponse && VSlotsResponse->IsDone()) {
            if (VSlotsResponse->IsOk()) {
                std::unordered_set<TNodeId> prevFilterNodeIds = std::move(FilterNodeIds);
                std::unordered_map<std::pair<TNodeId, ui32>, std::size_t> slotsPerDisk;
                for (const auto& slotEntry : VSlotsResponse->Get()->Record.GetEntries()) {
                    if (FilterGroupIds.count(slotEntry.GetInfo().GetGroupId()) > 0) {
                        if (prevFilterNodeIds.empty() || prevFilterNodeIds.count(slotEntry.GetKey().GetNodeId()) > 0) {
                            FilterNodeIds.insert(slotEntry.GetKey().GetNodeId());
                        }
                        TNode* node = FindNode(slotEntry.GetKey().GetNodeId());
                        if (node) {
                            node->SysViewVDisks.emplace_back(slotEntry);
                            node->HasDisks = true;
                        }
                    } else {
                        TNode* node = FindNode(slotEntry.GetKey().GetNodeId());
                        if (node) {
                            node->HasDisks = true;
                        }
                    }
                    auto& slots = slotsPerDisk[{slotEntry.GetKey().GetNodeId(), slotEntry.GetKey().GetPDiskId()}];
                    ++slots;
                    MaximumSlotsPerDisk = std::max(MaximumSlotsPerDisk.value_or(0), slots);
                }
                FieldsAvailable.set(+ENodeFields::HasDisks);
                FilterStorageStage = EFilterStorageStage::None;
                ApplyEverything();
            } else {
                AddProblem("bsc-storage-slots-no-data");
            }
            VSlotsResponse.reset();
        }
        if (PDisksResponse && PDisksResponse->IsDone()) {
            if (PDisksResponse->IsOk()) {
                std::unordered_map<TNodeId, std::size_t> disksPerNode;
                for (const auto& pdiskEntry : PDisksResponse->Get()->Record.GetEntries()) {
                    TNode* node = FindNode(pdiskEntry.GetKey().GetNodeId());
                    if (node) {
                        node->SysViewPDisks.emplace_back(pdiskEntry);
                        node->HasDisks = true;
                    }
                    auto& disks = disksPerNode[pdiskEntry.GetKey().GetNodeId()];
                    ++disks;
                    MaximumDisksPerNode = std::max(MaximumDisksPerNode.value_or(0), disks);
                }
                for (TNode* node : NodeView) {
                    node->CalcDisks();
                }
                FieldsAvailable.set(+ENodeFields::HasDisks);
                FieldsAvailable.set(+ENodeFields::Missing);
                FieldsAvailable.set(+ENodeFields::DiskSpaceUsage);
            } else {
                AddProblem("bsc-pdisks-no-data");
            }
            PDisksResponse.reset();
        }

        if (!TimeToAskWhiteboard()) {
            return;
        }

        ApplyEverything();

        if (FieldsAvailable.test(+ENodeFields::NodeInfo)) {
            AddEvent("TimeToAskWhiteboard");
            if (FilterDatabase) {
                FieldsRequired.set(+ENodeFields::SystemState);
            }
            std::vector<TNodeBatch> batches = BatchNodes(NodeView);
            SendWhiteboardRequests(batches);
        }
    }

    template<typename TWhiteboardEvent>
    void InitWhiteboardRequest(TWhiteboardEvent* request) {
        if (AllWhiteboardFields) {
            request->AddFieldsRequired(-1);
        }
    }

    template<>
    void InitWhiteboardRequest(NKikimrWhiteboard::TEvSystemStateRequest* request) {
        if (AllWhiteboardFields) {
            request->AddFieldsRequired(-1);
        } else {
            request->MutableFieldsRequired()->CopyFrom(GetDefaultWhiteboardFields<NKikimrWhiteboard::TSystemStateInfo>());
            request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresUsedFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresTotalFieldNumber);
        }
    }

    template<>
    void InitWhiteboardRequest(NKikimrWhiteboard::TEvTabletStateRequest* request) {
        if (AllWhiteboardFields) {
            request->AddFieldsRequired(-1);
        }
        request->SetGroupBy("Type,State");
    }

    template<>
    void InitWhiteboardRequest(NKikimrWhiteboard::TEvNodeStateRequest* request) {
        if (AllWhiteboardFields) {
            request->AddFieldsRequired(-1);
        } else {
            request->MutableFieldsRequired()->CopyFrom(GetDefaultWhiteboardFields<NKikimrWhiteboard::TNodeStateInfo>());
            request->AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kConnectTimeFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kClockSkewUsFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kPingTimeUsFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kUtilizationFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kScopeIdFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kBytesWrittenFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kWriteThroughputFieldNumber);
        }
    }

    void SendWhiteboardRequest(TNodeBatch& batch) {
        TNodeId nodeId = OffloadMerge ? batch.ChooseNodeId() : 0;
        if (nodeId) {
            if (FieldsNeeded(FieldsSystemState) && SystemViewerResponse.count(nodeId) == 0) {
                auto viewerRequest = std::make_unique<TEvViewer::TEvViewerRequest>();
                InitWhiteboardRequest(viewerRequest->Record.MutableSystemRequest());
                viewerRequest->Record.SetTimeout(Timeout / 2);
                std::unordered_set<TNodeId> nodeIds;
                for (const TNode* node : batch.NodesToAskAbout) {
                    nodeIds.insert(node->GetNodeId());
                    viewerRequest->Record.MutableLocation()->AddNodeId(node->GetNodeId());
                }
                SystemViewerRequest[nodeId] = std::move(nodeIds);
                SystemViewerResponse.emplace(nodeId, MakeViewerRequest(nodeId, viewerRequest.release()));
                NodeBatches.emplace(nodeId, batch);
                ++WhiteboardStateRequestsInFlight;
            }
            if (FieldsNeeded(FieldsTablets) && TabletViewerResponse.count(nodeId) == 0) {
                auto viewerRequest = std::make_unique<TEvViewer::TEvViewerRequest>();
                InitWhiteboardRequest(viewerRequest->Record.MutableTabletRequest());
                viewerRequest->Record.SetTimeout(Timeout / 2);
                for (const TNode* node : batch.NodesToAskAbout) {
                    viewerRequest->Record.MutableLocation()->AddNodeId(node->GetNodeId());
                }
                TabletViewerResponse.emplace(nodeId, MakeViewerRequest(nodeId, viewerRequest.release()));
                NodeBatches.emplace(nodeId, batch); // ignore second insert because they are the same
                ++WhiteboardStateRequestsInFlight;
            }
            if (batch.HasStaticNodes) {
                if (FieldsNeeded(FieldsPDisks) && PDiskViewerResponse.count(nodeId) == 0) {
                    auto viewerRequest = std::make_unique<TEvViewer::TEvViewerRequest>();
                    InitWhiteboardRequest(viewerRequest->Record.MutablePDiskRequest());
                    viewerRequest->Record.SetTimeout(Timeout / 2);
                    for (const TNode* node : batch.NodesToAskAbout) {
                        if (node->IsStatic()) {
                            viewerRequest->Record.MutableLocation()->AddNodeId(node->GetNodeId());
                        }
                    }
                    PDiskViewerResponse.emplace(nodeId, MakeViewerRequest(nodeId, viewerRequest.release()));
                    NodeBatches.emplace(nodeId, batch); // ignore second insert because they are the same
                    ++WhiteboardStateRequestsInFlight;
                }
                if (FieldsNeeded(FieldsVDisks) && VDiskViewerResponse.count(nodeId) == 0) {
                    auto viewerRequest = std::make_unique<TEvViewer::TEvViewerRequest>();
                    InitWhiteboardRequest(viewerRequest->Record.MutableVDiskRequest());
                    viewerRequest->Record.SetTimeout(Timeout / 2);
                    for (const TNode* node : batch.NodesToAskAbout) {
                        if (node->IsStatic()) {
                            viewerRequest->Record.MutableLocation()->AddNodeId(node->GetNodeId());
                        }
                    }
                    VDiskViewerResponse.emplace(nodeId, MakeViewerRequest(nodeId, viewerRequest.release()));
                    NodeBatches.emplace(nodeId, batch); // ignore second insert because they are the same
                    ++WhiteboardStateRequestsInFlight;
                }
            }
            if (FieldsNeeded(FieldsPeers) && PeersViewerResponse.count(nodeId) == 0) {
                auto viewerRequest = std::make_unique<TEvViewer::TEvViewerRequest>();
                InitWhiteboardRequest(viewerRequest->Record.MutableNodeRequest());
                viewerRequest->Record.SetTimeout(Timeout / 2);
                for (const TNode* node : batch.NodesToAskAbout) {
                    viewerRequest->Record.MutableLocation()->AddNodeId(node->GetNodeId());
                }
                PeersViewerResponse.emplace(nodeId, MakeViewerRequest(nodeId, viewerRequest.release()));
                NodeBatches.emplace(nodeId, batch); // ignore second insert because they are the same
                ++WhiteboardStateRequestsInFlight;
            }
        } else {
            for (const TNode* node : batch.NodesToAskAbout) {
                if (node->Disconnected) {
                    continue;
                }
                TNodeId nodeId = node->GetNodeId();
                if (FieldsNeeded(FieldsSystemState)) {
                    if (SystemStateResponse.count(nodeId) == 0) {
                        auto request = new TEvWhiteboard::TEvSystemStateRequest();
                        InitWhiteboardRequest(&request->Record);
                        SystemStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request));
                        ++WhiteboardStateRequestsInFlight;
                    }
                }
                if (FieldsNeeded(FieldsTablets)) {
                    if (TabletStateResponse.count(nodeId) == 0) {
                        auto request = std::make_unique<TEvWhiteboard::TEvTabletStateRequest>();
                        InitWhiteboardRequest(&request->Record);
                        TabletStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request.release()));
                        ++WhiteboardStateRequestsInFlight;
                    }
                }
                if (FieldsNeeded(FieldsPeers)) {
                    if (PeersStateResponse.count(nodeId) == 0) {
                        auto request = std::make_unique<TEvWhiteboard::TEvNodeStateRequest>();
                        InitWhiteboardRequest(&request->Record);
                        PeersStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request.release()));
                        ++WhiteboardStateRequestsInFlight;
                    }
                }
                if (node->IsStatic()) {
                    if (FieldsNeeded(FieldsVDisks)) {
                        if (VDiskStateResponse.count(nodeId) == 0) {
                            auto request = new TEvWhiteboard::TEvVDiskStateRequest();
                            InitWhiteboardRequest(&request->Record);
                            VDiskStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request));
                            ++WhiteboardStateRequestsInFlight;
                        }
                    }
                    if (FieldsNeeded(FieldsPDisks)) {
                        if (PDiskStateResponse.count(nodeId) == 0) {
                            auto request = new TEvWhiteboard::TEvPDiskStateRequest();
                            InitWhiteboardRequest(&request->Record);
                            PDiskStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request));
                            ++WhiteboardStateRequestsInFlight;
                        }
                    }
                }
            }
        }
    }

    void SendWhiteboardRequests(std::vector<TNodeBatch>& batches) {
        for (TNodeBatch& batch : batches) {
            SendWhiteboardRequest(batch);
        }
    }

    int WhiteboardRequestRound = 1;

    void ProcessWhiteboard() {
        AddEvent("ProcessWhiteboard");
        if (FieldsNeeded(FieldsReversePeers) && WhiteboardRequestRound++ == 1) {
            std::unordered_set<TNodeId> nodeIds;
            std::unordered_set<TNodeId> reverseNodeIds;
            for (auto& [nodeId, response] : PeersViewerResponse) {
                if (response.IsOk()) {
                    auto& nodeResponse(*(response.Get()->Record.MutableNodeResponse()));
                    for (const auto& nodeState : nodeResponse.GetNodeStateInfo()) {
                        if (nodeState.GetNodeId()) {
                            nodeIds.insert(nodeState.GetNodeId());
                        }
                        if (nodeState.GetConnected() && nodeState.GetPeerNodeId()) {
                            reverseNodeIds.insert(nodeState.GetPeerNodeId());
                        }
                    }
                }
            }
            for (auto& [nodeId, response] : PeersStateResponse) {
                if (response.IsOk()) {
                    nodeIds.insert(nodeId);
                    const auto& nodeState(response.Get()->Record);
                    for (const auto& nodeStateInfo : nodeState.GetNodeStateInfo()) {
                        if (nodeStateInfo.GetConnected() && nodeStateInfo.GetPeerNodeId()) {
                            reverseNodeIds.insert(nodeStateInfo.GetPeerNodeId());
                        }
                    }
                }
            }
            for (auto nodeId : nodeIds) {
                reverseNodeIds.erase(nodeId);
            }
            if (!reverseNodeIds.empty()) {
                std::unordered_map<TNodeId, TNode*> reverseNodesByNodeId;
                TNodeView reverseNodeView;
                for (TNode& node : NodeData) {
                    reverseNodesByNodeId[node.GetNodeId()] = &node;
                }
                for (TNodeId reverseNodeId : reverseNodeIds) {
                    auto it = reverseNodesByNodeId.find(reverseNodeId);
                    if (it != reverseNodesByNodeId.end()) {
                        reverseNodeView.push_back(it->second);
                    }
                }
                AddEvent("ReversePeers");
                std::vector<TNodeBatch> batches = BatchNodes(reverseNodeView);
                SendWhiteboardRequests(batches);
                if (WhiteboardStateRequestsInFlight > 0) {
                    return;
                }
            }
        }
        if (FieldsNeeded(FieldsSystemState)) {
            TInstant now = TInstant::Now();
            std::unordered_set<TNodeId> removeNodes;
            for (const auto& [responseNodeId, response] : SystemViewerResponse) {
                if (response.IsOk()) {
                    const auto& systemResponse(response.Get()->Record.GetSystemResponse());
                    std::unordered_set<TNodeId> nodesWithoutData = std::move(SystemViewerRequest[responseNodeId]);
                    for (const auto& systemInfo : systemResponse.GetSystemStateInfo()) {
                        TNodeId nodeId = systemInfo.GetNodeId();
                        TNode* node = FindNode(nodeId);
                        if (node) {
                            nodesWithoutData.erase(nodeId);
                            node->MergeFrom(systemInfo, now);
                            // if (!node->Database && node->IsStatic()) {
                            //     node->Database = DomainPath;
                            // }
                            if (Database && node->Database) {
                                if (node->Database != Database && (!SharedDatabase || node->Database != SharedDatabase)) {
                                    removeNodes.insert(nodeId);
                                }
                            }
                        }
                    }
                    for (auto nodeId : nodesWithoutData) {
                        TNode* node = FindNode(nodeId);
                        if (node) {
                            node->DisconnectNode();
                        }
                    }
                } else {
                    TNode* node = FindNode(responseNodeId);
                    if (node) {
                        node->DisconnectNode();
                    }
                }
            }
            for (const auto& [nodeId, response] : SystemStateResponse) {
                if (response.IsOk()) {
                    const auto& systemState(response.Get()->Record);
                    if (systemState.SystemStateInfoSize() > 0) {
                        TNode* node = FindNode(nodeId);
                        if (node) {
                            node->MergeFrom(systemState.GetSystemStateInfo(0), now);
                            // if (!node->Database && node->IsStatic()) {
                            //     node->Database = DomainPath;
                            // }
                            if (Database && node->Database) {
                                if (node->Database != Database && (!SharedDatabase || node->Database != SharedDatabase)) {
                                    removeNodes.insert(nodeId);
                                }
                            }
                        }
                    }
                } else {
                    TNode* node = FindNode(nodeId);
                    if (node) {
                        node->DisconnectNode();
                    }
                }
            }
            if (!removeNodes.empty()) {
                NodeView.erase(std::remove_if(NodeView.begin(), NodeView.end(), [&removeNodes](const TNode* node) { return removeNodes.count(node->GetNodeId()) > 0; }), NodeView.end());
                TotalNodes = FoundNodes = NodeView.size();
                InvalidateNodes();
            }
            FieldsAvailable |= FieldsSystemState;
            FieldsAvailable.set(+ENodeFields::Database);
        }
        if (FieldsNeeded(FieldsTablets)) {
            for (auto& [nodeId, response] : TabletViewerResponse) {
                if (response.IsOk()) {
                    auto& tabletResponse(*(response.Get()->Record.MutableTabletResponse()));
                    if (tabletResponse.TabletStateInfoSize() > 0 && !tabletResponse.GetTabletStateInfo(0).HasCount()) {
                        GroupWhiteboardResponses(tabletResponse, "NodeId,Type,State");
                    }
                    for (const auto& tabletState : tabletResponse.GetTabletStateInfo()) {
                        TNode* node = FindNode(tabletState.GetNodeId());
                        if (node) {
                            if (tabletState.GetState() != NKikimrWhiteboard::TTabletStateInfo::Dead) {
                                NKikimrViewer::TTabletStateInfo& viewerTablet(node->Tablets.emplace_back());
                                viewerTablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(tabletState.GetType()));
                                viewerTablet.SetState(GetFlagFromTabletState(tabletState.GetState()));
                                viewerTablet.SetCount(tabletState.GetCount());
                            }
                        }
                    }
                }
            }
            for (auto& [nodeId, response] : TabletStateResponse) {
                if (response.IsOk()) {
                    const auto& tabletState(response.Get()->Record);
                    TNode* node = FindNode(nodeId);
                    if (node) {
                        for (const auto& protoTabletState : tabletState.GetTabletStateInfo()) {
                            if (protoTabletState.GetState() != NKikimrWhiteboard::TTabletStateInfo::Dead) {
                                NKikimrViewer::TTabletStateInfo& viewerTablet(node->Tablets.emplace_back());
                                viewerTablet.SetType(NKikimrTabletBase::TTabletTypes::EType_Name(protoTabletState.GetType()));
                                viewerTablet.SetState(GetFlagFromTabletState(protoTabletState.GetState()));
                                viewerTablet.SetCount(protoTabletState.GetCount());
                            }
                        }
                    }
                }
            }
            FieldsAvailable |= FieldsTablets;
        }
        if (FieldsNeeded(FieldsVDisks)) {
            for (auto& [nodeId, response] : VDiskViewerResponse) {
                if (response.IsOk()) {
                    auto& vDiskResponse(*(response.Get()->Record.MutableVDiskResponse()));
                    for (const auto& vDiskState : vDiskResponse.GetVDiskStateInfo()) {
                        TNode* node = FindNode(vDiskState.GetNodeId());
                        if (node) {
                            node->VDisks.emplace_back(vDiskState);
                        }
                    }
                }
            }
            for (const auto& [nodeId, response] : VDiskStateResponse) {
                if (response.IsOk()) {
                    const auto& vDiskState(response.Get()->Record);
                    TNode* node = FindNode(nodeId);
                    if (node) {
                        for (const auto& protoVDiskState : vDiskState.GetVDiskStateInfo()) {
                            node->VDisks.emplace_back(protoVDiskState);
                        }
                    }
                }
            }
            FieldsAvailable |= FieldsVDisks;
        }
        if (FieldsNeeded(FieldsPDisks)) {
            for (auto& [nodeId, response] : PDiskViewerResponse) {
                if (response.IsOk()) {
                    auto& pDiskResponse(*(response.Get()->Record.MutablePDiskResponse()));
                    for (const auto& pDiskState : pDiskResponse.GetPDiskStateInfo()) {
                        TNode* node = FindNode(pDiskState.GetNodeId());
                        if (node) {
                            node->PDisks.emplace_back(pDiskState);
                            node->CalcDisks();
                        }
                    }
                }
            }
            for (const auto& [nodeId, response] : PDiskStateResponse) {
                if (response.IsOk()) {
                    const auto& pDiskState(response.Get()->Record);
                    TNode* node = FindNode(nodeId);
                    if (node) {
                        for (const auto& protoPDiskState : pDiskState.GetPDiskStateInfo()) {
                            node->PDisks.emplace_back(protoPDiskState);
                        }
                        node->CalcDisks();
                    }
                }
            }
            FieldsAvailable |= FieldsPDisks;
            FieldsAvailable.set(+ENodeFields::Missing);
            FieldsAvailable.set(+ENodeFields::DiskSpaceUsage);
        }
        bool needCalcPeers = false;
        if (FieldsNeeded(FieldsPeers)) {
            for (auto& [nodeId, response] : PeersViewerResponse) {
                if (response.IsOk()) {
                    auto& nodeResponse(*(response.Get()->Record.MutableNodeResponse()));
                    for (const auto& nodeState : nodeResponse.GetNodeStateInfo()) {
                        TNode* node = FindNode(nodeState.GetNodeId());
                        if (node) {
                            node->Peers.emplace_back(nodeState);
                        }
                    }
                }
            }
            for (auto& [nodeId, response] : PeersStateResponse) {
                if (response.IsOk()) {
                    const auto& nodeState(response.Get()->Record);
                    TNode* node = FindNode(nodeId);
                    if (node) {
                        for (const auto& protoNodeState : nodeState.GetNodeStateInfo()) {
                            node->Peers.emplace_back(protoNodeState).SetNodeId(nodeId);
                        }
                    }
                }
            }
            FieldsAvailable |= FieldsPeers;
            needCalcPeers = true;
        }
        if (FieldsNeeded(FieldsReversePeers)) {
            for (auto& [nodeId, response] : PeersViewerResponse) {
                if (response.IsOk()) {
                    auto& nodeResponse(*(response.Get()->Record.MutableNodeResponse()));
                    for (const auto& nodeState : nodeResponse.GetNodeStateInfo()) {
                        if (nodeState.GetPeerNodeId()) {
                            TNode* reverseNode = FindNode(nodeState.GetPeerNodeId());
                            if (reverseNode) {
                                reverseNode->ReversePeers.emplace_back(nodeState);
                            }
                        }
                    }
                }
            }
            for (auto& [nodeId, response] : PeersStateResponse) {
                if (response.IsOk()) {
                    const auto& nodeState(response.Get()->Record);
                    for (const auto& protoNodeState : nodeState.GetNodeStateInfo()) {
                        TNode* reverseNode = FindNode(protoNodeState.GetPeerNodeId());
                        if (reverseNode) {
                            reverseNode->ReversePeers.emplace_back(protoNodeState).SetNodeId(nodeId);
                        }
                    }
                }
            }
            FieldsAvailable |= FieldsReversePeers;
            needCalcPeers = true;
        }
        if (needCalcPeers) {
            for (TNode* node : NodeView) {
                node->CalcPeers();
            }
        }
        ApplyEverything();
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        NodesInfoResponse->Set(std::move(ev));
        ProcessResponses();
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvNodeStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        if (NodeStateResponse && !NodeStateResponse->IsDone() && nodeId == TActivationContext::ActorSystem()->NodeId) {
            NodeStateResponse->Set(std::move(ev));
            ProcessResponses();
            RequestDone();
        } else if (PeersStateResponse[nodeId].Set(std::move(ev))) {
            WhiteboardRequestDone();
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        if (ev->Cookie == ENavigateRequestDatabase) {
            DatabaseNavigateResponse->Set(std::move(ev));
        } else if (ev->Cookie == ENavigateRequestResource) {
            ResourceNavigateResponse->Set(std::move(ev));
        } else if (ev->Cookie == ENavigateRequestPath) {
            PathNavigateResponse->Set(std::move(ev));
        }
        ProcessResponses();
        RequestDone();
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        if (ev->Cookie == EBoardInfoRequestDatabase) {
            DatabaseBoardInfoResponse->Set(std::move(ev));
        } else if (ev->Cookie == EBoardInfoRequestResource) {
            ResourceBoardInfoResponse->Set(std::move(ev));
        }
        ProcessResponses();
        RequestDone();
    }

    void Handle(TEvHive::TEvResponseHiveNodeStats::TPtr& ev) {
        if (HiveNodeStats[ev->Cookie].Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void WhiteboardRequestDone() {
        --WhiteboardStateRequestsInFlight;
        if (WhiteboardStateRequestsInFlight == 0) {
            ProcessWhiteboard();
        }
        RequestDone();
    }

    void Handle(TEvWhiteboard::TEvSystemStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        if (SystemStateResponse[nodeId].Set(std::move(ev))) {
            WhiteboardRequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        if (VDiskStateResponse[nodeId].Set(std::move(ev))) {
            WhiteboardRequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        if (PDiskStateResponse[nodeId].Set(std::move(ev))) {
            WhiteboardRequestDone();
        }
    }

    void Handle(TEvWhiteboard::TEvTabletStateResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        if (TabletStateResponse[nodeId].Set(std::move(ev))) {
            WhiteboardRequestDone();
        }
    }

    void Handle(TEvViewer::TEvViewerResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        switch (ev->Get()->Record.Response_case()) {
            case NKikimrViewer::TEvViewerResponse::ResponseCase::kSystemResponse:
                if (SystemViewerResponse[nodeId].Set(std::move(ev))) {
                    NodeBatches.erase(nodeId);
                    WhiteboardRequestDone();
                }
                return;
            case NKikimrViewer::TEvViewerResponse::ResponseCase::kTabletResponse:
                if (TabletViewerResponse[nodeId].Set(std::move(ev))) {
                    NodeBatches.erase(nodeId);
                    WhiteboardRequestDone();
                }
                return;
            case NKikimrViewer::TEvViewerResponse::ResponseCase::kPDiskResponse:
                if (PDiskViewerResponse[nodeId].Set(std::move(ev))) {
                    NodeBatches.erase(nodeId);
                    WhiteboardRequestDone();
                }
                return;
            case NKikimrViewer::TEvViewerResponse::ResponseCase::kVDiskResponse:
                if (VDiskViewerResponse[nodeId].Set(std::move(ev))) {
                    NodeBatches.erase(nodeId);
                    WhiteboardRequestDone();
                }
                return;
            case NKikimrViewer::TEvViewerResponse::ResponseCase::kNodeResponse:
                if (PeersViewerResponse[nodeId].Set(std::move(ev))) {
                    NodeBatches.erase(nodeId);
                    WhiteboardRequestDone();
                }
                return;
            default:
                break;
        }
        TString error("WrongResponse");
        {
            auto itSystemViewerResponse = SystemViewerResponse.find(nodeId);
            if (itSystemViewerResponse != SystemViewerResponse.end()) {
                if (itSystemViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletViewerResponse = TabletViewerResponse.find(nodeId);
            if (itTabletViewerResponse != TabletViewerResponse.end()) {
                if (itTabletViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itPDiskViewerResponse = PDiskViewerResponse.find(nodeId);
            if (itPDiskViewerResponse != PDiskViewerResponse.end()) {
                if (itPDiskViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itVDiskViewerResponse = VDiskViewerResponse.find(nodeId);
            if (itVDiskViewerResponse != VDiskViewerResponse.end()) {
                if (itVDiskViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itPeersViewerResponse = PeersViewerResponse.find(nodeId);
            if (itPeersViewerResponse != PeersViewerResponse.end()) {
                if (itPeersViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        if (StoragePoolsResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetGroupsResponse::TPtr& ev) {
        if (GroupsResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        if (VSlotsResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetPDisksResponse::TPtr& ev) {
        if (PDisksResponse->Set(std::move(ev))) {
            ProcessResponses();
            RequestDone();
        }
    }

    void FailViewerRequestsForNode(TNodeId nodeId, const TString& error) {
        {
            auto itSystemViewerResponse = SystemViewerResponse.find(nodeId);
            if (itSystemViewerResponse != SystemViewerResponse.end()) {
                if (itSystemViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletViewerResponse = TabletViewerResponse.find(nodeId);
            if (itTabletViewerResponse != TabletViewerResponse.end()) {
                if (itTabletViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itPDiskViewerResponse = PDiskViewerResponse.find(nodeId);
            if (itPDiskViewerResponse != PDiskViewerResponse.end()) {
                if (itPDiskViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itVDiskViewerResponse = VDiskViewerResponse.find(nodeId);
            if (itVDiskViewerResponse != VDiskViewerResponse.end()) {
                if (itVDiskViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itPeersViewerResponse = PeersViewerResponse.find(nodeId);
            if (itPeersViewerResponse != PeersViewerResponse.end()) {
                if (itPeersViewerResponse->second.Error(error)) {
                    if (NodeBatches.count(nodeId)) {
                        SendWhiteboardRequest(NodeBatches[nodeId]);
                        NodeBatches.erase(nodeId);
                    }
                    WhiteboardRequestDone();
                }
            }
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        TNodeId nodeId = ev->Get()->NodeId;
        TNode* node = FindNode(nodeId);
        if (node) {
            node->DisconnectNode();
            if (FieldsRequired.test(+ENodeFields::PDisks) || FieldsRequired.test(+ENodeFields::VDisks)) {
                node->RemapDisks();
            }
        }
        TString error("NodeDisconnected");
        {
            auto itSystemStateResponse = SystemStateResponse.find(nodeId);
            if (itSystemStateResponse != SystemStateResponse.end()) {
                if (itSystemStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itVDiskStateResponse = VDiskStateResponse.find(nodeId);
            if (itVDiskStateResponse != VDiskStateResponse.end()) {
                if (itVDiskStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itPDiskStateResponse = PDiskStateResponse.find(nodeId);
            if (itPDiskStateResponse != PDiskStateResponse.end()) {
                if (itPDiskStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itTabletStateResponse = TabletStateResponse.find(nodeId);
            if (itTabletStateResponse != TabletStateResponse.end()) {
                if (itTabletStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itPeersStateResponse = PeersStateResponse.find(nodeId);
            if (itPeersStateResponse != PeersStateResponse.end()) {
                if (itPeersStateResponse->second.Error(error)) {
                    WhiteboardRequestDone();
                }
            }
        }
        FailViewerRequestsForNode(nodeId, error);
    }

    bool OnBscError(const TString& error) {
        bool result = false;
        if (StoragePoolsResponse && StoragePoolsResponse->Error(error)) {
            ProcessResponses();
            RequestDone();
            result = true;
        }
        if (GroupsResponse && GroupsResponse->Error(error)) {
            ProcessResponses();
            RequestDone();
            result = true;
        }
        if (VSlotsResponse && VSlotsResponse->Error(error)) {
            ProcessResponses();
            RequestDone();
            result = true;
        }
        if (PDisksResponse && PDisksResponse->Error(error)) {
            ProcessResponses();
            RequestDone();
            result = true;
        }
        return result;
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            TString error = TStringBuilder() << "Failed to establish pipe to " << ev->Get()->TabletId << ": "
                << NKikimrProto::EReplyStatus_Name(ev->Get()->Status);
            auto it = HiveNodeStats.find(ev->Get()->TabletId);
            if (it != HiveNodeStats.end()) {
                if (it->second.Error(error)) {
                    AddProblem("hive-error");
                    ProcessResponses();
                    RequestDone();
                }
            }
            if (ev->Get()->TabletId == GetBSControllerId()) {
                if (OnBscError(error)) {
                    AddProblem("bsc-error");
                }
            }
            FailPipeConnect(ev->Get()->TabletId);
        }
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr& ev) {
        CurrentTimeoutState = static_cast<ETimeoutTag>(ev->Get()->Tag);
        TString error = "Timeout";
        if (ev->Get()->Tag == TimeoutTablets) {
            if (NodesInfoResponse && NodesInfoResponse->Error(error)) {
                ProcessResponses();
                RequestDone();
            }
            if (NodeStateResponse && NodeStateResponse->Error(error)) {
                ProcessResponses();
                RequestDone();
            }
            if (DatabaseNavigateResponse && DatabaseNavigateResponse->Error(error)) {
                ProcessResponses();
                RequestDone();
            }
            if (ResourceNavigateResponse && ResourceNavigateResponse->Error(error)) {
                ProcessResponses();
                RequestDone();
            }
            if (PathNavigateResponse && PathNavigateResponse->Error(error)) {
                ProcessResponses();
                RequestDone();
            }
            if (OnBscError(error)) {
                AddProblem("bsc-timeout");
                FailPipeConnect(GetBSControllerId());
            }
            for (auto& [hiveId, response] : HiveNodeStats) {
                if (response.Error(error)) {
                    AddProblem("hive-timeout");
                    ProcessResponses();
                    RequestDone();
                    FailPipeConnect(hiveId);
                }
            }
        }
        if (ev->Get()->Tag == TimeoutFinal) {
            for (auto& [nodeId, response] : SystemViewerResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : TabletViewerResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : PDiskViewerResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : VDiskViewerResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : PeersViewerResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : SystemStateResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : VDiskStateResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : PDiskStateResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : TabletStateResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            for (auto& [nodeId, response] : PeersStateResponse) {
                if (response.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
            if (WaitingForResponse()) {
                ReplyAndPassAway();
            }
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == TEvents::TEvUndelivered::ReasonActorUnknown && ev->Get()->SourceType == TEvViewer::TEvViewerRequest::EventType) {
            static const TString error = "Undelivered";
            TNodeId nodeId = ev.Get()->Cookie;
            FailViewerRequestsForNode(nodeId, error);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvNodeStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvHive::TEvResponseHiveNodeStats, Handle);
            hFunc(NSysView::TEvSysView::TEvGetGroupsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetStoragePoolsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetPDisksResponse, Handle);
            hFunc(TEvViewer::TEvViewerResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvents::TEvWakeup, HandleTimeout);
            hFunc(TEvents::TEvUndelivered, Undelivered);
        }
    }

    void ReplyAndPassAway() override {
        AddEvent("ReplyAndPassAway");
        ApplyEverything();
        NKikimrViewer::TNodesInfo json;
        json.SetVersion(Viewer->GetCapabilityVersion("/viewer/nodes"));
        json.SetFieldsAvailable(FieldsAvailable.to_string());
        json.SetFieldsRequired(FieldsRequired.to_string());
        if (NeedFilter) {
            json.SetNeedFilter(true);
        }
        if (NeedGroup) {
            json.SetNeedGroup(true);
        }
        if (NeedSort) {
            json.SetNeedSort(true);
        }
        if (NeedLimit) {
            json.SetNeedLimit(true);
        }
        json.SetTotalNodes(TotalNodes);
        json.SetFoundNodes(FoundNodes);
        if (MaximumDisksPerNode.has_value()) {
            json.SetMaximumDisksPerNode(MaximumDisksPerNode.value());
        }
        if (MaximumSlotsPerDisk.has_value()) {
            json.SetMaximumSlotsPerDisk(MaximumSlotsPerDisk.value());
        }
        if (NoDC) {
            json.SetNoDC(true);
        }
        if (NoRack) {
            json.SetNoRack(true);
        }
        for (auto problem : Problems) {
            json.AddProblems(problem);
        }
        if (NodeGroups.empty()) {
            for (TNode* node : NodeView) {
                NKikimrViewer::TNodeInfo& jsonNode = *json.AddNodes();
                if (FieldsAvailable.test(+ENodeFields::NodeInfo)) {
                    jsonNode.SetNodeId(node->GetNodeId());
                }
                if (node->Database) {
                    jsonNode.SetDatabase(node->Database);
                }
                if (node->UptimeSeconds) {
                    jsonNode.SetUptimeSeconds(node->UptimeSeconds);
                }
                if (node->Disconnected) {
                    jsonNode.SetDisconnected(node->Disconnected);
                }
                if (node->CpuUsage) {
                    jsonNode.SetCpuUsage(node->CpuUsage);
                }
                if (node->DiskSpaceUsage) {
                    jsonNode.SetDiskSpaceUsage(node->DiskSpaceUsage);
                }
                if (FieldsAvailable.test(+ENodeFields::Connections)) {
                    jsonNode.SetConnections(node->Connections);
                }
                if (FieldsAvailable.test(+ENodeFields::ConnectStatus)) {
                    jsonNode.SetConnectStatus(GetViewerFlag(node->ConnectStatus));
                }
                if (FieldsAvailable.test(+ENodeFields::NetworkUtilization)) {
                    jsonNode.SetNetworkUtilization(node->NetworkUtilization);
                    jsonNode.SetNetworkUtilizationMin(node->NetworkUtilizationMin);
                    jsonNode.SetNetworkUtilizationMax(node->NetworkUtilizationMax);
                }
                if (FieldsAvailable.test(+ENodeFields::ClockSkew)) {
                    jsonNode.SetClockSkewUs(node->ClockSkewUs);
                    jsonNode.SetClockSkewMinUs(node->ClockSkewMinUs);
                    jsonNode.SetClockSkewMaxUs(node->ClockSkewMaxUs);
                    if (FieldsAvailable.test(+ENodeFields::ReversePeers)) {
                        jsonNode.SetReverseClockSkewUs(node->ReverseClockSkewUs);
                    }
                }
                if (FieldsAvailable.test(+ENodeFields::PingTime)) {
                    jsonNode.SetPingTimeUs(node->PingTimeUs);
                    jsonNode.SetPingTimeMinUs(node->PingTimeMinUs);
                    jsonNode.SetPingTimeMaxUs(node->PingTimeMaxUs);
                    if (FieldsAvailable.test(+ENodeFields::ReversePeers)) {
                        jsonNode.SetReversePingTimeUs(node->ReversePingTimeUs);
                    }
                }
                if (FieldsAvailable.test(+ENodeFields::SendThroughput)) {
                    jsonNode.SetSendThroughput(node->SendThroughput);
                }
                if (FieldsAvailable.test(+ENodeFields::ReceiveThroughput)) {
                    jsonNode.SetReceiveThroughput(node->ReceiveThroughput);
                }
                if (FieldsAvailable.test(+ENodeFields::NodeInfo) || FieldsAvailable.test(+ENodeFields::SystemState)) {
                    *jsonNode.MutableSystemState() = std::move(node->SystemState);
                }
                if (FieldsAvailable.test(+ENodeFields::PDisks)) {
                    std::sort(node->PDisks.begin(), node->PDisks.end(), [](const NKikimrWhiteboard::TPDiskStateInfo& a, const NKikimrWhiteboard::TPDiskStateInfo& b) {
                        return a.path() < b.path();
                    });
                    for (NKikimrWhiteboard::TPDiskStateInfo& pDisk : node->PDisks) {
                        (*jsonNode.AddPDisks()) = std::move(pDisk);
                    }
                }
                if (FieldsAvailable.test(+ENodeFields::VDisks)) {
                    std::sort(node->VDisks.begin(), node->VDisks.end(), [](const NKikimrWhiteboard::TVDiskStateInfo& a, const NKikimrWhiteboard::TVDiskStateInfo& b) {
                        return VDiskIDFromVDiskID(a.vdiskid()) < VDiskIDFromVDiskID(b.vdiskid());
                    });
                    for (NKikimrWhiteboard::TVDiskStateInfo& vDisk : node->VDisks) {
                        (*jsonNode.AddVDisks()) = std::move(vDisk);
                    }
                }
                if (FieldsAvailable.test(+ENodeFields::Tablets)) {
                    std::sort(node->Tablets.begin(), node->Tablets.end(), [](const NKikimrViewer::TTabletStateInfo& a, const NKikimrViewer::TTabletStateInfo& b) {
                        return a.type() < b.type();
                    });
                    for (NKikimrViewer::TTabletStateInfo& tablet : node->Tablets) {
                        (*jsonNode.AddTablets()) = std::move(tablet);
                    }
                }
                if (FieldsAvailable.test(+ENodeFields::SendThroughput)) {
                    jsonNode.SetSendThroughput(node->SendThroughput);
                }
                if (FieldsAvailable.test(+ENodeFields::ReceiveThroughput)) {
                    jsonNode.SetReceiveThroughput(node->ReceiveThroughput);
                }
                if (FieldsRequired.test(+ENodeFields::Peers)) {
                    std::sort(node->Peers.begin(), node->Peers.end(), [](const NKikimrWhiteboard::TNodeStateInfo& a, const NKikimrWhiteboard::TNodeStateInfo& b) {
                        return a.peernodeid() < b.peernodeid();
                    });
                    for (NKikimrWhiteboard::TNodeStateInfo& peer : node->Peers) {
                        (*jsonNode.AddPeers()) = std::move(peer);
                    }
                }
                if (FieldsRequired.test(+ENodeFields::ReversePeers)) {
                    std::sort(node->ReversePeers.begin(), node->ReversePeers.end(), [](const NKikimrWhiteboard::TNodeStateInfo& a, const NKikimrWhiteboard::TNodeStateInfo& b) {
                        return a.nodeid() < b.nodeid();
                    });
                    for (NKikimrWhiteboard::TNodeStateInfo& peer : node->ReversePeers) {
                        (*jsonNode.AddReversePeers()) = std::move(peer);
                    }
                }
            }
        } else {
            for (const TNodeGroup& nodeGroup : NodeGroups) {
                NKikimrViewer::TNodeGroup& jsonNodeGroup = *json.AddNodeGroups();
                jsonNodeGroup.SetGroupName(nodeGroup.Name);
                jsonNodeGroup.SetNodeCount(nodeGroup.Nodes.size());
            }
        }
        AddEvent("RenderingResult");
        TStringStream out;
        Proto2Json(json, out, {
            .EnumMode = TProto2JsonConfig::EnumValueMode::EnumName,
            .MapAsObject = true,
            .StringifyNumbers = TProto2JsonConfig::EStringifyNumbersMode::StringifyInt64Always,
            .WriteNanAsString = true,
        });
        AddEvent("ResultReady");
        TBase::ReplyAndPassAway(GetHTTPOKJSON(out.Str()));
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            get:
                tags:
                  - viewer
                summary: Nodes info
                description: Information about nodes
                parameters:
                  - name: database
                    in: query
                    description: database name
                    required: false
                    type: string
                  - name: path
                    in: query
                    description: path to schema object
                    required: false
                    type: string
                  - name: node_id
                    in: query
                    description: node id
                    required: false
                    type: integer
                  - name: group_id
                    in: query
                    description: group id
                    required: false
                    type: integer
                  - name: pool
                    in: query
                    description: storage pool name
                    required: false
                    type: string
                  - name: type
                    in: query
                    description: >
                        return nodes of specific type:
                          * `static`
                          * `dynamic`
                          * `storage`
                          * `any`
                  - name: with
                    in: query
                    description: >
                        filter groups by missing or space:
                          * `missing`
                          * `space`
                  - name: storage
                    in: query
                    description: return storage info
                    required: false
                    type: boolean
                  - name: tablets
                    in: query
                    description: return tablets info
                    required: false
                    type: boolean
                  - name: problems_only
                    in: query
                    description: return only problem nodes
                    required: false
                    type: boolean
                  - name: uptime
                    in: query
                    description: return only nodes with less uptime in sec.
                    required: false
                    type: integer
                  - name: filter
                    in: query
                    description: filter nodes by id or host
                    required: false
                    type: string
                  - name: sort
                    in: query
                    description: >
                        sort by:
                          * `NodeId`
                          * `Host`
                          * `NodeName`
                          * `DC`
                          * `Rack`
                          * `Version`
                          * `Uptime`
                          * `Memory`
                          * `CPU`
                          * `LoadAverage`
                          * `Missing`
                          * `DiskSpaceUsage`
                          * `Database`
                          * `SystemState`
                          * `Connections`
                          * `ConnectStatus`
                          * `NetworkUtilization`
                          * `ClockSkew`
                          * `PingTime`
                          * `SendThroughput`
                          * `ReceiveThroughput`
                    required: false
                    type: string
                  - name: group
                    in: query
                    description: >
                        group by:
                          * `NodeId`
                          * `Host`
                          * `NodeName`
                          * `Database`
                          * `DiskSpaceUsage`
                          * `DC`
                          * `Rack`
                          * `Missing`
                          * `Uptime`
                          * `Version`
                          * `SystemState`
                          * `ConnectStatus`
                          * `NetworkUtilization`
                          * `ClockSkew`
                          * `PingTime`
                    required: false
                    type: string
                  - name: filter_group_by
                    in: query
                    description: >
                        group by:
                          * `NodeId`
                          * `Host`
                          * `NodeName`
                          * `Database`
                          * `DiskSpaceUsage`
                          * `DC`
                          * `Rack`
                          * `Missing`
                          * `Uptime`
                          * `Version`
                          * `SystemState`
                          * `ConnectStatus`
                          * `NetworkUtilization`
                          * `ClockSkew`
                          * `PingTime`
                    required: false
                    type: string
                  - name: filter_group
                    in: query
                    description: content for filter group by
                    required: false
                    type: string
                  - name: fields_required
                    in: query
                    description: >
                        list of fields required in response, the more - the heavier could be request. `all` for all fields:
                          * `NodeId` (always required)
                          * `SystemState`
                          * `PDisks`
                          * `VDisks`
                          * `Tablets`
                          * `Peers`
                          * `Host`
                          * `NodeName`
                          * `DC`
                          * `Rack`
                          * `Version`
                          * `Uptime`
                          * `Memory`
                          * `CPU`
                          * `LoadAverage`
                          * `Missing`
                          * `DiskSpaceUsage`
                          * `SubDomainKey`
                          * `DisconnectTime`
                          * `Database`
                          * `Connections`
                          * `ConnectStatus`
                          * `NetworkUtilization`
                          * `ClockSkew`
                          * `PingTime`
                          * `SendThroughput`
                          * `ReceiveThroughput`
                    required: false
                    type: string
                  - name: offset
                    in: query
                    description: skip N nodes
                    required: false
                    type: integer
                  - name: limit
                    in: query
                    description: limit to N nodes
                    required: false
                    type: integer
                  - name: timeout
                    in: query
                    description: timeout in ms
                    required: false
                    type: integer
                  - name: direct
                    in: query
                    description: fulfill request on the target node without redirects
                    required: false
                    type: boolean
                responses:
                    200:
                        description: OK
                        content:
                            application/json:
                                schema:
                                    type: object
                    400:
                        description: Bad Request
                    403:
                        description: Forbidden
                    504:
                        description: Gateway Timeout
                )___");
        node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TNodesInfo>();
        return node;
    }
};

}
