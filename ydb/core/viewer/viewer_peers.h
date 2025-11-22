#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include "viewer_helper.h"
#include <ydb/core/viewer/yaml/yaml.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

enum class EPeerFields : ui8 {
    PeerId,
    HostName,
    NodeName,
    DC,
    Rack,
    SystemState,
    Version,
    ConnectStatus,
    SendThroughput,
    ReceiveThroughput,
    NetworkUtilization,
    ClockSkew,
    PingTime,
    PileName,
    BytesSend,
    BytesReceived,
    COUNT,
};

constexpr ui8 operator+(EPeerFields e) { return static_cast<ui8>(e); }

class TJsonPeers : public TViewerPipeClient {
    using TThis = TJsonPeers;
    using TBase = TViewerPipeClient;
    using TNodeId = ui32;
    using TFieldsType = std::bitset<+EPeerFields::COUNT>;

    std::optional<TRequestResponse<TEvInterconnect::TEvNodesInfo>> NodesInfoResponse;
    bool NodesInfoResponseProcessed = false;
    std::optional<TRequestResponse<TEvWhiteboard::TEvNodeStateResponse>> NodeStateResponse;
    bool NodeStateResponseProcessed = false;

    int WhiteboardStateRequestsInFlight = 0;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvSystemStateResponse>> SystemStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvWhiteboard::TEvNodeStateResponse>> PeersStateResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> SystemViewerResponse;
    std::unordered_map<TNodeId, TRequestResponse<TEvViewer::TEvViewerResponse>> PeersViewerResponse;

    std::unordered_set<TNodeId> RestrictedNodeIds; // due to access rights
    TNodeId NodeId = 0;
    std::optional<std::size_t> Offset;
    std::optional<std::size_t> Limit;
    TString Filter;
    bool AllWhiteboardFields = false;

    using TGroupSortKey = std::variant<TString, ui64, float, int>;

    struct TPeer {
        TEvInterconnect::TNodeInfo NodeInfo;
        NKikimrWhiteboard::TSystemStateInfo SystemState;
        NKikimrWhiteboard::TNodeStateInfo Forward;
        NKikimrWhiteboard::TNodeStateInfo Reverse;

        bool IsGoodForReturning() { return Forward.GetConnected(); }
        TNodeId GetPeerId() const { return NodeInfo.NodeId; }

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

        TString GetNodeName() const { return SystemState.GetNodeName(); }

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

        TString GetPileName() const {
            if (NodeInfo.Location.GetBridgePileName()) {
                return NodeInfo.Location.GetBridgePileName().value_or("");
            }
            return SystemState.GetLocation().GetBridgePileName();
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

        bool IsStatic() const { return NodeInfo.IsStatic; }

        TString GetClockSkewForGroup() const {
            auto clockSkew = abs(Forward.GetClockSkewUs()) / 1000;
            if (clockSkew < 1) {
                return "<1ms";
            }
            if (clockSkew < 10) {
                return "1ms..10ms";
            }
            return "10ms+";
        }

        TString GetPingTimeForGroup() const {
            auto pingTime = Forward.GetPingTimeUs() / 1000;
            if (pingTime < 1) {
                return "<1ms";
            }
            if (pingTime < 10) {
                return "1ms..10ms";
            }
            return "10ms+";
        }

        TString GetNetworkUtilizationForGroup() const {
            //return TStringBuilder() << std::ceil(std::clamp<float>(NetworkUtilization, 0, 100) / 5) * 5 << '%';
            // we want 0%-95% groups instead of 5%-100% groups
            return TStringBuilder() << std::floor(std::clamp<float>(Forward.GetUtilization(), 0, 100) / 5) * 5 << '%';
        }

        TString GetGroupName(EPeerFields groupBy) const {
            TString groupName;
            switch (groupBy) {
            case EPeerFields::PeerId:
                groupName = ToString(GetPeerId());
                break;
            case EPeerFields::HostName:
                groupName = GetHostName();
                break;
            case EPeerFields::NodeName:
                groupName = GetNodeName();
                break;
            case EPeerFields::DC:
                groupName = GetDataCenter();
                break;
            case EPeerFields::Rack:
                groupName = GetRack();
                break;
            case EPeerFields::SystemState:
                groupName = NKikimrWhiteboard::EFlag_Name(SystemState.GetSystemState());
                break;
            case EPeerFields::ConnectStatus:
                groupName = NKikimrWhiteboard::EFlag_Name(Forward.GetConnectStatus());
                break;
            case EPeerFields::NetworkUtilization:
                groupName = GetNetworkUtilizationForGroup();
                break;
            case EPeerFields::ClockSkew:
                groupName = GetClockSkewForGroup();
                break;
            case EPeerFields::PingTime:
                groupName = GetPingTimeForGroup();
                break;
            case EPeerFields::PileName:
                groupName = GetPileName();
                break;
            default:
                break;
            }
            if (groupName.empty()) {
                groupName = "unknown";
            }
            return groupName;
        }

        TGroupSortKey GetGroupSortKey(EPeerFields groupBy) const {
            switch (groupBy) {
            case EPeerFields::PeerId:
            case EPeerFields::HostName:
            case EPeerFields::NodeName:
            case EPeerFields::DC:
            case EPeerFields::Rack:
                return GetGroupName(groupBy);
            case EPeerFields::SystemState:
                return static_cast<int>(SystemState.GetSystemState());
            case EPeerFields::ConnectStatus:
                return static_cast<int>(Forward.GetConnectStatus());
            case EPeerFields::NetworkUtilization:
                return Forward.GetUtilization();
            case EPeerFields::ClockSkew:
                return static_cast<int>(abs(Forward.GetClockSkewUs()) / 1000);
            case EPeerFields::PingTime:
                return Forward.GetPingTimeUs() / 1000;
            case EPeerFields::PileName:
                return GetPileName();
            default:
                return TString();
            }
        }

        void MergeFrom(const NKikimrWhiteboard::TSystemStateInfo& systemState) {
            SystemState.MergeFrom(systemState);
            Cleanup();
        }

        void MergeFrom(const NKikimrWhiteboard::TNodeStateInfo& nodeState) {
            Reverse.MergeFrom(nodeState);
        }
    };

    using TPeerData = std::vector<TPeer>;
    using TPeerView = std::deque<TPeer*>;

    struct TPeerGroup {
        TString Name;
        TGroupSortKey SortKey;
        TPeerView Peers;
    };

    TPeerData PeerData;
    TPeerView PeerView;
    std::vector<TPeerGroup> PeerGroups;
    std::unordered_map<TNodeId, TPeer*> PeersByNodeId;
    EPeerFields SortBy = EPeerFields::PeerId;
    bool ReverseSort = false;
    EPeerFields GroupBy = EPeerFields::PeerId;
    EPeerFields FilterGroupBy = EPeerFields::PeerId;
    TString FilterGroup;
    bool NeedFilter = false;
    bool NeedGroup = false;
    bool NeedSort = true; // by default
    bool NeedLimit = false;
    ui64 TotalPeers = 0;
    ui64 FoundPeers = 0;
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

    static EPeerFields ParseEPeerFields(TStringBuf field) {
        EPeerFields result = EPeerFields::COUNT;
        if (field == "PeerId" || field == "NodeId" || field == "Id") {
            result = EPeerFields::PeerId;
        } else if (field == "Host" || field == "HostName") {
            result = EPeerFields::HostName;
        } else if (field == "NodeName") {
            result = EPeerFields::NodeName;
        } else if (field == "DC") {
            result = EPeerFields::DC;
        } else if (field == "Rack") {
            result = EPeerFields::Rack;
        } else if (field == "Version") {
            result = EPeerFields::Version;
        } else if (field == "ConnectStatus") {
            result = EPeerFields::ConnectStatus;
        } else if (field == "NetworkUtilization") {
            result = EPeerFields::NetworkUtilization;
        } else if (field == "ClockSkew") {
            result = EPeerFields::ClockSkew;
        } else if (field == "PingTime") {
            result = EPeerFields::PingTime;
        } else if (field == "PileName") {
            result = EPeerFields::PileName;
        } else if (field == "SystemState") {
            result = EPeerFields::SystemState;
        } else if (field == "BytesSend") {
            result = EPeerFields::BytesSend;
        } else if (field == "BytesReceived") {
            result = EPeerFields::BytesReceived;
        }
        return result;
    }

  public:
    TJsonPeers(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev, "/viewer/peers")
    {
        Filter = Params.Get("filter");
        NodeId = FromStringWithDefault<ui32>(Params.Get("node_id"), 0);
        if (NodeId == 0) {
            NodeId = TlsActivationContext->ActorSystem()->NodeId;
        }
        NeedFilter = !Filter.empty();
        if (Params.Has("offset")) {
            Offset = FromStringWithDefault<ui32>(Params.Get("offset"), 0);
            NeedLimit = true;
        }
        if (Params.Has("limit")) {
            Limit = FromStringWithDefault<ui32>(Params.Get("limit"), std::numeric_limits<ui32>::max());
            NeedLimit = true;
        }
        TStringBuf sort = Params.Get("sort");
        if (sort) {
            NeedSort = true;
            if (sort.StartsWith("-") || sort.StartsWith("+")) {
                ReverseSort = (sort[0] == '-');
                sort.Skip(1);
            }
            SortBy = ParseEPeerFields(sort);
        }
        TStringBuf group = Params.Get("group");
        if (group) {
            NeedGroup = true;
            GroupBy = ParseEPeerFields(group);
            NeedSort = false;
            NeedLimit = false;
        }
        if (Params.Has("filter_group") && Params.Has("filter_group_by")) {
            FilterGroup = Params.Get("filter_group");
            FilterGroupBy = ParseEPeerFields(Params.Get("filter_group_by"));
            NeedFilter = true;
        }
    }

    void Bootstrap() override {
        if (TBase::NeedToRedirect()) {
            return;
        }
        if (IsDatabaseRequest() && !Viewer->CheckAccessViewer(TBase::GetRequest())) {
            auto nodes = GetDatabaseNodes();
            RestrictedNodeIds = std::unordered_set<TNodeId>(nodes.begin(), nodes.end());
            NeedFilter = true;
        }
        NodesInfoResponse = MakeRequest<TEvInterconnect::TEvNodesInfo>(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        {
            auto request = std::make_unique<TEvWhiteboard::TEvNodeStateRequest>();
            request->Record.AddFieldsRequired(-1);
            NodeStateResponse = MakeWhiteboardRequest(NodeId, request.release());
        }
        TBase::Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup);
    }

    void InvalidatePeers() { PeersByNodeId.clear(); }

    void RebuildPeersByNodeId() {
        PeersByNodeId.clear();
        for (TPeer* peer : PeerView) {
            PeersByNodeId.emplace(peer->GetPeerId(), peer);
        }
    }

    TPeer* FindPeer(TNodeId peerId) {
        if (PeersByNodeId.empty()) {
            RebuildPeersByNodeId();
        }
        auto itPeer = PeersByNodeId.find(peerId);
        if (itPeer != PeersByNodeId.end()) {
            return itPeer->second;
        }
        return nullptr;
    }

    bool FilterDone() const {
        return !NeedFilter;
    }

    void ApplyFilter() {
        if (!RestrictedNodeIds.empty()) {
            TPeerView peerView;
            for (TPeer* peer : PeerView) {
                if (RestrictedNodeIds.count(peer->GetPeerId()) > 0) {
                    peerView.push_back(peer);
                }
            }
            PeerView.swap(peerView);
            FoundPeers = TotalPeers = PeerView.size();
            InvalidatePeers();
            RestrictedNodeIds.clear();
            AddEvent("Restricted Filter Applied");
        }

        if (NeedFilter) {
            if (!Filter.empty()) {
                TVector<TString> filterWords;
                StringSplitter(Filter).SplitBySet(", ").SkipEmpty().Collect(&filterWords);
                TPeerView peerView;
                for (TPeer* peer : PeerView) {
                    for (const TString& word : filterWords) {
                        if (::ToString(peer->GetPeerId()).Contains(word)) {
                            peerView.push_back(peer);
                            break;
                        }
                        if (peer->GetHostName().Contains(word)) {
                            peerView.push_back(peer);
                            break;
                        }
                    }
                }
                PeerView.swap(peerView);
                Filter.clear();
                InvalidatePeers();
                AddEvent("Search Filter Applied");
            }
            if (!FilterGroup.empty()) {
                TPeerView peerView;
                for (TPeer* peer : PeerView) {
                    if (peer->GetGroupName(FilterGroupBy) == FilterGroup) {
                        peerView.push_back(peer);
                    }
                }
                PeerView.swap(peerView);
                FilterGroup.clear();
                InvalidatePeers();
                AddEvent("Group Filter Applied");
            }
            NeedFilter = !Filter.empty() || !FilterGroup.empty();
            FoundPeers = PeerView.size();
        }
    }

    void GroupCollection() {
        std::unordered_map<TString, size_t> peerGroups;
        PeerGroups.clear();
        for (TPeer* peer : PeerView) {
            auto gb = peer->GetGroupName(GroupBy);
            TPeerGroup* peerGroup = nullptr;
            auto it = peerGroups.find(gb);
            if (it == peerGroups.end()) {
                peerGroups.emplace(gb, PeerGroups.size());
                peerGroup = &PeerGroups.emplace_back();
                peerGroup->Name = gb;
                peerGroup->SortKey = peer->GetGroupSortKey(GroupBy);
            } else {
                peerGroup = &PeerGroups[it->second];
            }
            peerGroup->Peers.push_back(peer);
        }
    }

    void ApplyGroup() {
        if (FilterDone() && NeedGroup) {
            switch (GroupBy) {
            case EPeerFields::PeerId:
            case EPeerFields::HostName:
            case EPeerFields::NodeName:
            case EPeerFields::DC:
            case EPeerFields::Rack:
            case EPeerFields::PileName:
                GroupCollection();
                SortCollection(PeerGroups, [](const TPeerGroup& peerGroup) { return peerGroup.SortKey; });
                NeedGroup = false;
                break;
            case EPeerFields::Version:
            case EPeerFields::SystemState:
            case EPeerFields::ConnectStatus:
            case EPeerFields::NetworkUtilization:
            case EPeerFields::ClockSkew:
            case EPeerFields::PingTime:
                GroupCollection();
                SortCollection(PeerGroups, [](const TPeerGroup& peerGroup) { return peerGroup.SortKey; }, true);
                NeedGroup = false;
                break;
            case EPeerFields::COUNT:
            case EPeerFields::ReceiveThroughput:
            case EPeerFields::SendThroughput:
            case EPeerFields::BytesSend:
            case EPeerFields::BytesReceived:
                break;
            }
            AddEvent("Group Applied");
        }
    }

    void ApplySort() {
        if (FilterDone() && NeedSort) {
            switch (SortBy) {
            case EPeerFields::PeerId:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->GetPeerId(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::HostName:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->GetHostName(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::NodeName:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->GetNodeName(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::DC:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->NodeInfo.Location.GetDataCenterId(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::Rack:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->NodeInfo.Location.GetRackId(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::Version:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->SystemState.GetVersion(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::SystemState:
                SortCollection(PeerView, [](const TPeer* peer) { return static_cast<int>(peer->SystemState.GetSystemState()); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::SendThroughput:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->Forward.GetWriteThroughput(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::ReceiveThroughput:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->Reverse.GetWriteThroughput(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::NetworkUtilization:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->Forward.GetUtilization(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::ConnectStatus:
                SortCollection(PeerView, [](const TPeer* peer) { return static_cast<int>(peer->Forward.GetConnectStatus()); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::PingTime:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->Forward.GetPingTimeUs(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::ClockSkew:
                SortCollection(PeerView, [](const TPeer* peer) { return abs(peer->Forward.GetClockSkewUs()); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::PileName:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->GetPileName(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::BytesSend:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->Forward.GetBytesWritten(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::BytesReceived:
                SortCollection(PeerView, [](const TPeer* peer) { return peer->Reverse.GetBytesWritten(); }, ReverseSort);
                NeedSort = false;
                break;
            case EPeerFields::COUNT:
                break;
            }
            if (!NeedSort) {
                InvalidatePeers();
            }
            AddEvent("Sort Applied");
        }
    }

    void ApplyLimit() {
        if (FilterDone() && !NeedSort && !NeedGroup && NeedLimit) {
            if (Offset) {
                PeerView.erase(PeerView.begin(), PeerView.begin() + std::min(*Offset, PeerView.size()));
                InvalidatePeers();
            }
            if (Limit) {
                PeerView.resize(std::min(*Limit, PeerView.size()));
                InvalidatePeers();
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

    void ProcessResponses() {
        AddEvent("ProcessResponses");
        if (NodesInfoResponse) {
            if (NodesInfoResponse->IsDone() && !NodesInfoResponseProcessed) {
                if (NodesInfoResponse->IsOk()) {
                    bool seenDC = false;
                    bool seenRack = false;
                    for (const auto& ni : NodesInfoResponse->Get()->Nodes) {
                        if (ni.NodeId == NodeId) {
                            continue;
                        }
                        TPeer& peer = PeerData.emplace_back();
                        peer.NodeInfo = ni;
                        if (ni.Host && !peer.SystemState.GetHost()) {
                            peer.SystemState.SetHost(ni.Host);
                        }
                        if (ni.Location.GetDataCenterId() != 0) {
                            seenDC = true;
                        }
                        if (ni.Location.GetRackId() != 0) {
                            seenRack = true;
                        }
                    }
                    for (TPeer& peer : PeerData) {
                        PeerView.emplace_back(&peer);
                    }
                    FoundPeers = TotalPeers = PeerView.size();
                    NoDC = !seenDC;
                    NoRack = !seenRack;
                } else {
                    AddProblem("no-nodes-info");
                }
                NodesInfoResponse.reset();
            } else {
                return; // no further processing until we get node list
            }
            NodesInfoResponseProcessed = true;
        }

        if (NodeStateResponse && NodeStateResponse->IsDone() && TotalPeers > 0 && !NodeStateResponseProcessed) {
            if (NodeStateResponse->IsOk()) {
                std::vector<TNodeId> peerIds;
                RebuildPeersByNodeId();
                PeerView.clear();
                for (const auto& nodeStateInfo : NodeStateResponse->Get()->Record.GetNodeStateInfo()) {
                    TNodeId peerId = nodeStateInfo.GetPeerNodeId() ? nodeStateInfo.GetPeerNodeId() : FromStringWithDefault(TStringBuf(nodeStateInfo.GetPeerName()).Before(':'), 0);
                    if (peerId) {
                        TPeer* peer = FindPeer(peerId);
                        if (peer) {
                            peer->Forward.MergeFrom(nodeStateInfo);
                            if (peer->IsGoodForReturning()) {
                                peerIds.push_back(peerId);
                                PeerView.emplace_back(peer);
                            }
                        }
                    }
                }
                RebuildPeersByNodeId();
                FoundPeers = TotalPeers = PeerView.size();
                ApplyEverything();
                for (TPeer* peer : PeerView) {
                    if (peer->IsGoodForReturning()) {
                        peerIds.push_back(peer->GetPeerId());
                    }
                }
                if (!peerIds.empty()) {
                    SendWhiteboardRequests(peerIds);
                }
            } else {
                AddProblem("no-node-state-info");
            }
            NodeStateResponseProcessed = true;
        }
    }

    template <typename TWhiteboardEvent> void InitWhiteboardRequest(TWhiteboardEvent* request) {
        if (AllWhiteboardFields) {
            request->AddFieldsRequired(-1);
        }
    }

    template <> void InitWhiteboardRequest(NKikimrWhiteboard::TEvSystemStateRequest* request) {
        if (AllWhiteboardFields) {
            request->AddFieldsRequired(-1);
        } else {
            request->MutableFieldsRequired()->CopyFrom(GetDefaultWhiteboardFields<NKikimrWhiteboard::TSystemStateInfo>());
            request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresUsedFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kCoresTotalFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kRealNumberOfCpusFieldNumber);
            request->AddFieldsRequired(NKikimrWhiteboard::TSystemStateInfo::kNetworkUtilizationFieldNumber);
        }
    }

    template <> void InitWhiteboardRequest(NKikimrWhiteboard::TEvNodeStateRequest* request) {
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
            request->AddFieldsRequired(NKikimrWhiteboard::TNodeStateInfo::kSessionStateFieldNumber);
            request->AddFilterNodeId(NodeId);
        }
    }

    void SendWhiteboardRequests(const std::vector<TNodeId>& nodeIds) {
        for (TNodeId nodeId : nodeIds) {
            if (SystemStateResponse.count(nodeId) == 0) {
                auto request = new TEvWhiteboard::TEvSystemStateRequest();
                InitWhiteboardRequest(&request->Record);
                SystemStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request));
                ++WhiteboardStateRequestsInFlight;
            }
            if (PeersStateResponse.count(nodeId) == 0) {
                auto request = std::make_unique<TEvWhiteboard::TEvNodeStateRequest>();
                InitWhiteboardRequest(&request->Record);
                PeersStateResponse.emplace(nodeId, MakeWhiteboardRequest(nodeId, request.release()));
                ++WhiteboardStateRequestsInFlight;
            }
        }
    }

    void ProcessWhiteboard() {
        AddEvent("ProcessWhiteboard");
        for (const auto& [nodeId, response] : SystemStateResponse) {
            if (response.IsOk()) {
                const auto& systemState(response.Get()->Record);
                if (systemState.SystemStateInfoSize() > 0) {
                    TPeer* peer = FindPeer(nodeId);
                    if (peer) {
                        peer->MergeFrom(systemState.GetSystemStateInfo(0));
                        peer->SystemState.SetNodeId(nodeId);
                    }
                }
            }
        }
        for (const auto& [nodeId, response] : PeersStateResponse) {
            if (response.IsOk()) {
                const auto& nodeState(response.Get()->Record);
                TPeer* peer = FindPeer(nodeId);
                if (peer) {
                    for (const auto& nodeStateInfo : nodeState.GetNodeStateInfo()) {
                        if (nodeStateInfo.GetPeerNodeId() == NodeId) {
                            peer->MergeFrom(nodeStateInfo);
                        }
                    }
                }
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
        if (NodeStateResponse && !NodeStateResponse->IsDone() && nodeId == NodeId) {
            if (NodeStateResponse->Set(std::move(ev))) {
                ProcessResponses();
                RequestDone();
            }
        } else if (PeersStateResponse[nodeId].Set(std::move(ev))) {
            WhiteboardRequestDone();
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

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        TNodeId nodeId = ev->Get()->NodeId;
        TString error("NodeDisconnected");
        {
            auto itSystemStateResponse = SystemStateResponse.find(nodeId);
            if (itSystemStateResponse != SystemStateResponse.end()) {
                if (itSystemStateResponse->second.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
        }
        {
            auto itPeersStateResponse = PeersStateResponse.find(nodeId);
            if (itPeersStateResponse != PeersStateResponse.end()) {
                if (itPeersStateResponse->second.Error(error)) {
                    AddProblem("wb-incomplete");
                    WhiteboardRequestDone();
                }
            }
        }
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr&) {
        static const TString error("Timeout");
        for (auto& [nodeId, response] : SystemStateResponse) {
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
            AddEvent("WaitingForSomethingOnTimeout");
            ReplyAndPassAway();
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        static const TString error = "Undelivered";
        TNodeId nodeId = ev.Get()->Cookie;
            
        if (ev->Get()->SourceType == TEvWhiteboard::TEvNodeStateRequest::EventType) {
            if (PeersStateResponse[nodeId].Error(error)) {
                RequestDone();
            }
        }
        if (ev->Get()->SourceType == TEvWhiteboard::TEvSystemStateRequest::EventType) {
            if (SystemStateResponse[nodeId].Error(error)) {
                RequestDone();
            }
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvWhiteboard::TEvNodeStateResponse, Handle);
            hFunc(TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvents::TEvWakeup, HandleTimeout);
            hFunc(TEvents::TEvUndelivered, Undelivered);
        }
    }

    void ReplyAndPassAway() override {
        AddEvent("ReplyAndPassAway");
        ApplyEverything();
        NKikimrViewer::TPeersInfo json;
        json.SetVersion(Viewer->GetCapabilityVersion("/viewer/peers"));
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
        json.SetTotalPeers(TotalPeers);
        json.SetFoundPeers(FoundPeers);
        if (NoDC) {
            json.SetNoDC(true);
        }
        if (NoRack) {
            json.SetNoRack(true);
        }
        for (auto problem : Problems) {
            json.AddProblems(problem);
        }
        if (CachedDataMaxAge) {
            json.SetCachedDataMaxAge(CachedDataMaxAge.MilliSeconds());
        }
        if (PeerGroups.empty()) {
            for (TPeer* peer : PeerView) {
                NKikimrViewer::TPeerInfo& jsonPeer = *json.AddPeers();
                jsonPeer.MutableSystemState()->CopyFrom(peer->SystemState);
                jsonPeer.MutableForward()->CopyFrom(peer->Forward);
                jsonPeer.MutableReverse()->CopyFrom(peer->Reverse);
            }
        } else {
            for (const TPeerGroup& peerGroup : PeerGroups) {
                NKikimrViewer::TPeerGroup& jsonPeerGroup = *json.AddPeerGroups();
                jsonPeerGroup.SetGroupName(peerGroup.Name);
                jsonPeerGroup.SetPeerCount(peerGroup.Peers.size());
            }
        }
        AddEvent("RenderingResult");
        TStringStream jsonBody;
        Proto2Json(json, jsonBody);
        AddEvent("ResultRendered");
        if (Span) {
            Span.Attribute("result_size", TStringBuilder() << jsonBody.Size());
        }
        TBase::ReplyAndPassAway(GetHTTPOKJSON(jsonBody.Str()));
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
            get:
                tags:
                  - viewer
                summary: To get information about peers
                description: Information about peers
                parameters:
                  - name: node_id
                    in: query
                    description: node id
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
                          * `PeerId`
                          * `PeerName`
                          * `DC`
                          * `Rack`
                          * `PileName`
                          * `Version`
                          * `SystemState`
                          * `ConnectStatus`
                          * `NetworkUtilization`
                          * `ClockSkew`
                          * `PingTime`
                          * `BytesSend`
                          * `BytesReceived`
                    required: false
                    type: string
                  - name: group
                    in: query
                    description: >
                        group by:
                          * `NodeId`
                          * `Host`
                          * `NodeName`
                          * `PileName`
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
                          * `PeerId`
                          * `PeerName`
                          * `DC`
                          * `Rack`
                          * `PileName`
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
        node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TPeersInfo>();
        return node;
    } 
};

} // namespace NKikimr::NViewer
