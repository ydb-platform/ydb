#pragma once

#include "cms_ut_common.h"
#include "sentinel.h"
#include "sentinel_impl.h"
#include "cms_impl.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCmsTest {

using namespace NCms;
using namespace NCms::NSentinel;
using TPDiskID = NCms::TPDiskID;

const auto& MockConfig = TFakeNodeWhiteboardService::Config;
auto& MockNodes = TFakeNodeWhiteboardService::Info;

static constexpr ui32 DefaultStateLimit = 5;
static constexpr ui32 GoodStateLimit = 5;
static constexpr ui32 DefaultErrorStateLimit = 60;
static constexpr TDuration ZeroGracePeriod = TDuration::Zero();
static constexpr TInstant ZeroBootTimestamp = TInstant::Zero();
auto DefaultStateLimits = NCms::TCmsSentinelConfig::DefaultStateLimits();

static constexpr NCms::EPDiskState ErrorStates[] = {
    NKikimrBlobStorage::TPDiskState::InitialFormatReadError,
    NKikimrBlobStorage::TPDiskState::InitialSysLogReadError,
    NKikimrBlobStorage::TPDiskState::InitialSysLogParseError,
    NKikimrBlobStorage::TPDiskState::InitialCommonLogReadError,
    NKikimrBlobStorage::TPDiskState::InitialCommonLogParseError,
    NKikimrBlobStorage::TPDiskState::CommonLoggerInitError,
    NKikimrBlobStorage::TPDiskState::OpenFileError,
    NKikimrBlobStorage::TPDiskState::ChunkQuotaError,
    NKikimrBlobStorage::TPDiskState::DeviceIoError,
    NKikimrBlobStorage::TPDiskState::Stopped,
};

constexpr NCms::EPDiskState FaultyStates[] = {
    NKikimrBlobStorage::TPDiskState::Initial,
    NKikimrBlobStorage::TPDiskState::InitialFormatRead,
    NKikimrBlobStorage::TPDiskState::InitialSysLogRead,
    NKikimrBlobStorage::TPDiskState::InitialCommonLogRead,
};

class TTestEnv: public TCmsTestEnv {
    static void MockClusterInfo(TClusterInfoPtr& info) {
        info.Reset(new TClusterInfo);

        for (const auto& pdisk : MockConfig.GetResponse().GetStatus(0).GetBaseConfig().GetPDisk()) {
            const ui32 nodeId = pdisk.GetNodeId();

            if (info->HasNode(nodeId)) {
                info->AddPDisk(pdisk);
                continue;
            }

            const TString name = TStringBuilder() << "node-" << nodeId;
            NActorsInterconnect::TNodeLocation location;
            location.SetRack(TStringBuilder() << "rack-" << (nodeId - 1) % 8 + 1);
            info->AddNode(TEvInterconnect::TNodeInfo(nodeId, name, name, name, 10000, TNodeLocation(location)), nullptr);
            info->AddPDisk(pdisk);
        }
    }

    void WaitForSentinelBoot() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back([this](IEventHandle& ev) {
            return ev.GetRecipientRewrite() == Sentinel
                && ev.GetTypeRewrite() == TEvSentinel::TEvConfigUpdated::EventType;
        });
        options.FinalEvents.emplace_back([this](IEventHandle& ev) {
            return ev.GetRecipientRewrite() == Sentinel
                && ev.GetTypeRewrite() == TEvSentinel::TEvStateUpdated::EventType;
        });
        UNIT_ASSERT(DispatchEvents(options));
    }

public:
    void RestartSentinel() {
        Send(new IEventHandle(Sentinel, TActorId(), new TEvents::TEvPoisonPill));
        WaitFor("Sentinel termination", [this] { return !FindActor(Sentinel); }, TDuration::Seconds(1));
        EnableScheduleForActor(Sentinel, false);

        Sentinel = Register(CreateSentinel(State));
        EnableScheduleForActor(Sentinel, true);
        WaitForSentinelBoot();
    }

    void UpdateConfig() {
        Send(new IEventHandle(Sentinel, TActorId(), new TEvSentinel::TEvUpdateConfig));
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvSentinel::TEvConfigUpdated::EventType);
        UNIT_ASSERT(DispatchEvents(options));
    }

    void SetBscMaintenanceStatus(const TPDiskID& id, EMaintenanceStatus::E status) {
        TGuard<TMutex> guard(TFakeNodeWhiteboardService::Mutex);
        auto* config = TFakeNodeWhiteboardService::Config.MutableResponse()->MutableStatus(0)->MutableBaseConfig();
        for (auto& pdisk : *config->MutablePDisk()) {
            if (pdisk.GetNodeId() == id.NodeId && pdisk.GetPDiskId() == id.DiskId) {
                pdisk.SetMaintenanceStatus(status);
                return;
            }
        }
        UNIT_FAIL("Target PDisk is missing from BSC config");
    }

    void CheckMaintenanceStatus(const TPDiskID& id, EMaintenanceStatus::E expectedActual,
            TMaybe<EMaintenanceStatus::E> expectedDesired = {}) {
        const auto edge = AllocateEdgeActor();
        auto request = MakeHolder<TEvCms::TEvGetSentinelStateRequest>();
        request->Record.SetShow(NKikimrCms::TGetSentinelStateRequest::ALL);
        auto* range = request->Record.AddRanges();
        range->SetBegin(id.NodeId);
        range->SetEnd(id.NodeId);
        Send(new IEventHandle(Sentinel, edge, request.Release()));

        const auto response = GrabEdgeEventRethrow<TEvCms::TEvGetSentinelStateResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus().GetCode(), NKikimrCms::TStatus::OK);
        for (const auto& pdisk : response->Get()->Record.GetPDisks()) {
            if (pdisk.GetId().GetNodeId() == id.NodeId && pdisk.GetId().GetDiskId() == id.DiskId) {
                UNIT_ASSERT_VALUES_EQUAL_C(pdisk.GetInfo().GetMaintenanceStatus(), static_cast<ui32>(expectedActual),
                    "Unexpected BSC maintenance status in Sentinel");
                if (expectedDesired) {
                    UNIT_ASSERT_VALUES_EQUAL_C(pdisk.GetInfo().GetDesiredMaintenanceStatus(), static_cast<ui32>(*expectedDesired),
                        "Unexpected CMS maintenance target");
                }
                return;
            }
        }
        UNIT_FAIL("Target PDisk is missing from Sentinel state");
    }

    TSet<TPDiskID> GetSentinelPDisks() {
        auto request = MakeHolder<TEvCms::TEvGetSentinelStateRequest>();
        request->Record.SetShow(NKikimrCms::TGetSentinelStateRequest::ALL);
        auto* range = request->Record.AddRanges();
        range->SetBegin(0);
        range->SetEnd(Max<ui32>());
        Send(new IEventHandle(Sentinel, GetSender(), request.Release()));

        TAutoPtr<IEventHandle> handle;
        auto* response = GrabEdgeEventRethrow<TEvCms::TEvGetSentinelStateResponse>(handle);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus().GetCode(), NKikimrCms::TStatus::OK);
        TSet<TPDiskID> result;
        for (const auto& pdisk : response->Record.GetPDisks()) {
            result.emplace(pdisk.GetId().GetNodeId(), pdisk.GetId().GetDiskId());
        }
        return result;
    }

    TActorId GetSentinelActor() const {
        return Sentinel;
    }

    void SetPDiskStateImpl(const TSet<TPDiskID>& ids, EPDiskState state) {
        for (const auto& id : ids) {
            Y_ABORT_UNLESS(MockNodes.contains(id.NodeId));
            auto& node = MockNodes.at(id.NodeId);

            Y_ABORT_UNLESS(node.PDiskStateInfo.contains(id.DiskId));
            auto& pdisk = node.PDiskStateInfo.at(id.DiskId);

            pdisk.SetState(state);
        }

        Send(new IEventHandle(Sentinel, TActorId(), new TEvSentinel::TEvUpdateState));
    }

    explicit TTestEnv(ui32 nodeCount, ui32 pdisks, NKikimrCms::TCmsConfig config = {})
        : TCmsTestEnv(nodeCount, pdisks)
    {
        SetLogPriority(NKikimrServices::CMS, NLog::PRI_DEBUG);

        SetScheduledEventFilter([this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev, TDuration, TInstant&) -> bool {
            if (ev->Recipient != Sentinel) {
                return true;
            }

            switch (ev->GetTypeRewrite()) {
                case TEvSentinel::TEvUpdateConfig::EventType:
                case TEvSentinel::TEvUpdateState::EventType:
                case TEvents::TEvWakeup::EventType:
                    return false;

                default:
                    return true;
            }
        });

        auto prevObserver = SetObserverFunc(&TTestActorRuntimeBase::DefaultObserverFunc);
        SetObserverFunc([this, prevObserver](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvCms::TEvClusterStateRequest::EventType) {
                auto response = MakeHolder<TEvCms::TEvClusterStateResponse>();
                auto& record = response->Record;
                if (State) {
                    record.MutableStatus()->SetCode(NKikimrCms::TStatus::OK);
                    for (auto [_, nodeInfo]: State->ClusterInfo->AllNodes()) {
                        NCms::TCms::AddHostState(State->ClusterInfo, *nodeInfo, record, State->ClusterInfo->GetTimestamp());
                    }
                }
                Send(ev->Sender, TActorId(), response.Release(), 0, true);
                return TTestActorRuntime::EEventAction::DROP;
            }

            return prevObserver(ev);
        });

        State = new TCmsState;

        auto* sentinelConfig = config.MutableSentinelConfig();
        if (!sentinelConfig->HasInitialDeploymentGracePeriod()) {
            sentinelConfig->SetInitialDeploymentGracePeriod(0);
        }
        State->Config.Deserialize(config);
        MockClusterInfo(State->ClusterInfo);
        State->CmsActorId = GetSender();

        Sentinel = Register(CreateSentinel(State));
        EnableScheduleForActor(Sentinel, true);
        WaitForSentinelBoot();
    }

    void MockBridgeModePiles(ui32 numPiles) {
        auto& nodes = State->ClusterInfo->AllNodes();
        for (auto& [id, info] : nodes) {
            info->PileId = id % numPiles;
            State->ClusterInfo->NodeIdToPileId[id] = *info->PileId;
        }
    }

    // bridge info as node warden reports it, with piles assigned the way MockBridgeModePiles() does; piles without the
    // state given are synchronized
    void MockBridgeInfo(ui32 numPiles, const std::vector<NKikimrBridge::TClusterState::EPileState>& states = {}) {
        auto info = std::make_shared<TBridgeInfo>();
        info->Piles.resize(numPiles);
        for (ui32 i = 0; i < numPiles; ++i) {
            auto& pile = info->Piles[i];
            pile.BridgePileId = TBridgePileId::FromPileIndex(i);
            pile.State = i < states.size() ? states[i] : NKikimrBridge::TClusterState::SYNCHRONIZED;
        }
        for (const auto& [id, _] : State->ClusterInfo->AllNodes()) {
            auto& pile = info->Piles[id % numPiles];
            pile.StaticNodeIds.push_back(id);
            info->StaticNodeIdToPile[id] = &pile;
        }
        State->BridgeInfo = std::move(info);
    }

    TPDiskID RandomPDiskID() const {
        const auto& config = MockConfig.GetResponse().GetStatus(0).GetBaseConfig();
        const auto& pdisk = config.GetPDisk(RandomNumber(config.PDiskSize() - 1));
        return TPDiskID(pdisk.GetNodeId(), pdisk.GetPDiskId());
    }

    TSet<TPDiskID> PDisksForRandomRack() const {
        auto nodes = State->ClusterInfo->AllNodes();
        size_t idx = RandomNumber(nodes.size() - 1);
        auto target = std::next(nodes.begin(), idx)->second;

        TString targetRack = target->Location.HasKey(TNodeLocation::TKeys::Rack)
            ? target->Location.GetRackId()
            : "";

        TSet<TPDiskID> res;
        for (const auto& [id, info] : nodes) {
            TString foundRack = info->Location.HasKey(TNodeLocation::TKeys::Rack)
                ? info->Location.GetRackId()
                : "";
            if (targetRack == foundRack) {
                std::copy(info->PDisks.begin(), info->PDisks.end(), std::inserter(res, res.begin()));
            }
        }
        return res;
    }

    TSet<TPDiskID> PDisksForRandomPile() const {
        auto nodes = State->ClusterInfo->AllNodes();
        size_t idx = RandomNumber(nodes.size() - 1);
        auto target = std::next(nodes.begin(), idx)->second;

        TSet<TPDiskID> res;
        for (const auto& [id, info] : nodes) {
            if (info->PileId == target->PileId) {
                std::copy(info->PDisks.begin(), info->PDisks.end(), std::inserter(res, res.begin()));
            }
        }
        return res;
    }

    TSet<TPDiskID> PDisksForRandomNode() const {
        auto nodes = State->ClusterInfo->AllNodes();
        size_t idx = RandomNumber(nodes.size() - 1);

        auto info = std::next(nodes.begin(), idx)->second;
        Y_ABORT_UNLESS(info);
        return info->PDisks;
    }

    void SetNodeFaulty(ui32 nodeId, bool faulty) {
        // Config refreshes must report the same markers as the direct notification.
        auto& markers = State->ClusterInfo->AllNodes().at(nodeId)->Markers;
        markers.clear();
        if (faulty) {
            markers.insert(NKikimrCms::EMarker::MARKER_DISK_FAULTY);
        }

        auto v = TVector<NCms::TEvSentinel::TEvUpdateHostMarkers::THostMarkers>();
        v.push_back({
            .NodeId = nodeId,
            .Markers = markers,
        });
        Send(new IEventHandle(Sentinel, TActorId(), new TEvSentinel::TEvUpdateHostMarkers(std::move(v))));
    }

    void SetPDiskState(const TSet<TPDiskID>& pdisks, EPDiskState state) {
        SetPDiskStateImpl(pdisks, state);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvSentinel::TEvStateUpdated::EventType, 1);
        UNIT_ASSERT(DispatchEvents(options));
    }

    void SetPDiskState(const TSet<TPDiskID>& pdisks, EPDiskState state, EPDiskStatus expectedStatus) {
        SetPDiskStateImpl(pdisks, state);

        bool stateUpdated = false;

        struct TPDiskUpdates {
            bool UpdateStatusRequested = false;
            ui32 IgnoredUpdateRequests = 0;
        };
        THashMap<TPDiskID, TPDiskUpdates, TPDiskIDHash> pdiskUpdates;
        for (const auto& id : pdisks) {
            pdiskUpdates[id] = {};// TPDiskUpdates {false, 0});
        }

        auto check = [&](IEventHandle& ev) {
            switch (ev.GetTypeRewrite()) {
            case TEvSentinel::TEvStateUpdated::EventType:
                stateUpdated = true;
                break;

            case TEvBlobStorage::TEvControllerConfigRequest::EventType:
                {
                    TGuard<TMutex> guard(TFakeNodeWhiteboardService::Mutex);
                    const auto& request = ev.Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record;

                    UNIT_ASSERT(request.HasRequest());
                    for (ui32 i = 0; i < request.GetRequest().CommandSize(); ++i) {
                        if (request.GetRequest().GetCommand(i).HasUpdateDriveStatus()) {
                            const auto& update = request.GetRequest().GetCommand(i).GetUpdateDriveStatus();
                            TPDiskID id(update.GetHostKey().GetNodeId(), update.GetPDiskId());

                            auto it = pdiskUpdates.find(id);
                            if (it != pdiskUpdates.end()) {
                                if (expectedStatus == update.GetStatus()) {
                                    auto& vec = TFakeNodeWhiteboardService::BSControllerResponsePatterns[id];
                                    bool bscWillDisconnect = TFakeNodeWhiteboardService::NoisyBSCPipe
                                        && (TFakeNodeWhiteboardService::NoisyBSCPipeCounter + 1) % 3;
                                    if (!bscWillDisconnect && (vec.empty() || *vec.begin())) {
                                        it->second.UpdateStatusRequested = true;
                                    } else {
                                        it->second.IgnoredUpdateRequests++;
                                    }
                                }
                            }
                        }
                    }
                }
                break;
            default:
                break;
            }
            bool allUpdateStatusRequestedOrIgnored = true;
            for (const auto& [id, info] : pdiskUpdates) {
                allUpdateStatusRequestedOrIgnored &= (info.UpdateStatusRequested || info.IgnoredUpdateRequests == 6);
            }
            return stateUpdated && pdiskUpdates.size() && allUpdateStatusRequestedOrIgnored;
        };

        TDispatchOptions options;
        options.FinalEvents.emplace_back(check);
        UNIT_ASSERT(DispatchEvents(options));
    }

private:
    TCmsStatePtr State;
    TActorId Sentinel;

}; // TTestEnv

}
