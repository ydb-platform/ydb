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
static constexpr ui32 DefaultErrorStateLimit = 60;
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
        options.FinalEvents.emplace_back(TEvSentinel::TEvConfigUpdated::EventType);
        options.FinalEvents.emplace_back(TEvSentinel::TEvStateUpdated::EventType);
        UNIT_ASSERT(DispatchEvents(options));
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

public:
    explicit TTestEnv(ui32 nodeCount, ui32 pdisks)
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
                Send(ev->Sender, TActorId(), response.Release());
                return TTestActorRuntime::EEventAction::DROP;
            }

            return prevObserver(ev);
        });

        State = new TCmsState;
        MockClusterInfo(State->ClusterInfo);
        State->CmsActorId = GetSender();

        Sentinel = Register(CreateSentinel(State));
        EnableScheduleForActor(Sentinel, true);
        WaitForSentinelBoot();
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

    TSet<TPDiskID> PDisksForRandomNode() const {
        auto nodes = State->ClusterInfo->AllNodes();
        size_t idx = RandomNumber(nodes.size() - 1);

        auto info = std::next(nodes.begin(), idx)->second;
        Y_ABORT_UNLESS(info);
        return info->PDisks;
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
                                    if (!(TFakeNodeWhiteboardService::NoisyBSCPipeCounter % 3) && (vec.empty() || *vec.begin())) {
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
