#include "cms_impl.h"
#include "cms_state.h"
#include "sentinel.h"
#include "sentinel_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr::NCms {

#if defined LOG_T || \
    defined LOG_D || \
    defined LOG_I || \
    defined LOG_N || \
    defined LOG_W || \
    defined LOG_E || \
    defined LOG_C
#error log macro redefinition
#endif

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::CMS, "[Sentinel] [" << Name() << "] " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CMS, "[Sentinel] [" << Name() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::CMS, "[Sentinel] [" << Name() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CMS, "[Sentinel] [" << Name() << "] " << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::CMS, "[Sentinel] [" << Name() << "] " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::CMS, "[Sentinel] [" << Name() << "] " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::CMS, "[Sentinel] [" << Name() << "] " << stream)

namespace NSentinel {

/// TPDiskStatusComputer

TPDiskStatusComputer::TPDiskStatusComputer(const ui32& defaultStateLimit, const TLimitsMap& stateLimits)
    : DefaultStateLimit(defaultStateLimit)
    , StateLimits(stateLimits)
    , StateCounter(0)
{
}

void TPDiskStatusComputer::AddState(EPDiskState state) {
    if (StateCounter && state == State) {
        if (StateCounter != Max<ui64>()) {
            ++StateCounter;
        }
    } else {
        PrevState = std::exchange(State, state);
        StateCounter = 1;
    }
}

EPDiskStatus TPDiskStatusComputer::Compute(EPDiskStatus current, TString& reason) const {
    if (ForcedStatus) {
        reason = "Forced status";
        return *ForcedStatus;
    }

    if (!StateCounter) {
        reason = "Uninitialized StateCounter";
        return current;
    }

    auto it = StateLimits.find(State);
    const ui32 stateLimit = (it != StateLimits.end()) ? it->second : DefaultStateLimit;

    if (!stateLimit || StateCounter < stateLimit) {
        reason = TStringBuilder()
            <<  "PrevState# " << PrevState
            << " State# " << State
            << " StateCounter# " << StateCounter
            << " current# " << current;
        switch (PrevState) {
            case NKikimrBlobStorage::TPDiskState::Unknown:
                return current;
            default:
                return EPDiskStatus::INACTIVE;
        }
    }

    reason = TStringBuilder()
        <<  "PrevState# " << PrevState
        << " State# " << State
        << " StateCounter# " << StateCounter
        << " StateLimit# " << stateLimit;

    PrevState = State;

    switch (State) {
        case NKikimrBlobStorage::TPDiskState::Normal:
            return EPDiskStatus::ACTIVE;
        default:
            return EPDiskStatus::FAULTY;
    }
}

EPDiskState TPDiskStatusComputer::GetState() const {
    return State;
}

EPDiskState TPDiskStatusComputer::GetPrevState() const {
    return PrevState;
}

ui64 TPDiskStatusComputer::GetStateCounter() const {
    return StateCounter;
}

void TPDiskStatusComputer::Reset() {
    StateCounter = 0;
}

void TPDiskStatusComputer::SetForcedStatus(EPDiskStatus status) {
    ForcedStatus = status;
}

void TPDiskStatusComputer::ResetForcedStatus() {
    ForcedStatus.Clear();
}

/// TPDiskStatus

TPDiskStatus::TPDiskStatus(EPDiskStatus initialStatus, const ui32& defaultStateLimit, const TLimitsMap& stateLimits)
    : TPDiskStatusComputer(defaultStateLimit, stateLimits)
    , Current(initialStatus)
    , ChangingAllowed(true)
{
}

void TPDiskStatus::AddState(EPDiskState state) {
    TPDiskStatusComputer::AddState(state);
}

bool TPDiskStatus::IsChanged() const {
    TString unused;
    return Current != Compute(Current, unused);
}

void TPDiskStatus::ApplyChanges(TString& reason) {
    Current = Compute(Current, reason);
}

void TPDiskStatus::ApplyChanges() {
    TString unused;
    ApplyChanges(unused);
}

EPDiskStatus TPDiskStatus::GetStatus() const {
    return Current;
}

bool TPDiskStatus::IsNewStatusGood() const {
    TString unused;
    switch (Compute(Current, unused)) {
        case EPDiskStatus::INACTIVE:
        case EPDiskStatus::ACTIVE:
            return true;
        case EPDiskStatus::UNKNOWN:
        case EPDiskStatus::FAULTY:
        case EPDiskStatus::BROKEN:
        case EPDiskStatus::TO_BE_REMOVED:
        case EPDiskStatus::EDriveStatus_INT_MIN_SENTINEL_DO_NOT_USE_:
        case EPDiskStatus::EDriveStatus_INT_MAX_SENTINEL_DO_NOT_USE_:
            return false;
    }
}

bool TPDiskStatus::IsChangingAllowed() const {
    return ChangingAllowed;
}

void TPDiskStatus::AllowChanging() {
    if (!ChangingAllowed) {
        Reset();
    }

    ChangingAllowed = true;
}

void TPDiskStatus::DisallowChanging() {
    ChangingAllowed = false;
}

/// TPDiskInfo

TPDiskInfo::TPDiskInfo(EPDiskStatus initialStatus, const ui32& defaultStateLimit, const TLimitsMap& stateLimits)
    : TPDiskStatus(initialStatus, defaultStateLimit, stateLimits)
{
    Touch();
}

void TPDiskInfo::AddState(EPDiskState state) {
    TPDiskStatus::AddState(state);
    Touch();
}

/// TNodeInfo

bool TNodeInfo::HasFaultyMarker() const {
    return Markers.contains(NKikimrCms::MARKER_DISK_FAULTY)
        || Markers.contains(NKikimrCms::MARKER_DISK_BROKEN);
}

/// TClusterMap

TClusterMap::TClusterMap(TSentinelState::TPtr state)
    : State(state)
{
}

void TClusterMap::AddPDisk(const TPDiskID& id) {
    Y_ABORT_UNLESS(State->Nodes.contains(id.NodeId));
    const auto& location = State->Nodes[id.NodeId].Location;

    ByDataCenter[location.HasKey(TNodeLocation::TKeys::DataCenter) ? location.GetDataCenterId() : ""].insert(id);
    ByRoom[location.HasKey(TNodeLocation::TKeys::Module) ? location.GetModuleId() : ""].insert(id);
    ByRack[location.HasKey(TNodeLocation::TKeys::Rack) ? location.GetRackId() : ""].insert(id);
    NodeByRack[location.HasKey(TNodeLocation::TKeys::Rack) ? location.GetRackId() : ""].insert(id.NodeId);
}

/// TGuardian

TGuardian::TGuardian(TSentinelState::TPtr state, ui32 dataCenterRatio, ui32 roomRatio, ui32 rackRatio)
    : TClusterMap(state)
    , DataCenterRatio(dataCenterRatio)
    , RoomRatio(roomRatio)
    , RackRatio(rackRatio)
{
}

TClusterMap::TPDiskIDSet TGuardian::GetAllowedPDisks(const TClusterMap& all, TString& issues,
        TPDiskIgnoredMap& disallowed) const
{
    TPDiskIDSet result;
    TStringBuilder issuesBuilder;

    #define LOG_IGNORED(ratio) \
        issuesBuilder \
            << "Ignore state updates due to " << #ratio << "Ratio" \
            << ": changed# " << kv.second.size() \
            << ", total# " << all.By##ratio.at(kv.first).size() \
            << ", ratio# " << ratio##Ratio \
            << ", affected pdisks# " << JoinSeq(", ", kv.second) << Endl

    for (const auto& kv : ByDataCenter) {
        Y_ABORT_UNLESS(all.ByDataCenter.contains(kv.first));

        if (!kv.first || CheckRatio(kv, all.ByDataCenter, DataCenterRatio)) {
            result.insert(kv.second.begin(), kv.second.end());
        } else {
            LOG_IGNORED(DataCenter);
            for (auto& pdisk : kv.second) {
                disallowed.emplace(pdisk, NKikimrCms::TPDiskInfo::RATIO_BY_DATACENTER);
            }
        }
    }

    for (const auto& kv : ByRoom) {
        Y_ABORT_UNLESS(all.ByRoom.contains(kv.first));

        if (kv.first && !CheckRatio(kv, all.ByRoom, RoomRatio)) {
            LOG_IGNORED(Room);
            for (auto& pdisk : kv.second) {
                disallowed.emplace(pdisk, NKikimrCms::TPDiskInfo::RATIO_BY_ROOM);
            }
            EraseNodesIf(result, [&room = kv.second](const TPDiskID& id) {
                return room.contains(id);
            });
        }
    }

    for (const auto& kv : ByRack) {
        Y_ABORT_UNLESS(all.ByRack.contains(kv.first));
        // ignore check if there is only one node in a rack
        auto it = NodeByRack.find(kv.first);
        if (it != NodeByRack.end() && it->second.size() == 1) {
            continue;
        }
        if (kv.first && !CheckRatio(kv, all.ByRack, RackRatio)) {
            LOG_IGNORED(Rack);
            for (auto& pdisk : kv.second) {
                disallowed.emplace(pdisk, NKikimrCms::TPDiskInfo::RATIO_BY_RACK);
            }
            EraseNodesIf(result, [&rack = kv.second](const TPDiskID& id) {
                return rack.contains(id);
            });
        }
    }

    #undef LOG_IGNORED

    issues = issuesBuilder;
    return result;
}

/// Misc

IActor* CreateBSControllerPipe(TCmsStatePtr cmsState) {
    const ui64 bscId = MakeBSControllerID();

    NTabletPipe::TClientConfig config;
    config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
    return NTabletPipe::CreateClient(cmsState->CmsActorId, bscId, config);
}

/// Actors

template <typename TEvUpdated, typename TDerived>
class TUpdaterBase: public TActorBootstrapped<TDerived> {
public:
    using TBase = TUpdaterBase<TEvUpdated, TDerived>;

protected:
    void Reply() {
        this->Send(this->Parent, new TEvUpdated());
        this->PassAway();
    }

public:
    explicit TUpdaterBase(const TActorId& parent, TCmsStatePtr cmsState, TSentinelState::TPtr sentinelState)
        : Parent(parent)
        , CmsState(cmsState)
        , SentinelState(sentinelState)
        , Config(CmsState->Config.SentinelConfig)
    {
        for (auto& [_, info] : SentinelState->PDisks) {
            info->ClearTouched();
        }
    }

protected:
    const TActorId Parent;
    TCmsStatePtr CmsState;
    TSentinelState::TPtr SentinelState;
    const TCmsSentinelConfig& Config;

}; // TUpdaterBase

class TConfigUpdater: public TUpdaterBase<TEvSentinel::TEvConfigUpdated, TConfigUpdater> {
    enum class RetryCookie {
        BSC,
        CMS,
    };

    void MaybeReply() {
        if (SentinelState->ConfigUpdaterState.GotBSCResponse && SentinelState->ConfigUpdaterState.GotCMSResponse) {
            Reply();
        }
    }

    void RetryBSC() {
        ++SentinelState->ConfigUpdaterState.BSCAttempt;
        Schedule(Config.RetryUpdateConfig, new TEvents::TEvWakeup(static_cast<ui64>(RetryCookie::BSC)));
    }

    void RetryCMS() {
        ++SentinelState->ConfigUpdaterState.CMSAttempt;
        Schedule(Config.RetryUpdateConfig, new TEvents::TEvWakeup(static_cast<ui64>(RetryCookie::CMS)));
    }

    void OnRetry(TEvents::TEvWakeup::TPtr& ev) {
        switch (static_cast<RetryCookie>(ev->Get()->Tag)) {
            case RetryCookie::BSC:
                return RequestBSConfig();
            case RetryCookie::CMS:
                return RequestCMSClusterState();
            default:
                Y_ABORT("Unexpected case");
        }
    }

    void RequestBSConfig() {
        LOG_D("Request blobstorage config"
            << ": attempt# " << SentinelState->ConfigUpdaterState.BSCAttempt);

        if (!CmsState->BSControllerPipe) {
            CmsState->BSControllerPipe = this->Register(CreateBSControllerPipe(CmsState));
        }

        auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
        NTabletPipe::SendData(SelfId(), CmsState->BSControllerPipe, request.Release());
    }

    void RequestCMSClusterState() {
        LOG_D("Request CMS cluster state"
            << ": attempt# " << SentinelState->ConfigUpdaterState.CMSAttempt);
        // We aren't tracking delivery due to invariant that CMS always kills sentinel when dies itself
        Send(CmsState->CmsActorId, new TEvCms::TEvClusterStateRequest());
    }

    void Handle(TEvCms::TEvClusterStateResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvCms::TEvClusterStateResponse"
            << ": response# " << record.ShortDebugString());

        if (!record.HasStatus() || !record.GetStatus().HasCode() || record.GetStatus().GetCode() != NKikimrCms::TStatus::OK) {
            TString error = "<no description>";
            if (record.HasStatus()) {
                if (record.GetStatus().HasCode()) {
                    error = ToString(record.GetStatus().GetCode()) + " ";
                }
                if (record.GetStatus().HasReason()) {
                    error = record.GetStatus().GetReason();
                }
            }

            LOG_E("Unsuccesful response from CMS"
                << ": error# " << error);
            return RetryCMS();
        }

        if (record.HasState()) {
            SentinelState->Nodes.clear();
            for (const auto& host : record.GetState().GetHosts()) {
                if (host.HasNodeId() && host.HasLocation() && host.HasName()) {
                    THashSet<NKikimrCms::EMarker> markers;
                    for (auto marker : host.GetMarkers()) {
                        markers.insert(static_cast<NKikimrCms::EMarker>(marker));
                    }

                    SentinelState->Nodes.emplace(host.GetNodeId(), TNodeInfo{
                        .Host = host.GetName(),
                        .Location = NActors::TNodeLocation(host.GetLocation()),
                        .Markers = std::move(markers),
                    });
                }
            }
        }

        SentinelState->ConfigUpdaterState.GotCMSResponse = true;
        MaybeReply();
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvBlobStorage::TEvControllerConfigResponse"
            << ": response# " << response.ShortDebugString());

        if (!response.GetSuccess() || !response.StatusSize() || !response.GetStatus(0).GetSuccess()) {
            TString error = "<no description>";
            if (response.StatusSize()) {
                error = response.GetStatus(0).GetErrorDescription();
            }

            LOG_E("Unsuccesful response from BSC"
                << ": size# " << response.StatusSize()
                << ", error# " << error);
            RetryBSC();
        } else {
            auto& pdisks = SentinelState->PDisks;

            for (const auto& pdisk : response.GetStatus(0).GetBaseConfig().GetPDisk()) {
                TPDiskID id(pdisk.GetNodeId(), pdisk.GetPDiskId());

                if (pdisks.contains(id)) {
                    pdisks.at(id)->Touch();
                    continue;
                }

                pdisks.emplace(id, new TPDiskInfo(pdisk.GetDriveStatus(), Config.DefaultStateLimit, Config.StateLimits));
            }

            SentinelState->ConfigUpdaterState.GotBSCResponse = true;
            MaybeReply();
        }
    }

    void OnPipeDisconnected() {
        LOG_E("Pipe to BSC disconnected");
        RetryBSC();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SENTINEL_CONFIG_UPDATER_ACTOR;
    }

    static TStringBuf Name() {
        return "ConfigUpdater"sv;
    }

    using TBase::TBase;

    void Bootstrap() {
        RequestBSConfig();
        RequestCMSClusterState();
        Become(&TThis::StateWork);
    }

    void PassAway() override {
        SentinelState->PrevConfigUpdaterState = SentinelState->ConfigUpdaterState;
        SentinelState->ConfigUpdaterState.Clear();
        TBase::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvSentinel::TEvBSCPipeDisconnected, OnPipeDisconnected);

            hFunc(TEvCms::TEvClusterStateResponse, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);

            hFunc(TEvents::TEvWakeup, OnRetry);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

}; // TConfigUpdater

class TStateUpdater: public TUpdaterBase<TEvSentinel::TEvStateUpdated, TStateUpdater> {
    using TEvWhiteboard = NNodeWhiteboard::TEvWhiteboard;

    static EPDiskState SafePDiskState(EPDiskState state) {
        switch (state) {
            case NKikimrBlobStorage::TPDiskState::Initial:
            case NKikimrBlobStorage::TPDiskState::InitialFormatRead:
            case NKikimrBlobStorage::TPDiskState::InitialFormatReadError:
            case NKikimrBlobStorage::TPDiskState::InitialSysLogRead:
            case NKikimrBlobStorage::TPDiskState::InitialSysLogReadError:
            case NKikimrBlobStorage::TPDiskState::InitialSysLogParseError:
            case NKikimrBlobStorage::TPDiskState::InitialCommonLogRead:
            case NKikimrBlobStorage::TPDiskState::InitialCommonLogReadError:
            case NKikimrBlobStorage::TPDiskState::InitialCommonLogParseError:
            case NKikimrBlobStorage::TPDiskState::CommonLoggerInitError:
            case NKikimrBlobStorage::TPDiskState::Normal:
            case NKikimrBlobStorage::TPDiskState::OpenFileError:
            case NKikimrBlobStorage::TPDiskState::ChunkQuotaError:
            case NKikimrBlobStorage::TPDiskState::DeviceIoError:
                return state;
            default:
                LOG_C("Unknown pdisk state: " << (ui32)state);
                return NKikimrBlobStorage::TPDiskState::Unknown;
        }
    }

    bool AcceptNodeReply(ui32 nodeId) {
        auto it = SentinelState->StateUpdaterWaitNodes.find(nodeId);
        if (it == SentinelState->StateUpdaterWaitNodes.end()) {
            return false;
        }

        SentinelState->StateUpdaterWaitNodes.erase(it);
        return true;
    }

    void MaybeReply() {
        if (SentinelState->StateUpdaterWaitNodes) {
            return;
        }

        Reply();
    }

    void MarkNodePDisks(ui32 nodeId, EPDiskState state, bool skipTouched = false) {
        auto it = SentinelState->PDisks.lower_bound(TPDiskID(nodeId, 0));
        while (it != SentinelState->PDisks.end() && it->first.NodeId == nodeId) {
            if (skipTouched && it->second->IsTouched()) {
                ++it;
                continue;
            }

            Y_ABORT_UNLESS(!it->second->IsTouched());
            it->second->AddState(state);
            ++it;
        }
    }

    void RequestPDiskState(ui32 nodeId) {
        const TActorId wbId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
        const ui32 flags = IEventHandle::FlagTrackDelivery;

        LOG_D("Request pdisks state"
            << ": nodeId# " << nodeId
            << ", wbId# " << wbId);
        Send(wbId, new TEvWhiteboard::TEvPDiskStateRequest(), flags, nodeId);
    }

    void Handle(TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        const ui32 nodeId = ev->Sender.NodeId();
        const auto& record = ev->Get()->Record;

        LOG_D("Handle TEvWhiteboard::TEvPDiskStateResponse"
            << ": nodeId# " << nodeId
            << ", response# " << record.ShortDebugString());

        if (!AcceptNodeReply(nodeId)) {
            LOG_W("PDisk info from unknown node"
                << ": nodeId# " << nodeId);
            return;
        }

        if (!record.PDiskStateInfoSize()) {
            LOG_E("There is no pdisk info"
                << ": nodeId# " << nodeId);
            MarkNodePDisks(nodeId, NKikimrBlobStorage::TPDiskState::Missing);
        } else {
            for (const auto& info : record.GetPDiskStateInfo()) {
                auto it = SentinelState->PDisks.find(TPDiskID(nodeId, info.GetPDiskId()));
                if (it == SentinelState->PDisks.end()) {
                    continue;
                }

                const auto safeState = SafePDiskState(info.GetState());
                LOG_T("SafePDiskState"
                    << ": pdiskId# " << it->first
                    << ", original# " << (ui32)info.GetState()
                    << ", safeState# " << safeState);

                it->second->AddState(safeState);
            }

            MarkNodePDisks(nodeId, NKikimrBlobStorage::TPDiskState::Missing, true);
        }

        MaybeReply();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        using EReason = TEvents::TEvUndelivered::EReason;

        const ui32 nodeId = ev->Cookie;
        const EReason reason = ev->Get()->Reason;

        LOG_D("Handle TEvents::TEvUndelivered"
            << ": nodeId# " << nodeId
            << ", sourceType# " << ev->Get()->SourceType
            << ", reason# " << reason);

        if (!AcceptNodeReply(nodeId)) {
            LOG_W("Undelivered to unknown node"
                << ": nodeId# " << nodeId);
            return;
        }

        LOG_E("Cannot get pdisks state"
            << ": nodeId# " << nodeId
            << ", reason# " << reason);

        switch (reason) {
            case EReason::Disconnected:
                MarkNodePDisks(nodeId, NKikimrBlobStorage::TPDiskState::NodeDisconnected);
                break;

            default:
                MarkNodePDisks(nodeId, NKikimrBlobStorage::TPDiskState::Unknown);
                break;
        }

        MaybeReply();
    }

    void TimedOut() {
        LOG_E("Timed out"
            << ": timeout# " << Config.UpdateStateTimeout);

        while (SentinelState->StateUpdaterWaitNodes) {
            const ui32 nodeId = *SentinelState->StateUpdaterWaitNodes.begin();

            MarkNodePDisks(nodeId, NKikimrBlobStorage::TPDiskState::Timeout);
            AcceptNodeReply(nodeId);
        }

        MaybeReply();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SENTINEL_STATE_UPDATER_ACTOR;
    }

    static TStringBuf Name() {
        return "StateUpdater"sv;
    }

    using TBase::TBase;

    void Bootstrap() {
        for (const auto& [id, _] : SentinelState->PDisks) {
            if (SentinelState->StateUpdaterWaitNodes.insert(id.NodeId).second) {
                RequestPDiskState(id.NodeId);
            }
        }

        Become(&TThis::StateWork, Config.UpdateStateTimeout, new TEvSentinel::TEvTimeout());
    }

    void PassAway() override {
        SentinelState->StateUpdaterWaitNodes.clear();
        TBase::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvSentinel::TEvTimeout, TimedOut);

            hFunc(TEvWhiteboard::TEvPDiskStateResponse, Handle);

            hFunc(TEvents::TEvUndelivered, Handle);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

}; // TStateUpdater

class TSentinel: public TActorBootstrapped<TSentinel> {
    struct TCounters {
        using TDynamicCounters = ::NMonitoring::TDynamicCounters;
        using TDynamicCounterPtr = ::NMonitoring::TDynamicCounterPtr;

        TDynamicCounterPtr Group;
        TDynamicCounters::TCounterPtr PDisksTotal;
        TDynamicCounters::TCounterPtr PDisksPendingChange;
        TDynamicCounters::TCounterPtr PDisksChanged;
        TDynamicCounters::TCounterPtr PDisksNotChanged;

        explicit TCounters(TDynamicCounterPtr counters)
            : Group(counters)
            , PDisksTotal(counters->GetCounter("PDisksTotal", false))
            , PDisksPendingChange(counters->GetCounter("PDisksPendingChange", true))
            , PDisksChanged(counters->GetCounter("PDisksChanged", true))
            , PDisksNotChanged(counters->GetCounter("PDisksNotChanged", true))
        {
        }

        ~TCounters() {
            Group->ResetCounters();
        }
    };

    struct TUpdaterState {
        TActorId Id;
        TInstant StartedAt;
        bool Delayed;

        void Clear() {
            Id = TActorId();
            StartedAt = TInstant::Zero();
            Delayed = false;
        }
    };

    struct TUpdaterInfo: public TUpdaterState {
        TUpdaterState PrevState;

        TUpdaterInfo() {
            PrevState.Clear();
            TUpdaterState::Clear();
        }

        void Start(const TActorId& id, const TInstant& now) {
            Id = id;
            StartedAt = now;
            Delayed = false;
        }

        void Clear() {
            PrevState = *this;
            TUpdaterState::Clear();
        }
    };

    static TInstant Now() {
        return TlsActivationContext->Now();
    }

    template <typename TUpdater>
    void StartUpdater(TUpdaterInfo& updater) {
        if (ConfigUpdater.Id || StateUpdater.Id) {
            LOG_I(TUpdater::Name() << " was delayed");
            updater.Delayed = true;
            return;
        }

        LOG_D("Start " << TUpdater::Name());
        updater.Start(RegisterWithSameMailbox(new TUpdater(SelfId(), CmsState, SentinelState)), Now());
    }

    template <typename TEvThis, typename TUpdaterThat>
    void ScheduleUpdate(TUpdaterInfo& updaterThis, const TDuration& intervalThis, TUpdaterInfo& updaterThat) {
        TDuration elapsed = Now() - updaterThis.StartedAt;
        TDuration delay;

        if (elapsed > intervalThis) {
            delay = TDuration::MilliSeconds(1);
        } else {
            delay = intervalThis - elapsed;
        }

        updaterThis.Clear();
        Schedule(delay, new TEvThis());

        if (updaterThat.Delayed) {
            StartUpdater<TUpdaterThat>(updaterThat);
        }
    }

    void RemoveUntouched() {
        EraseNodesIf(SentinelState->PDisks, [](const auto& kv) {
            return !kv.second->IsTouched();
        });
    }

    void EnsureAllTouched() const {
        Y_ABORT_UNLESS(AllOf(SentinelState->PDisks, [](const auto& kv) {
            return kv.second->IsTouched();
        }));
    }

    void LogStatusChange(const TPDiskID& id, EPDiskStatus status, EPDiskStatus requiredStatus, const TString& reason) {
        auto ev = MakeHolder<TCms::TEvPrivate::TEvLogAndSend>();

        ev->LogData.SetRecordType(NKikimrCms::TLogRecordData::PDISK_MONITOR_ACTION);
        auto& action = *ev->LogData.MutablePDiskMonitorAction();
        id.Serialize(action.MutablePDiskId());
        action.SetCurrentStatus(status);
        action.SetRequiredStatus(requiredStatus);

        Y_ABORT_UNLESS(SentinelState->Nodes.contains(id.NodeId));
        action.SetHost(SentinelState->Nodes[id.NodeId].Host);

        if (reason) {
            action.SetReason(reason);
        }

        Send(CmsState->CmsActorId, std::move(ev));
    }

    void UpdateConfig() {
        LOG_D("UpdateConfig");
        StartUpdater<TConfigUpdater>(ConfigUpdater);
    }

    void OnConfigUpdated() {
        LOG_D("Config was updated in " << (Now() - ConfigUpdater.StartedAt));

        RemoveUntouched();
        *Counters->PDisksTotal = SentinelState->PDisks.size();

        ScheduleUpdate<TEvSentinel::TEvUpdateConfig, TStateUpdater>(
            ConfigUpdater, Config.UpdateConfigInterval, StateUpdater
        );
    }

    void UpdateState() {
        LOG_D("UpdateState");
        StartUpdater<TStateUpdater>(StateUpdater);
    }

    void OnStateUpdated() {
        LOG_D("State was updated in " << (Now() - StateUpdater.StartedAt));

        EnsureAllTouched();

        if (SentinelState->Nodes.empty()) {
            LOG_C("Missing cluster info");
            ScheduleUpdate<TEvSentinel::TEvUpdateState, TConfigUpdater>(
                StateUpdater, Config.UpdateStateInterval, ConfigUpdater
            );

            return;
        }

        TClusterMap all(SentinelState);
        TGuardian changed(SentinelState, Config.DataCenterRatio, Config.RoomRatio, Config.RackRatio);
        TClusterMap::TPDiskIDSet alwaysAllowed;

        for (auto& pdisk : SentinelState->PDisks) {
            const TPDiskID& id = pdisk.first;
            TPDiskInfo& info = *(pdisk.second);

            auto it = SentinelState->Nodes.find(id.NodeId);
            if (it == SentinelState->Nodes.end()) {
                LOG_E("Missing node info"
                    << ": pdiskId# " << id);
                info.IgnoreReason = NKikimrCms::TPDiskInfo::MISSING_NODE;
                continue;
            }

            if (it->second.HasFaultyMarker()) {
                info.SetForcedStatus(EPDiskStatus::FAULTY);
            } else {
                info.ResetForcedStatus();
            }

            all.AddPDisk(id);
            if (info.IsChanged()) {
                if (info.IsNewStatusGood()) {
                    alwaysAllowed.insert(id);
                } else {
                    changed.AddPDisk(id);
                }
            } else {
                info.AllowChanging();
            }
        }

        TString issues;
        TClusterMap::TPDiskIgnoredMap disallowed;
        TClusterMap::TPDiskIDSet allowed = changed.GetAllowedPDisks(all, issues, disallowed);
        std::move(alwaysAllowed.begin(), alwaysAllowed.end(), std::inserter(allowed, allowed.begin()));

        // we ignore all previous unhandled requests
        // usually it will never happen with correct config
        SentinelState->ChangeRequests.clear();

        for (const auto& id : allowed) {
            Y_ABORT_UNLESS(SentinelState->PDisks.contains(id));
            TPDiskInfo::TPtr info = SentinelState->PDisks.at(id);

            info->IgnoreReason = NKikimrCms::TPDiskInfo::NOT_IGNORED;

            if (!info->IsChangingAllowed()) {
                info->AllowChanging();
                continue;
            }

            const EPDiskStatus status = info->GetStatus();
            TString reason;
            info->ApplyChanges(reason);
            const EPDiskStatus requiredStatus = info->GetStatus();

            LOG_N("PDisk status changed"
                << ": pdiskId# " << id
                << ", status# " << status
                << ", required status# " << requiredStatus
                << ", reason# " << reason
                << ", dry run# " << Config.DryRun);
            LogStatusChange(id, status, requiredStatus, reason);

            if (!Config.DryRun) {
                SentinelState->ChangeRequests.emplace(id, info);
                (*Counters->PDisksPendingChange)++;
            }
        }

        for (const auto& [id, reason] : disallowed) {
            Y_ABORT_UNLESS(SentinelState->PDisks.contains(id));
            auto& pdisk = SentinelState->PDisks.at(id);
            pdisk->DisallowChanging();
            pdisk->IgnoreReason = reason;
        }

        if (issues) {
            LOG_W(issues);
        }

        ScheduleUpdate<TEvSentinel::TEvUpdateState, TConfigUpdater>(
            StateUpdater, Config.UpdateStateInterval, ConfigUpdater
        );

        SendBSCRequests();
    }

    void SendBSCRequests() {
        if (SentinelState->ChangeRequests.empty()) {
            return;
        }

        if (!CmsState->BSControllerPipe) {
            CmsState->BSControllerPipe = Register(CreateBSControllerPipe(CmsState));
        }

        LOG_D("Change pdisk status"
            << ": requestsSize# " << SentinelState->ChangeRequests.size());

        auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        for (const auto& [id, info] : SentinelState->ChangeRequests) {
            auto& command = *request->Record.MutableRequest()->AddCommand()->MutableUpdateDriveStatus();
            command.MutableHostKey()->SetNodeId(id.NodeId);
            command.SetPDiskId(id.DiskId);
            command.SetStatus(info->GetStatus());
        }
        request->Record.MutableRequest()->SetIgnoreDisintegratedGroupsChecks(true);

        NTabletPipe::SendData(SelfId(), CmsState->BSControllerPipe, request.Release(), ++SentinelState->ChangeRequestId);
    }

    void Handle(TEvSentinel::TEvUpdateHostMarkers::TPtr& ev) {
        for (auto& [nodeId, markers] : ev->Get()->HostMarkers) {
            auto it = SentinelState->Nodes.find(nodeId);
            if (it == SentinelState->Nodes.end()) {
                // markers will be updated upon next ConfigUpdate iteration
                continue;
            }

            it->second.Markers = std::move(markers);
        }
    }

    void Handle(TEvCms::TEvGetSentinelStateRequest::TPtr& ev) {
        const auto& reqRecord = ev->Get()->Record;

        const auto show = reqRecord.HasShow()
            ? reqRecord.GetShow()
            : NKikimrCms::TGetSentinelStateRequest::UNHEALTHY;

        TMap<ui32, ui32> ranges = {{1, 20}};
        if (reqRecord.RangesSize() > 0) {
            ranges.clear();
            for (const auto& range : reqRecord.GetRanges()) {
                if (range.HasBegin() && range.HasEnd()) {
                    ranges.emplace(range.GetBegin(), range.GetEnd());
                }
            }
        }

        auto checkRanges = [&ranges](ui32 nodeId) {
            auto next = ranges.upper_bound(nodeId);
            if (next != ranges.begin()) {
                --next;
                return next->second >= nodeId;
            }

            return false;
        };

        auto filterByStatus = [](const TPDiskInfo& info, NKikimrCms::TGetSentinelStateRequest::EShow filter) {
            switch (filter) {
                case NKikimrCms::TGetSentinelStateRequest::UNHEALTHY:
                    return info.GetState() != NKikimrBlobStorage::TPDiskState::Normal
                        || info.GetStatus() != EPDiskStatus::ACTIVE;
                case NKikimrCms::TGetSentinelStateRequest::SUSPICIOUS:
                    return info.GetState() != NKikimrBlobStorage::TPDiskState::Normal
                        || info.GetStatus() != EPDiskStatus::ACTIVE
                        || !info.IsTouched()
                        || !info.IsChangingAllowed();
                default:
                    return true;
            }
        };

        auto response = MakeHolder<TEvCms::TEvGetSentinelStateResponse>();

        auto& record = response->Record;
        record.MutableStatus()->SetCode(NKikimrCms::TStatus::OK);
        Config.Serialize(*record.MutableSentinelConfig());

        auto serializeUpdater = [](const auto& updater, auto* out){
            out->SetActorId(updater.Id.ToString());
            out->SetStartedAt(updater.StartedAt.ToString());
            out->SetDelayed(updater.Delayed);
        };

        if (SentinelState) {
            auto& stateUpdater = *record.MutableStateUpdater();
            serializeUpdater(StateUpdater, stateUpdater.MutableUpdaterInfo());
            serializeUpdater(StateUpdater.PrevState, stateUpdater.MutablePrevUpdaterInfo());
            for (const auto& waitNode : SentinelState->StateUpdaterWaitNodes) {
                stateUpdater.AddWaitNodes(waitNode);
            }

            auto& configUpdater = *record.MutableConfigUpdater();
            serializeUpdater(ConfigUpdater, configUpdater.MutableUpdaterInfo());
            serializeUpdater(ConfigUpdater.PrevState, configUpdater.MutablePrevUpdaterInfo());
            configUpdater.SetBSCAttempt(SentinelState->ConfigUpdaterState.BSCAttempt);
            configUpdater.SetPrevBSCAttempt(SentinelState->PrevConfigUpdaterState.BSCAttempt);
            configUpdater.SetCMSAttempt(SentinelState->ConfigUpdaterState.CMSAttempt);
            configUpdater.SetPrevCMSAttempt(SentinelState->PrevConfigUpdaterState.CMSAttempt);

            for (const auto& [id, info] : SentinelState->PDisks) {
                if (filterByStatus(*info, show) && checkRanges(id.NodeId)) {
                    auto& entry = *record.AddPDisks();
                    entry.MutableId()->SetNodeId(id.NodeId);
                    entry.MutableId()->SetDiskId(id.DiskId);
                    entry.MutableInfo()->SetState(info->GetState());
                    entry.MutableInfo()->SetPrevState(info->GetPrevState());
                    entry.MutableInfo()->SetStateCounter(info->GetStateCounter());
                    entry.MutableInfo()->SetStatus(info->ActualStatus);
                    entry.MutableInfo()->SetDesiredStatus(info->GetStatus());
                    entry.MutableInfo()->SetPrevDesiredStatus(info->PrevStatus);
                    entry.MutableInfo()->SetStatusChangeAttempts(info->StatusChangeAttempt);
                    entry.MutableInfo()->SetPrevStatusChangeAttempts(info->PrevStatusChangeAttempt);
                    entry.MutableInfo()->SetChangingAllowed(info->IsChangingAllowed());
                    entry.MutableInfo()->SetTouched(info->IsTouched());
                    entry.MutableInfo()->SetLastStatusChange(info->LastStatusChange.ToString());
                    entry.MutableInfo()->SetIgnoreReason(info->IgnoreReason);
                    entry.MutableInfo()->SetStatusChangeFailed(info->StatusChangeFailed);
                }
            }
        }

        Send(ev->Sender, std::move(response));
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        const auto& response = ev->Get()->Record.GetResponse();

        LOG_D("Handle TEvBlobStorage::TEvControllerConfigResponse"
            << ": response# " << response.ShortDebugString()
            << ", cookie# " << ev->Cookie);

        if (ev->Cookie != SentinelState->ChangeRequestId) {
            LOG_W("Ignore TEvBlobStorage::TEvControllerConfigResponse"
                << ": cookie# " << ev->Cookie
                << ", expected# " << SentinelState->ChangeRequestId);
            return;
        }

        if (SentinelState->ChangeRequests.empty()) {
            LOG_W("Ignore TEvBlobStorage::TEvControllerConfigResponse: empty queue");
            return;
        }

        auto onPDiskStatusChanged = [&](const TPDiskID& id, TPDiskInfo& info) {
            info.LastStatusChangeFailed = false;
            info.StatusChangeFailed = false;
            info.PrevStatus = info.ActualStatus;
            info.ActualStatus = info.GetStatus();
            info.LastStatusChange = Now();
            info.PrevStatusChangeAttempt = info.StatusChangeAttempt;
            info.StatusChangeAttempt = SentinelState->StatusChangeAttempt;

            LOG_N("PDisk status has been changed"
                << ": pdiskId# " << id);

            (*Counters->PDisksChanged)++;
        };

        if (!response.GetSuccess() || !response.StatusSize()) {
            for (auto& [key, req] : SentinelState->ChangeRequests) {
                req->LastStatusChangeFailed = false;
                req->StatusChangeFailed = true;
                req->StatusChangeAttempt = SentinelState->StatusChangeAttempt;
            }

            Y_ABORT_UNLESS(SentinelState->ChangeRequests.size() >= response.StatusSize());
            auto it = SentinelState->ChangeRequests.begin();
            for (const auto& status : response.GetStatus()) {
                if (!status.GetSuccess()) {
                    it->second->LastStatusChangeFailed = true;
                    LOG_E("Unsuccesful response from BSC"
                        << ": error# " << status.GetErrorDescription());
                }
                ++it;
            }

            MaybeRetry();
        } else {
            for (auto& [id, info] : SentinelState->ChangeRequests) {
                onPDiskStatusChanged(id, *info);
            }

            SentinelState->ChangeRequests.clear();
            SentinelState->StatusChangeAttempt = 0;
        }
    }

    void OnRetry() {
        LOG_D("Retrying"
            << ": attempt# " << SentinelState->StatusChangeAttempt);
        SendBSCRequests();
    }

    void MaybeRetry() {
        if (SentinelState->StatusChangeAttempt++ < Config.ChangeStatusRetries && !SentinelState->ChangeRequests.empty()) {
            Schedule(Config.RetryChangeStatus, new TEvents::TEvWakeup());
        } else {
            SentinelState->StatusChangeAttempt = 0;

            for (auto& kv : std::exchange(SentinelState->ChangeRequests, {})) {
                kv.second->StatusChangeFailed = true;

                LOG_C("PDisk status has NOT been changed"
                    << ": pdiskId# " << kv.first);

                (*Counters->PDisksNotChanged)++;
            }
        }
    }

    void OnPipeDisconnected() {
        if (const TActorId& actor = ConfigUpdater.Id) {
            Send(actor, new TEvSentinel::TEvBSCPipeDisconnected());
        }

        MaybeRetry();
    }

    void PassAway() override {
        if (const TActorId& actor = std::exchange(ConfigUpdater.Id, {})) {
            Send(actor, new TEvents::TEvPoisonPill());
        }

        if (const TActorId& actor = std::exchange(StateUpdater.Id, {})) {
            Send(actor, new TEvents::TEvPoisonPill());
        }

        TActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SENTINEL_ACTOR;
    }

    static TStringBuf Name() {
        return "Main"sv;
    }

    explicit TSentinel(TCmsStatePtr cmsState)
        : CmsState(cmsState)
        , Config(CmsState->Config.SentinelConfig)
        , SentinelState(new TSentinelState)
    {
    }

    void Bootstrap() {
        auto counters = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "sentinel");
        Counters.Reset(new TCounters(counters));

        UpdateConfig();
        UpdateState();

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvSentinel::TEvUpdateConfig, UpdateConfig);
            sFunc(TEvSentinel::TEvConfigUpdated, OnConfigUpdated);
            sFunc(TEvSentinel::TEvUpdateState, UpdateState);
            sFunc(TEvSentinel::TEvStateUpdated, OnStateUpdated);
            hFunc(TEvSentinel::TEvUpdateHostMarkers, Handle);
            sFunc(TEvSentinel::TEvBSCPipeDisconnected, OnPipeDisconnected);

            hFunc(TEvCms::TEvGetSentinelStateRequest, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);

            sFunc(TEvents::TEvWakeup, OnRetry);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

private:
    TCmsStatePtr CmsState;
    const TCmsSentinelConfig& Config;
    THolder<TCounters> Counters;

    TUpdaterInfo ConfigUpdater;
    TUpdaterInfo StateUpdater;
    TSentinelState::TPtr SentinelState;

}; // TSentinel

} // NSentinel

IActor* CreateSentinel(TCmsStatePtr state) {
    return new NSentinel::TSentinel(state);
}

} // NKikimr::NCms
