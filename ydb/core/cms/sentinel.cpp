#include "cms_impl.h"
#include "sentinel.h"
#include "sentinel_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/protos/services.pb.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
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
    if (!StateCounter) {
        reason = "Uninitialized StateCounter";
        return current;
    }

    auto it = StateLimits.find(State);
    const ui32 stateLimit = (it != StateLimits.end()) ? it->second : DefaultStateLimit;

    if (!stateLimit || StateCounter < stateLimit) {
        reason = TStringBuilder()
            << " PrevState# " << PrevState
            << " State# " << State
            << " StateCounter# " << StateCounter
            << " current# " << current;
        switch (PrevState) {
        case  NKikimrBlobStorage::TPDiskState::Unknown:
            return current;
        default:
            return EPDiskStatus::INACTIVE;
        }
    }

    reason = TStringBuilder()
        << " PrevState# " << PrevState
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

void TPDiskStatusComputer::Reset() {
    StateCounter = 0;
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

bool TPDiskStatus::IsChanged(TString& reason) const {
    return Current != Compute(Current, reason);
}

bool TPDiskStatus::IsChanged() const {
    TString unused;
    return IsChanged(unused);
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

/// TClusterMap

TClusterMap::TClusterMap(TCmsStatePtr state)
    : State(state)
{
}

void TClusterMap::AddPDisk(const TPDiskID& id) {
    Y_VERIFY(State->ClusterInfo->HasNode(id.NodeId));
    Y_VERIFY(State->ClusterInfo->HasPDisk(id));
    const auto& location = State->ClusterInfo->Node(id.NodeId).Location;

    ByDataCenter[location.HasKey(TNodeLocation::TKeys::DataCenter) ? location.GetDataCenterId() : ""].insert(id);
    ByRoom[location.HasKey(TNodeLocation::TKeys::Module) ? location.GetModuleId() : ""].insert(id);
    ByRack[location.HasKey(TNodeLocation::TKeys::Rack) ? location.GetRackId() : ""].insert(id);
    NodeByRack[location.HasKey(TNodeLocation::TKeys::Rack) ? location.GetRackId() : ""].insert(id.NodeId);
}

/// TGuardian

TGuardian::TGuardian(TCmsStatePtr state, ui32 dataCenterRatio, ui32 roomRatio, ui32 rackRatio)
    : TClusterMap(state)
    , DataCenterRatio(dataCenterRatio)
    , RoomRatio(roomRatio)
    , RackRatio(rackRatio)
{
}

TClusterMap::TPDiskIDSet TGuardian::GetAllowedPDisks(const TClusterMap& all, TString& issues,
        TPDiskIDSet& disallowed) const {
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
        Y_VERIFY(all.ByDataCenter.contains(kv.first));

        if (!kv.first || CheckRatio(kv, all.ByDataCenter, DataCenterRatio)) {
            result.insert(kv.second.begin(), kv.second.end());
        } else {
            LOG_IGNORED(DataCenter);
            disallowed.insert(kv.second.begin(), kv.second.end());
        }
    }

    for (const auto& kv : ByRoom) {
        Y_VERIFY(all.ByRoom.contains(kv.first));

        if (kv.first && !CheckRatio(kv, all.ByRoom, RoomRatio)) {
            LOG_IGNORED(Room);
            disallowed.insert(kv.second.begin(), kv.second.end());
            EraseNodesIf(result, [&room = kv.second](const TPDiskID& id) {
                return room.contains(id);
            });
        }
    }

    for (const auto& kv : ByRack) {
        Y_VERIFY(all.ByRack.contains(kv.first));
        // ignore check if there is only one node in a rack
        auto it = NodeByRack.find(kv.first);
        if (it != NodeByRack.end() && it->second.size() == 1) {
            continue;
        }
        if (kv.first && !CheckRatio(kv, all.ByRack, RackRatio)) {
            LOG_IGNORED(Rack);
            disallowed.insert(kv.second.begin(), kv.second.end());
            EraseNodesIf(result, [&rack = kv.second](const TPDiskID& id) {
                return rack.contains(id);
            });
        }
    }

    #undef LOG_IGNORED

    issues = issuesBuilder;
    return result;
}

/// Main state
struct TSentinelState: public TSimpleRefCount<TSentinelState> {
    using TPtr = TIntrusivePtr<TSentinelState>;

    TMap<TPDiskID, TPDiskInfo::TPtr> PDisks;
    THashSet<ui32> StateUpdaterWaitNodes;
    ui32 ConfigUpdaterAttempt = 0;
};

/// Actors

template <typename TDerived>
class TSentinelChildBase: public TActorBootstrapped<TDerived> {
protected:
    void ConnectBSC() {
        auto domains = AppData()->DomainsInfo;
        const ui32 domainUid = domains->GetDomainUidByTabletId(CmsState->CmsTabletId);
        const ui64 bscId = MakeBSControllerID(domains->GetDefaultStateStorageGroup(domainUid));

        NTabletPipe::TClientConfig config;
        config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        CmsState->BSControllerPipe = this->Register(NTabletPipe::CreateClient(CmsState->CmsActorId, bscId, config));
    }

public:
    using TBase = TSentinelChildBase<TDerived>;

    explicit TSentinelChildBase(const TActorId& parent, TCmsStatePtr cmsState)
        : Parent(parent)
        , CmsState(cmsState)
        , Config(CmsState->Config.SentinelConfig)
    {
    }

protected:
    const TActorId Parent;
    TCmsStatePtr CmsState;
    const TCmsSentinelConfig& Config;

}; // TSentinelChildBase

template <typename TEvUpdated, typename TDerived>
class TUpdaterBase: public TSentinelChildBase<TDerived> {
public:
    using TBase = TUpdaterBase<TEvUpdated, TDerived>;

protected:
    void Reply() {
        this->Send(this->Parent, new TEvUpdated());
        this->PassAway();
    }

public:
    explicit TUpdaterBase(const TActorId& parent, TCmsStatePtr cmsState, TSentinelState::TPtr sentinelState)
        : TSentinelChildBase<TDerived>(parent, cmsState)
        , SentinelState(sentinelState)
    {
        for (auto& [_, info] : SentinelState->PDisks) {
            info->ClearTouched();
        }
    }

protected:
    TSentinelState::TPtr SentinelState;

}; // TUpdaterBase

class TConfigUpdater: public TUpdaterBase<TEvSentinel::TEvConfigUpdated, TConfigUpdater> {
    void Retry() {
        ++SentinelState->ConfigUpdaterAttempt;
        Schedule(Config.RetryUpdateConfig, new TEvSentinel::TEvRetry());
    }

    void RequestBSConfig() {
        LOG_D("Request blobstorage config"
            << ": attempt# " << SentinelState->ConfigUpdaterAttempt);

        if (!CmsState->BSControllerPipe) {
            ConnectBSC();
        }

        auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        request->Record.MutableRequest()->AddCommand()->MutableQueryBaseConfig();
        NTabletPipe::SendData(SelfId(), CmsState->BSControllerPipe, request.Release());
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
                << ", size# " << response.StatusSize()
                << ", error# " << error);
            Retry();
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

            Reply();
        }
    }

    void OnPipeDisconnected() {
        LOG_E("Pipe to BSC disconnected");
        Retry();
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
        Become(&TThis::StateWork);
    }

    void PassAway() override {
        SentinelState->ConfigUpdaterAttempt = 0;
        TActor::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvSentinel::TEvRetry, RequestBSConfig);
            sFunc(TEvSentinel::TEvBSCPipeDisconnected, OnPipeDisconnected);

            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);

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

            Y_VERIFY(!it->second->IsTouched());
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
        TActor::PassAway();
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

class TStatusChanger: public TSentinelChildBase<TStatusChanger> {
    void Reply(bool success = true) {
        Send(Parent, new TEvSentinel::TEvStatusChanged(Id, success));
        PassAway();
    }

    void MaybeRetry() {
        if (Info->StatusChangerState->Attempt++ < Config.ChangeStatusRetries) {
            Schedule(Config.RetryChangeStatus, new TEvSentinel::TEvRetry());
        } else {
            Reply(false);
        }
    }

    void RequestStatusChange() {
        LOG_D("Change pdisk status"
            << ": pdiskId# " << Id
            << ", status# " << Info->StatusChangerState->Status
            << ", attempt# " << Info->StatusChangerState->Attempt);

        if (!CmsState->BSControllerPipe) {
            ConnectBSC();
        }

        auto request = MakeHolder<TEvBlobStorage::TEvControllerConfigRequest>();
        auto& command = *request->Record.MutableRequest()->AddCommand()->MutableUpdateDriveStatus();
        command.MutableHostKey()->SetNodeId(Id.NodeId);
        command.SetPDiskId(Id.DiskId);
        command.SetStatus(Info->StatusChangerState->Status);
        NTabletPipe::SendData(SelfId(), CmsState->BSControllerPipe, request.Release());
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
                << ", size# " << response.StatusSize()
                << ", error# " << error);
            MaybeRetry();
        } else {
            Reply();
        }
    }

    void OnPipeDisconnected() {
        LOG_E("Pipe to BSC disconnected");
        MaybeRetry();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SENTINEL_STATUS_CHANGER_ACTOR;
    }

    void PassAway() override {
        Info->StatusChangerState.Reset();
        TActor::PassAway();
    }

    static TStringBuf Name() {
        return "StatusChanger"sv;
    }

    explicit TStatusChanger(
            const TActorId& parent,
            TCmsStatePtr state,
            const TPDiskID& id,
            TPDiskInfo::TPtr info,
            NKikimrBlobStorage::EDriveStatus status)
        : TBase(parent, state)
        , Id(id)
        , Info(info)
    {
        info->StatusChangerState = new TStatusChangerState(status);
    }

    void Bootstrap() {
        RequestStatusChange();
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvSentinel::TEvRetry, RequestStatusChange);
            sFunc(TEvSentinel::TEvBSCPipeDisconnected, OnPipeDisconnected);

            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);

            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

private:
    const TPDiskID Id;
    TPDiskInfo::TPtr Info;
}; // TStatusChanger

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

    struct TUpdaterInfo {
        TActorId Id;
        TInstant StartedAt;
        bool Delayed;

        TUpdaterInfo() {
            Clear();
        }

        void Start(const TActorId& id, const TInstant& now) {
            Id = id;
            StartedAt = now;
            Delayed = false;
        }

        void Clear() {
            Id = TActorId();
            StartedAt = TInstant::Zero();
            Delayed = false;
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
        Y_VERIFY(AllOf(SentinelState->PDisks, [](const auto& kv) {
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

        Y_VERIFY(CmsState->ClusterInfo->HasNode(id.NodeId));
        action.SetHost(CmsState->ClusterInfo->Node(id.NodeId).Host);

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

        if (!CmsState->ClusterInfo) {
            LOG_C("Missing cluster info");
            ScheduleUpdate<TEvSentinel::TEvUpdateState, TConfigUpdater>(
                StateUpdater, Config.UpdateStateInterval, ConfigUpdater
            );

            return;
        }

        TClusterMap all(CmsState);
        TGuardian changed(CmsState, Config.DataCenterRatio, Config.RoomRatio, Config.RackRatio);
        TClusterMap::TPDiskIDSet alwaysAllowed;

        for (auto& pdisk : SentinelState->PDisks) {
            const TPDiskID& id = pdisk.first;
            TPDiskInfo& info = *(pdisk.second);

            if (!CmsState->ClusterInfo->HasNode(id.NodeId)) {
                LOG_E("Missing node info"
                    << ": pdiskId# " << id);
                continue;
            }

            if (!CmsState->ClusterInfo->HasPDisk(id)) {
                LOG_E("Missing pdisk info"
                    << ": pdiskId# " << id);
                continue;
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
        THashSet<TPDiskID, TPDiskIDHash> disallowed;
        TClusterMap::TPDiskIDSet allowed = changed.GetAllowedPDisks(all, issues, disallowed);
        std::move(alwaysAllowed.begin(), alwaysAllowed.end(), std::inserter(allowed, allowed.begin()));

        for (const auto& id : allowed) {
            Y_VERIFY(SentinelState->PDisks.contains(id));
            TPDiskInfo::TPtr info = SentinelState->PDisks.at(id);

            if (!info->IsChangingAllowed()) {
                info->AllowChanging();
                continue;
            }

            if (info->StatusChanger) {
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
                info->StatusChanger = RegisterWithSameMailbox(new TStatusChanger(SelfId(), CmsState, id, info, requiredStatus));
                (*Counters->PDisksPendingChange)++;
            }
        }

        for (const auto& id : disallowed) {
            Y_VERIFY(SentinelState->PDisks.contains(id));
            SentinelState->PDisks.at(id)->DisallowChanging();
        }

        if (issues) {
            LOG_W(issues);
        }

        ScheduleUpdate<TEvSentinel::TEvUpdateState, TConfigUpdater>(
            StateUpdater, Config.UpdateStateInterval, ConfigUpdater
        );
    }

    void Handle(TEvCms::TEvGetSentinelStateRequest::TPtr& ev) {
        auto response = MakeHolder<TEvCms::TEvGetSentinelStateResponse>();

        auto& record = response->Record;
        record.MutableStatus()->SetCode(NKikimrCms::TStatus::OK);
        Config.Serialize(*record.MutableSentinelConfig());

        if (SentinelState) {
            auto& stateUpdater = *record.MutableStateUpdater();
            stateUpdater.MutableUpdaterInfo()->SetActorId(StateUpdater.Id.ToString());
            stateUpdater.MutableUpdaterInfo()->SetStartedAt(StateUpdater.StartedAt.ToString());
            stateUpdater.MutableUpdaterInfo()->SetDelayed(StateUpdater.Delayed);
            for (const auto& waitNode : SentinelState->StateUpdaterWaitNodes) {
                stateUpdater.AddWaitNodes(waitNode);
            }

            auto& configUpdater = *record.MutableConfigUpdater();
            configUpdater.MutableUpdaterInfo()->SetActorId(ConfigUpdater.Id.ToString());
            configUpdater.MutableUpdaterInfo()->SetStartedAt(ConfigUpdater.StartedAt.ToString());
            configUpdater.MutableUpdaterInfo()->SetDelayed(ConfigUpdater.Delayed);
            configUpdater.SetAttempt(SentinelState->ConfigUpdaterAttempt);

            for (const auto& [id, info] : SentinelState->PDisks) {
                auto& entry = *record.AddPDisks();
                entry.MutableId()->SetNodeId(id.NodeId);
                entry.MutableId()->SetDiskId(id.DiskId);
                entry.MutableInfo()->SetState(info->GetState());
                entry.MutableInfo()->SetPrevState(info->GetPrevState());
                entry.MutableInfo()->SetStateCounter(info->GetStateCounter());
                entry.MutableInfo()->SetStatus(info->GetStatus());
                entry.MutableInfo()->SetChangingAllowed(info->IsChangingAllowed());
                entry.MutableInfo()->SetTouched(info->IsTouched());
                if(info->StatusChangerState) {
                    entry.MutableInfo()->SetDesiredStatus(info->StatusChangerState->Status);
                    entry.MutableInfo()->SetStatusChangeAttempts(info->StatusChangerState->Attempt);
                }
            }
        }

        Send(ev->Sender, std::move(response));
    }

    void Handle(TEvSentinel::TEvStatusChanged::TPtr& ev) {
        const TPDiskID& id = ev->Get()->Id;
        const bool success = ev->Get()->Success;

        LOG_D("Handle TEvSentinel::TEvStatusChanged"
            << ": pdiskId# " << id
            << ", success# " << (success ? "true" : "false"));

        auto it = SentinelState->PDisks.find(id);
        if (it == SentinelState->PDisks.end()) {
            LOG_W("Status of unknown pdisk has been changed"
                << ": pdiskId# " << id);
            return;
        }

        if (!success) {
            LOG_C("PDisk status has NOT been changed"
                << ": pdiskId# " << id);
            (*Counters->PDisksNotChanged)++;
        } else {
            LOG_N("PDisk status has been changed"
                << ": pdiskId# " << id);
            (*Counters->PDisksChanged)++;
        }

        it->second->StatusChanger = TActorId();
    }

    void OnPipeDisconnected() {
        if (const TActorId& actor = ConfigUpdater.Id) {
            Send(actor, new TEvSentinel::TEvBSCPipeDisconnected());
        }

        for (const auto& [_, info] : SentinelState->PDisks) {
            if (const TActorId& actor = info->StatusChanger) {
                Send(actor, new TEvSentinel::TEvBSCPipeDisconnected());
            }
        }
    }

    void PassAway() override {
        if (const TActorId& actor = ConfigUpdater.Id) {
            Send(actor, new TEvents::TEvPoisonPill());
        }

        if (const TActorId& actor = StateUpdater.Id) {
            Send(actor, new TEvents::TEvPoisonPill());
        }

        for (const auto& [_, info] : SentinelState->PDisks) {
            if (const TActorId& actor = info->StatusChanger) {
                Send(actor, new TEvents::TEvPoisonPill());
            }
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
            hFunc(TEvSentinel::TEvStatusChanged, Handle);
            hFunc(TEvCms::TEvGetSentinelStateRequest, Handle);
            sFunc(TEvSentinel::TEvBSCPipeDisconnected, OnPipeDisconnected);

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
