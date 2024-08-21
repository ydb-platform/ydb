#pragma once

#include "defs.h"
#include "dsproxy.h"

#include <ydb/core/blobstorage/base/utility.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBlobStorageGroupProxy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static constexpr TDuration UpdateResponsivenessTimeout = TDuration::MilliSeconds(500);
static constexpr TDuration ResponsivenessTrackerWindow = TDuration::Seconds(5);
static constexpr ui32 ResponsivenessTrackerMaxQueue = 10000; // number of stat series items per single VDisk

class TBlobStorageGroupProxy : public TActorBootstrapped<TBlobStorageGroupProxy> {
    enum {
        EvUpdateResponsiveness = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvUpdateGroupStat,
        EvStopBatchingPutRequests,
        EvStopBatchingGetRequests,
        EvConfigureQueryTimeout,
        EvEstablishingSessionTimeout,
        Ev5min,
        EvCheckDeadlines,
    };

    struct TEvUpdateResponsiveness : TEventLocal<TEvUpdateResponsiveness, EvUpdateResponsiveness> {};
    struct TEvUpdateGroupStat : TEventLocal<TEvUpdateGroupStat, EvUpdateGroupStat> {};
    struct TEvStopBatchingPutRequests : TEventLocal<TEvStopBatchingPutRequests, EvStopBatchingPutRequests> {};
    struct TEvStopBatchingGetRequests : TEventLocal<TEvStopBatchingGetRequests, EvStopBatchingGetRequests> {};
    struct TEvConfigureQueryTimeout : TEventLocal<TEvConfigureQueryTimeout, EvConfigureQueryTimeout> {};
    struct TEvEstablishingSessionTimeout : TEventLocal<TEvEstablishingSessionTimeout, EvEstablishingSessionTimeout> {};

    template <typename TEventPtr>
    struct TBatchedQueue {
        TBatchedVec<TEventPtr> Queue;
        ui64 Bytes = 0;
        ui64 RequestCount = 0;
    };

    struct TPutBatchedBucket {
        NKikimrBlobStorage::EPutHandleClass HandleClass;
        TEvBlobStorage::TEvPut::ETactic Tactic;

        TPutBatchedBucket(NKikimrBlobStorage::EPutHandleClass handleClass, TEvBlobStorage::TEvPut::ETactic tactic)
            : HandleClass(handleClass)
            , Tactic(tactic)
        {
            Y_ABORT_UNLESS(NKikimrBlobStorage::EPutHandleClass_MIN <= handleClass &&
                    NKikimrBlobStorage::EPutHandleClass_MAX >= handleClass, "incorrect PutHandleClass");
            Y_ABORT_UNLESS(0 <= tactic && tactic < TEvBlobStorage::TEvPut::TacticCount, "incorrect PutTactic");
        }
    };

    static std::atomic<TMonotonic> ThrottlingTimestamp;
    const TGroupId GroupId;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    std::shared_ptr<TBlobStorageGroupInfo::TTopology> Topology;
    TIntrusivePtr<TDsProxyNodeMon> NodeMon;
    TIntrusivePtr<TStoragePoolCounters> StoragePoolCounters;
    TIntrusivePtr<TGroupSessions> Sessions;
    TDeque<std::unique_ptr<IEventHandle>> InitQueue;
    std::multimap<TInstant, TActorId> DeadlineMap;
    THashMap<TActorId, std::multimap<TInstant, TActorId>::iterator, TActorId::THash> ActiveRequests;
    ui64 UnconfiguredBufferSize = 0;
    const bool IsEjected;
    bool ForceWaitAllDrives;
    bool UseActorSystemTimeInBSQueue;
    bool IsLimitedKeyless = false;
    bool IsFullMonitoring = false; // current state of monitoring
    ui32 MinREALHugeBlobInBytes = 0;

    TActorId MonActor;
    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    TBSProxyContextPtr BSProxyCtx;

    TString ErrorDescription;
    TString ExtraLogInfo; // is empty or ends with a space

    // node layout -- obtained with bootstrap
    TNodeLayoutInfoPtr NodeLayoutInfo = nullptr;

    TDiskResponsivenessTracker ResponsivenessTracker = {ResponsivenessTrackerMaxQueue, ResponsivenessTrackerWindow};
    TDiskResponsivenessTracker::TPerDiskStatsPtr PerDiskStats = MakeIntrusive<TDiskResponsivenessTracker::TPerDiskStats>();

    TEvStopBatchingPutRequests::TPtr StopPutBatchingEvent;
    ui64 BatchedPutRequestCount = 0;

    static constexpr ui64 PutHandleClassCount = NKikimrBlobStorage::EPutHandleClass_MAX + 1;
    static_assert(NKikimrBlobStorage::EPutHandleClass_MIN == 1); // counting from one
    static_assert(PutHandleClassCount < 10);
    static constexpr ui64 PutTacticCount = TEvBlobStorage::TEvPut::TacticCount;
    static_assert(PutTacticCount <= 10);

    TBatchedQueue<TEvBlobStorage::TEvPut::TPtr> BatchedPuts[PutHandleClassCount][PutTacticCount];
    static constexpr ui64 PutBatchecBucketCount = PutHandleClassCount * PutTacticCount;
    TStackVec<TPutBatchedBucket, PutBatchecBucketCount> PutBatchedBucketQueue;

    TEvStopBatchingGetRequests::TPtr StopGetBatchingEvent;
    ui64 BatchedGetRequestCount = 0;

    static constexpr ui64 GetHandleClassCount = NKikimrBlobStorage::EGetHandleClass_MAX + 1;
    static_assert(NKikimrBlobStorage::EGetHandleClass_MIN == 1); // counting from one

    TBatchedQueue<TEvBlobStorage::TEvGet::TPtr> BatchedGets[GetHandleClassCount];
    TStackVec<NKikimrBlobStorage::EGetHandleClass, GetHandleClassCount> GetBatchedBucketQueue;

    TMemorizableControlWrapper EnablePutBatching;
    TMemorizableControlWrapper EnableVPatch;

    TInstant EstablishingSessionStartTime;

    const TDuration MuteDuration = TDuration::Seconds(5);
    TLogPriorityMuteChecker<NLog::PRI_INFO, NLog::PRI_DEBUG> EstablishingSessionsPutMuteChecker;
    TLogPriorityMuteChecker<NLog::PRI_INFO, NLog::PRI_DEBUG> ErrorStateMuteChecker;
    TLogPriorityMuteChecker<NLog::PRI_CRIT, NLog::PRI_DEBUG> InvalidGroupIdMuteChecker;

    bool HasInvalidGroupId() const { return GroupId.GetRawId() == Max<ui32>(); }
    void ProcessInitQueue();

    TMemorizableControlWrapper SlowDiskThreshold;
    TMemorizableControlWrapper PredictedDelayMultiplier;

    TDuration LongRequestThreshold;

    TAccelerationParams GetAccelerationParams();

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Enable monitoring

    // Create monitoring and ensure all counters if needed
    void EnsureMonitoring(bool fullIfPossible);

    template<typename TEvent>
    void IncMonEvent(const TEvent *ev) {
        if (!Mon) {
           return;
        }
        if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvPut>) {
            Mon->CountPutEvent(ev->Buffer.size());
        } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvGet>) {
            Mon->EventGet->Inc();
        } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvBlock>) {
            Mon->EventBlock->Inc();
        } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvDiscover>) {
            Mon->EventDiscover->Inc();
        } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvRange>) {
            Mon->EventRange->Inc();
        } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvCollectGarbage>) {
            Mon->EventCollectGarbage->Inc();
        } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvStatus>) {
            Mon->EventStatus->Inc();
        } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvPatch>) {
            Mon->EventPatch->Inc();
        } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvAssimilate>) {
            Mon->EventAssimilate->Inc();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unconfigured state

    TEvEstablishingSessionTimeout *EstablishingSessionsTimeoutEv = nullptr;
    TEvConfigureQueryTimeout *ConfigureQueryTimeoutEv = nullptr;
    bool InEstablishingSessionsTimeout = false, InEstablishingSessionsTimeout5min = false;
    bool InUnconfiguredTimeout = false, InUnconfiguredTimeout5min = false;
    ui64 Cookie5min = 0;

    void Handle5min(TAutoPtr<IEventHandle> ev);
    void ClearTimeoutCounters();

    void SetStateEstablishingSessions();
    void SetStateEstablishingSessionsTimeout();
    void SetStateUnconfigured();
    void SetStateUnconfiguredTimeout();
    void SetStateWork();

    std::optional<TBlobStorageGroupInfo::EEncryptionMode> EncryptionMode;
    std::optional<TBlobStorageGroupInfo::ELifeCyclePhase> LifeCyclePhase;
    std::optional<ui64> GroupKeyNonce;
    std::optional<TCypherKey> CypherKey;

    void Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev);
    void ApplyGroupInfo(TIntrusivePtr<TBlobStorageGroupInfo>&& info, TIntrusivePtr<TStoragePoolCounters>&& counters);

    void WakeupUnconfigured(TEvConfigureQueryTimeout::TPtr ev);

   ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Establishing Sessions state

    ui32 NumUnconnectedDisks = Max<ui32>();

    void SwitchToWorkWhenGoodToGo();
    void WakeupEstablishingSessions(TEvEstablishingSessionTimeout::TPtr ev);
    void Handle(TEvProxyQueueState::TPtr& ev);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Put to init queue

    template<typename TEvent>
    void HandleEnqueue(TAutoPtr<TEventHandle<TEvent>> ev) {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
                << " HandleEnqueue# " << ev->Get()->Print(false) << " Marker# DSP17");
        if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvGet>) {
            LWTRACK(DSProxyGetEnqueue, ev->Get()->Orbit);
        } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvPut>) {
            LWTRACK(DSProxyPutEnqueue, ev->Get()->Orbit);
        }
        UnconfiguredBufferSize += ev->Get()->CalculateSize();
        InitQueue.emplace_back(ev.Release());
        if (UnconfiguredBufferSize > UnconfiguredBufferSizeLimit && InitQueue.size() > 1) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
                << " UnconfiguredBufferSize# " << UnconfiguredBufferSize << " > " << UnconfiguredBufferSizeLimit
                << ", dropping the queue (" << (ui64)InitQueue.size() << ")" << " Marker# DSP08");
            if (CurrentStateFunc() == &TThis::StateUnconfigured) {
                ErrorDescription = "Too many requests while waiting for configuration (DSPE2).";
                SetStateUnconfiguredTimeout();
            } else if (CurrentStateFunc() == &TThis::StateEstablishingSessions) {
                ErrorDescription = "Too many requests while establishing sessions (DSPE5).";
                SetStateEstablishingSessionsTimeout();
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Normal state

    template<typename TEventPtr>
    void EnableWilsonTracing(TEventPtr &ev, TAtomic &sampleRate) {
        if (!ev->TraceId) {
            const ui64 rate = AtomicGet(sampleRate);
            if (rate) {
                const ui64 num = RandomNumber<ui64>(1000000); // in range [0, 1000000)
                if (num < rate) {
                    ev->TraceId = NWilson::TTraceId::NewTraceIdThrottled(15, Max<ui32>(), ThrottlingTimestamp,
                        TMonotonic::Now(), TDuration::Seconds(1));
                }
            }
        }
    }

    TMaybe<TGroupStat::EKind> PutHandleClassToGroupStatKind(NKikimrBlobStorage::EPutHandleClass handleClass) {
        switch (handleClass) {
        case NKikimrBlobStorage::TabletLog:
            return TGroupStat::EKind::PUT_TABLET_LOG;

        case NKikimrBlobStorage::UserData:
            return TGroupStat::EKind::PUT_USER_DATA;

        default:
            return {};
        }
    }

    void ProcessBatchedPutRequests(TBatchedQueue<TEvBlobStorage::TEvPut::TPtr> &batchedPuts,
            NKikimrBlobStorage::EPutHandleClass handleClass, TEvBlobStorage::TEvPut::ETactic tactic);
    void Handle(TEvStopBatchingPutRequests::TPtr& ev);
    void Handle(TEvStopBatchingGetRequests::TPtr& ev);

    // todo: in-fly tracking for cancelation and
    void PushRequest(IActor *actor, TInstant deadline);
    void CheckDeadlines();
    void HandleNormal(TEvBlobStorage::TEvGet::TPtr &ev);
    void HandleNormal(TEvBlobStorage::TEvPut::TPtr &ev);
    void HandleNormal(TEvBlobStorage::TEvBlock::TPtr &ev);
    void HandleNormal(TEvBlobStorage::TEvPatch::TPtr &ev);
    void HandleNormal(TEvBlobStorage::TEvDiscover::TPtr &ev);
    void HandleNormal(TEvBlobStorage::TEvRange::TPtr &ev);
    void HandleNormal(TEvBlobStorage::TEvCollectGarbage::TPtr &ev);
    void HandleNormal(TEvBlobStorage::TEvStatus::TPtr &ev);
    void HandleNormal(TEvBlobStorage::TEvAssimilate::TPtr &ev);
    void Handle(TEvBlobStorage::TEvBunchOfEvents::TPtr ev);
    void Handle(TEvDeathNote::TPtr ev);
    void Handle(TEvGetQueuesInfo::TPtr ev);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Error state

    NLog::EPriority CheckPriorityForErrorState() {
        TInstant now = TActivationContext::Now();
        if (HasInvalidGroupId()) {
            return InvalidGroupIdMuteChecker.Register(now, MuteDuration);
        } else {
            return ErrorStateMuteChecker.Register(now, MuteDuration);
        }
    }

    template<typename TEvent>
    void HandleError(TAutoPtr<TEventHandle<TEvent>> ev) {
        bool full = false;
        if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvBlock> || std::is_same_v<TEvent, TEvBlobStorage::TEvCollectGarbage>) {
            full = ev->Get()->IsMonitored;
        }
        EnsureMonitoring(full);
        IncMonEvent(ev->Get());
        const NKikimrProto::EReplyStatus status = (!IsEjected || HasInvalidGroupId())
            ? NKikimrProto::ERROR
            : NKikimrProto::NO_GROUP;
        auto response = ev->Get()->MakeErrorResponse(status, ErrorDescription, GroupId);
        SetExecutionRelay(*response, std::move(ev->Get()->ExecutionRelay));
        NActors::NLog::EPriority priority = CheckPriorityForErrorState();
        LOG_LOG_S(*TlsActivationContext, priority, NKikimrServices::BS_PROXY, ExtraLogInfo << "Group# " << GroupId
                << " HandleError ev# " << ev->Get()->Print(false)
                << " Response# " << response->Print(false)
                << " Marker# DSP31");
        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // All states

    void PassAway() override;

    void Handle(TEvTimeStats::TPtr& ev);
    void Handle(TEvRequestProxySessionsState::TPtr &ev);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_PROXY_ACTOR;
    }

    TBlobStorageGroupProxy(TIntrusivePtr<TBlobStorageGroupInfo>&& info, bool forceWaitAllDrives,
            TIntrusivePtr<TDsProxyNodeMon> &nodeMon, TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters,
            const TBlobStorageProxyParameters& params);

    TBlobStorageGroupProxy(ui32 groupId, bool isEjected, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
            const TBlobStorageProxyParameters& params);

    void Bootstrap();

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Configuration process

    void HandleUpdateResponsiveness();
    void ScheduleUpdateResponsiveness();

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Group statistics

    const TDuration GroupStatUpdateInterval = TDuration::Seconds(10);
    bool GroupStatUpdateScheduled = false;
    TGroupStat Stat;

    void HandleUpdateGroupStat();
    void ScheduleUpdateGroupStat();
    void Handle(TEvLatencyReport::TPtr& ev);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // State functions

    STRICT_STFUNC(StateCommon,
        hFunc(TEvStopBatchingPutRequests, Handle);
        hFunc(TEvStopBatchingGetRequests, Handle);
        hFunc(TEvDeathNote, Handle);
        hFunc(TEvRequestProxySessionsState, Handle);
        hFunc(TEvBlobStorage::TEvBunchOfEvents, Handle);
        hFunc(TEvTimeStats, Handle);
        cFunc(TEvents::TSystem::Poison, PassAway);
        hFunc(TEvInterconnect::TEvNodesInfo, Handle);
        hFunc(TEvBlobStorage::TEvConfigureProxy, Handle);
        hFunc(TEvProxyQueueState, Handle);
        cFunc(EvUpdateResponsiveness, HandleUpdateResponsiveness);
        cFunc(EvUpdateGroupStat, HandleUpdateGroupStat);
        hFunc(TEvLatencyReport, Handle);
        IgnoreFunc(TEvConfigureQueryTimeout);
        IgnoreFunc(TEvEstablishingSessionTimeout);
        fFunc(Ev5min, Handle5min);
        cFunc(EvCheckDeadlines, CheckDeadlines);
        hFunc(TEvGetQueuesInfo, Handle);
    )

#define HANDLE_EVENTS(HANDLER) \
    hFunc(TEvBlobStorage::TEvPut, HANDLER); \
    hFunc(TEvBlobStorage::TEvGet, HANDLER); \
    hFunc(TEvBlobStorage::TEvBlock, HANDLER); \
    hFunc(TEvBlobStorage::TEvDiscover, HANDLER); \
    hFunc(TEvBlobStorage::TEvRange, HANDLER); \
    hFunc(TEvBlobStorage::TEvCollectGarbage, HANDLER); \
    hFunc(TEvBlobStorage::TEvStatus, HANDLER); \
    hFunc(TEvBlobStorage::TEvPatch, HANDLER); \
    hFunc(TEvBlobStorage::TEvAssimilate, HANDLER); \
    /**/

    STFUNC(StateUnconfigured) {
        switch (ev->GetTypeRewrite()) {
            HANDLE_EVENTS(HandleEnqueue);
            hFunc(TEvConfigureQueryTimeout, WakeupUnconfigured);
            default: return StateCommon(ev);
        }
    }

    STFUNC(StateUnconfiguredTimeout) {
        switch (ev->GetTypeRewrite()) {
            HANDLE_EVENTS(HandleError);
            default: return StateUnconfigured(ev);
        }
    }

    STFUNC(StateEstablishingSessions) {
        switch (ev->GetTypeRewrite()) {
            HANDLE_EVENTS(HandleEnqueue);
            hFunc(TEvEstablishingSessionTimeout, WakeupEstablishingSessions);
            default: return StateCommon(ev);
        }
    }

    STFUNC(StateEstablishingSessionsTimeout) {
        switch (ev->GetTypeRewrite()) {
            HANDLE_EVENTS(HandleError);
            default: return StateEstablishingSessions(ev);
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HANDLE_EVENTS(HandleNormal);
            default: return StateCommon(ev);
        }
    }

    STFUNC(StateEjected) {
        switch (ev->GetTypeRewrite()) {
            HANDLE_EVENTS(HandleError);
            default: return StateCommon(ev);
        }
    }
};

}
