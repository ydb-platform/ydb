#pragma once

#include "defs.h"
#include "dsproxy_mon.h"
#include "dsproxy_responsiveness.h"
#include "log_acc.h"
#include "group_sessions.h"

#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/blobstorage/base/batched_vec.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/base/transparent.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/group_stat.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/hash_set.h>

namespace NKikimr {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

constexpr ui32 TypicalPartsInBlob = 6;
constexpr ui32 TypicalDisksInSubring = 8;

constexpr ui32 MaxBatchedPutSize = 64 * 1024 - 512 - 5; // (MinREALHugeBlobInBytes - 1 - TDiskBlob::HugeBlobOverhead) for ssd and nvme

const TDuration ProxyConfigurationTimeout = TDuration::Seconds(20);
const ui32 ProxyRetryConfigurationInitialTimeout = 200;
const ui32 ProxyRetryConfigurationMaxTimeout = 5000;
const ui64 UnconfiguredBufferSizeLimit = 32 << 20;

const TDuration ProxyEstablishSessionsTimeout = TDuration::Seconds(100);

const ui64 DsPutWakeupMs = 60000;

const ui64 BufferSizeThreshold = 1 << 20;

const bool IsHandoffAccelerationEnabled = false;

const bool IsEarlyRequestAbortEnabled = false;
const bool IngressAsAReasonForErrorEnabled = false;

const ui32 BeginRequestSize = 10;
const ui32 MaxRequestSize = 1000;

const ui32 MaskSizeBits = 32;

constexpr bool DefaultEnablePutBatching = true;
constexpr bool DefaultEnableVPatch = false;

constexpr bool WithMovingPatchRequestToStaticNode = true;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Common types
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TDiskDelayPrediction {
    ui64 PredictedNs;
    ui32 DiskIdx;

    bool operator<(const TDiskDelayPrediction& other) const {
        return PredictedNs > other.PredictedNs;
    }
};

using TDiskDelayPredictions = TStackVec<TDiskDelayPrediction, TypicalDisksInSubring>;

struct TEvDeathNote : public TEventLocal<TEvDeathNote, TEvBlobStorage::EvDeathNote> {
    TStackVec<std::pair<TDiskResponsivenessTracker::TDiskId, TDuration>, 16> Responsiveness;

    TEvDeathNote(const TStackVec<std::pair<TDiskResponsivenessTracker::TDiskId, TDuration>, 16> &responsiveness)
        : Responsiveness(responsiveness)
    {}
};

struct TEvAbortOperation : public TEventLocal<TEvAbortOperation, TEvBlobStorage::EvAbortOperation>
{};

struct TEvLatencyReport : public TEventLocal<TEvLatencyReport, TEvBlobStorage::EvLatencyReport> {
    TGroupStat::EKind Kind;
    TDuration Sample;

    TEvLatencyReport(TGroupStat::EKind kind, TDuration sample)
        : Kind(kind)
        , Sample(sample)
    {}
};

struct TNodeLayoutInfo : TThrRefBase {
    // indexed by NodeId
    TNodeLocation SelfLocation;
    TVector<TNodeLocation> LocationPerOrderNumber;

    TNodeLayoutInfo(const TNodeLocation& selfLocation, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
            std::unordered_map<ui32, TNodeLocation>& map)
        : SelfLocation(selfLocation)
        , LocationPerOrderNumber(info->GetTotalVDisksNum())
    {
        for (ui32 i = 0; i < LocationPerOrderNumber.size(); ++i) {
            LocationPerOrderNumber[i] = map[info->GetActorId(i).NodeId()];
        }
    }
};

using TNodeLayoutInfoPtr = TIntrusivePtr<TNodeLayoutInfo>;

inline TStoragePoolCounters::EHandleClass HandleClassToHandleClass(NKikimrBlobStorage::EGetHandleClass handleClass) {
    switch (handleClass) {
        case NKikimrBlobStorage::FastRead:
            return TStoragePoolCounters::EHandleClass::HcGetFast;
        case NKikimrBlobStorage::AsyncRead:
            return TStoragePoolCounters::EHandleClass::HcGetAsync;
        case NKikimrBlobStorage::Discover:
            return TStoragePoolCounters::EHandleClass::HcGetDiscover;
        case NKikimrBlobStorage::LowRead:
            return TStoragePoolCounters::EHandleClass::HcGetLow;
    }
    return TStoragePoolCounters::EHandleClass::HcCount;
}

inline TStoragePoolCounters::EHandleClass HandleClassToHandleClass(NKikimrBlobStorage::EPutHandleClass handleClass) {
    switch (handleClass) {
        case NKikimrBlobStorage::TabletLog:
            return TStoragePoolCounters::EHandleClass::HcPutTabletLog;
        case NKikimrBlobStorage::UserData:
            return TStoragePoolCounters::EHandleClass::HcPutUserData;
        case NKikimrBlobStorage::AsyncBlob:
            return TStoragePoolCounters::EHandleClass::HcPutAsync;
    }
    return TStoragePoolCounters::EHandleClass::HcCount;
}

NActors::NLog::EPriority PriorityForStatusOutbound(NKikimrProto::EReplyStatus status);
NActors::NLog::EPriority PriorityForStatusResult(NKikimrProto::EReplyStatus status);
NActors::NLog::EPriority PriorityForStatusInbound(NKikimrProto::EReplyStatus status);

inline void SetExecutionRelay(IEventBase& ev, std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay) {
    switch (const ui32 type = ev.Type()) {
#define XX(T) \
        case TEvBlobStorage::Ev##T: \
            static_cast<TEvBlobStorage::TEv##T&>(ev).ExecutionRelay = std::move(executionRelay); \
            break; \
        case TEvBlobStorage::Ev##T##Result: \
            static_cast<TEvBlobStorage::TEv##T##Result&>(ev).ExecutionRelay = std::move(executionRelay); \
            break; \
        //

        XX(Put)
        XX(Get)
        XX(Block)
        XX(Discover)
        XX(Range)
        XX(CollectGarbage)
        XX(Status)
        XX(Patch)
        XX(Assimilate)
#undef XX

        default:
            Y_ABORT("unexpected event Type# 0x%08" PRIx32, type);
    }
}

class TBlobStorageGroupRequestActor : public TActor<TBlobStorageGroupRequestActor> {
public:
    template<typename TEv>
    TBlobStorageGroupRequestActor(TIntrusivePtr<TBlobStorageGroupInfo> info, TIntrusivePtr<TGroupQueues> groupQueues,
            TIntrusivePtr<TBlobStorageGroupProxyMon> mon, const TActorId& source, ui64 cookie,
            NKikimrServices::EServiceKikimr logComponent, bool logAccEnabled, TMaybe<TGroupStat::EKind> latencyQueueKind,
            TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters, ui32 restartCounter,
            NWilson::TTraceId&& traceId, const char *name, const TEv *event,
            std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay, NKikimrServices::TActivity::EType activity)
        : TActor(&TThis::InitialStateFunc, activity)
        , Info(std::move(info))
        , GroupQueues(std::move(groupQueues))
        , Mon(std::move(mon))
        , PoolCounters(storagePoolCounters)
        , LogCtx(logComponent, logAccEnabled)
        , ParentSpan(TWilson::BlobStorage, std::move(traceId), name)
        , RestartCounter(restartCounter)
        , CostModel(GroupQueues->CostModel)
        , Source(source)
        , Cookie(cookie)
        , LatencyQueueKind(latencyQueueKind)
        , RequestStartTime(now)
        , RacingDomains(&Info->GetTopology())
        , ExecutionRelay(std::move(executionRelay))
    {
        if (ParentSpan) {
            const NWilson::TTraceId& parentTraceId = ParentSpan.GetTraceId();
            Span = NWilson::TSpan(TWilson::BlobStorage, NWilson::TTraceId::NewTraceId(parentTraceId.GetVerbosity(),
                parentTraceId.GetTimeToLive()), ParentSpan.GetName());
            ParentSpan.Link(Span.GetTraceId());
            Span.Attribute("GroupId", Info->GroupID.GetRawId());
            Span.Attribute("RestartCounter", RestartCounter);
            event->ToSpan(Span);
        }

        Y_ABORT_UNLESS(CostModel);
    }

    virtual ~TBlobStorageGroupRequestActor() = default;

    virtual void Bootstrap() = 0;
    virtual void ReplyAndDie(NKikimrProto::EReplyStatus status) = 0;
    virtual std::unique_ptr<IEventBase> RestartQuery(ui32 counter) = 0;
    virtual ERequestType GetRequestType() const = 0;
    virtual ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const = 0;

    void Registered(TActorSystem *as, const TActorId& parentId) override;

    STFUNC(InitialStateFunc);

    void BootstrapImpl();

    template<typename T>
    void CountEvent(const T &ev) const {
        ERequestType request = GetRequestType();
        Mon->CountEvent(request, ev);
    }

    TActorId GetVDiskActorId(const TVDiskIdShort &shortId) const;

    bool ProcessEvent(TAutoPtr<IEventHandle>& ev, bool suppressCommonErrors = false);

    template<typename TEv>
    void CountPut(const std::unique_ptr<TEv>& ev) {
        ++GeneratedSubrequests;
        GeneratedSubrequestBytes += ev->GetBufferBytes();
    }

    template<typename TEv>
    void CountPuts(const TDeque<std::unique_ptr<TEv>>& q) {
        for (const auto& item : q) {
            CountPut(item);
        }
    }

    template<typename... TOptions>
    void CountPuts(const TDeque<std::variant<TOptions...>>& q) {
        for (const auto& item : q) {
            std::visit([&](auto& item) { CountPut(item); }, item);
        }
    }

    template<typename T>
    void SendToQueue(std::unique_ptr<T> event, ui64 cookie, bool timeStatsEnabled = false) {
        if constexpr (
            !std::is_same_v<T, TEvBlobStorage::TEvVGetBlock>
            && !std::is_same_v<T, TEvBlobStorage::TEvVBlock>
            && !std::is_same_v<T, TEvBlobStorage::TEvVStatus>
            && !std::is_same_v<T, TEvBlobStorage::TEvVCollectGarbage>
            && !std::is_same_v<T, TEvBlobStorage::TEvVAssimilate>
        ) {
            const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
            event->Record.MutableTimestamps()->SetSentByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        }

        if constexpr (!std::is_same_v<T, TEvBlobStorage::TEvVStatus> && !std::is_same_v<T, TEvBlobStorage::TEvVAssimilate>) {
            event->MessageRelevanceTracker = MessageRelevanceTracker;
            ui64 cost;
            if constexpr (std::is_same_v<T, TEvBlobStorage::TEvVMultiPut>) {
                bool internalQueue;
                cost = CostModel->GetCost(*event, &internalQueue);
            } else {
                cost = CostModel->GetCost(*event);
            }
            *PoolCounters->DSProxyDiskCostCounter += cost;

            LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::BS_REQUEST_COST,
                    "DSProxy Request Type# " << TypeName(*event) << " Cost# " << cost);
        }

        const TActorId queueId = GroupQueues->Send(*this, Info->GetTopology(), std::move(event), cookie, Span.GetTraceId(),
            timeStatsEnabled);
        ++RequestsInFlight;
    }

    void SendToQueues(TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &vGets, bool timeStatsEnabled);

    template<typename TEvent>
    void SendToQueues(TDeque<std::unique_ptr<TEvent>> &events, bool timeStatsEnabled) {
        for (auto& request : events) {
            ui64 messageCookie = request->Record.GetCookie();
            CountEvent(*request);
            TLogoBlobID id = GetBlobId(request);
            TVDiskID vDiskId = VDiskIDFromVDiskID(request->Record.GetVDiskID());
            LWTRACK(DSProxyPutVPutIsSent, request->Orbit, Info->GetFailDomainOrderNumber(vDiskId),
                    Info->GroupID.GetRawId(), id.Channel(), id.PartId(), id.ToString(), id.BlobSize());
            SendToQueue(std::move(request), messageCookie, timeStatsEnabled);
        }
    }

    template<typename TPtr>
    void ProcessReplyFromQueue(const TPtr& /*ev*/) {
        Y_ABORT_UNLESS(RequestsInFlight);
        --RequestsInFlight;
        CheckPostponedQueue();
    }

    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVPut> &ev);
    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVMultiPut> &ev);
    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVMovedPatch> &ev);
    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVPatchStart> &ev);
    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> &ev);

    void SendToProxy(std::unique_ptr<IEventBase> event, ui64 cookie = 0, NWilson::TTraceId traceId = {});
    void SendResponseAndDie(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats, TActorId source, ui64 cookie);
    void SendResponseAndDie(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats = nullptr);

    void PassAway() override;

    void SendResponse(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats, TActorId source,
        ui64 cookie, bool term = true);
    void SendResponse(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats = nullptr);

    static double GetStartTime(const NKikimrBlobStorage::TTimestamps& timestamps);
    static double GetTotalTimeMs(const NKikimrBlobStorage::TTimestamps& timestamps);
    static double GetVDiskTimeMs(const NKikimrBlobStorage::TTimestamps& timestamps);

private:
    void CheckPostponedQueue();

protected:
    const TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TIntrusivePtr<TGroupQueues> GroupQueues;
    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    TIntrusivePtr<TStoragePoolCounters> PoolCounters;
    TLogContext LogCtx;
    NWilson::TSpan ParentSpan;
    NWilson::TSpan Span;
    TStackVec<std::pair<TDiskResponsivenessTracker::TDiskId, TDuration>, 16> Responsiveness;
    TString ErrorReason;
    TMaybe<TStoragePoolCounters::EHandleClass> RequestHandleClass;
    ui32 RequestBytes = 0;
    ui32 GeneratedSubrequests = 0;
    ui32 GeneratedSubrequestBytes = 0;
    bool Dead = false;
    const ui32 RestartCounter = 0;
    std::shared_ptr<const TCostModel> CostModel;

private:
    const TActorId Source;
    const ui64 Cookie;
    std::shared_ptr<TMessageRelevanceTracker> MessageRelevanceTracker = std::make_shared<TMessageRelevanceTracker>();
    ui32 RequestsInFlight = 0;
    std::unique_ptr<IEventBase> Response;
    const TMaybe<TGroupStat::EKind> LatencyQueueKind;
    const TInstant RequestStartTime;
    THPTimer Timer;
    std::deque<std::unique_ptr<IEventHandle>> PostponedQ;
    TBlobStorageGroupInfo::TGroupFailDomains RacingDomains; // a set of domains we've received RACE from
    TActorId ProxyActorId;
    std::shared_ptr<TEvBlobStorage::TExecutionRelay> ExecutionRelay;
    bool ExecutionRelayUsed = false;
};

void Encrypt(char *destination, const char *source, size_t shift, size_t sizeBytes, const TLogoBlobID &id,
        const TBlobStorageGroupInfo &info);

void EncryptInplace(TRope& rope, ui32 offset, ui32 size, const TLogoBlobID& id, const TBlobStorageGroupInfo& info);

void Decrypt(char *destination, const char *source, size_t shift, size_t sizeBytes, const TLogoBlobID &id,
        const TBlobStorageGroupInfo &info);
void DecryptInplace(TRope& rope, ui32 offset, ui32 shift, ui32 size, const TLogoBlobID& id, const TBlobStorageGroupInfo& info);

IActor* CreateBlobStorageGroupRangeRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvRange *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupPutRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvPut *ev,
    ui64 cookie, NWilson::TTraceId traceId, bool timeStatsEnabled,
    TDiskResponsivenessTracker::TPerDiskStatsPtr stats,
    TMaybe<TGroupStat::EKind> latencyQueueKind, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
    bool enableRequestMod3x3ForMinLatecy);

IActor* CreateBlobStorageGroupPutRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon,
    TBatchedVec<TEvBlobStorage::TEvPut::TPtr> &ev,
    bool timeStatsEnabled,
    TDiskResponsivenessTracker::TPerDiskStatsPtr stats,
    TMaybe<TGroupStat::EKind> latencyQueueKind, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
    NKikimrBlobStorage::EPutHandleClass handleClass, TEvBlobStorage::TEvPut::ETactic tactic,
    bool enableRequestMod3x3ForMinLatecy);

IActor* CreateBlobStorageGroupGetRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvGet *ev,
    ui64 cookie, NWilson::TTraceId traceId, TNodeLayoutInfoPtr&& nodeLayout,
    TMaybe<TGroupStat::EKind> latencyQueueKind, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupPatchRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvPatch *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters,
    bool useVPatch);

IActor* CreateBlobStorageGroupMultiGetRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvGet *ev,
    ui64 cookie, NWilson::TTraceId traceId, TMaybe<TGroupStat::EKind> latencyQueueKind,
    TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupIndexRestoreGetRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvGet *ev,
    ui64 cookie, NWilson::TTraceId traceId, TMaybe<TGroupStat::EKind> latencyQueueKind,
    TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupDiscoverRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvDiscover *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupMirror3dcDiscoverRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvDiscover *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupMirror3of4DiscoverRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvDiscover *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupCollectGarbageRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvCollectGarbage *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupMultiCollectRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvCollectGarbage *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupBlockRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvBlock *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupStatusRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
    const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvStatus *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters);

IActor* CreateBlobStorageGroupAssimilateRequest(const TIntrusivePtr<TBlobStorageGroupInfo>& info,
    const TIntrusivePtr<TGroupQueues>& state, const TActorId& source,
    const TIntrusivePtr<TBlobStorageGroupProxyMon>& mon, TEvBlobStorage::TEvAssimilate *ev,
    ui64 cookie, NWilson::TTraceId traceId, TInstant now, TIntrusivePtr<TStoragePoolCounters>& storagePoolCounters);

IActor* CreateBlobStorageGroupEjectedProxy(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon);

IActor* CreateBlobStorageGroupProxyConfigured(TIntrusivePtr<TBlobStorageGroupInfo>&& info,
    bool forceWaitAllDrives, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
    TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters, const TControlWrapper &enablePutBatching,
    const TControlWrapper &enableVPatch);

IActor* CreateBlobStorageGroupProxyUnconfigured(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
    const TControlWrapper &enablePutBatching, const TControlWrapper &enableVPatch);

}//NKikimr
