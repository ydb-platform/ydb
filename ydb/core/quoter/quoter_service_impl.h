#pragma once
#include "defs.h"
#include "quoter_service.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/lwtrace/shuttle.h>

#include <util/generic/set.h>
#include <util/generic/deque.h>

namespace NKikimr {
namespace NQuoter {

inline static const TString CONSUMED_COUNTER_NAME = "QuotaConsumed";
inline static const TString REQUESTED_COUNTER_NAME = "QuotaRequested";
inline static const TString RESOURCE_COUNTER_SENSOR_NAME = "resource";
inline static const TString QUOTER_COUNTER_SENSOR_NAME = "quoter";
inline static const TString QUOTER_SERVICE_COUNTER_SENSOR_NAME = "quoter_service";
inline static const TString RESOURCE_QUEUE_SIZE_COUNTER_SENSOR_NAME = "QueueSize";
inline static const TString RESOURCE_QUEUE_WEIGHT_COUNTER_SENSOR_NAME = "QueueWeight";
inline static const TString RESOURCE_ALLOCATED_OFFLINE_COUNTER_SENSOR_NAME = "AllocatedOffline";
inline static const TString RESOURCE_DROPPED_COUNTER_SENSOR_NAME = "QuotaDropped";
inline static const TString RESOURCE_ACCUMULATED_COUNTER_SENSOR_NAME = "QuotaAccumulated";
inline static const TString RESOURCE_RECEIVED_FROM_KESUS_COUNTER_SENSOR_NAME = "QuotaReceivedFromKesus";
inline static const TString REQUEST_QUEUE_TIME_SENSOR_NAME = "RequestQueueTimeMs";
inline static const TString REQUESTS_COUNT_SENSOR_NAME = "RequestsCount";
inline static const TString ELAPSED_MICROSEC_IN_STARVATION_SENSOR_NAME = "ElapsedMicrosecInStarvation";
inline static const TString REQUEST_TIME_SENSOR_NAME = "RequestTimeMs";
inline static const TString DISCONNECTS_COUNTER_SENSOR_NAME = "Disconnects";

using EResourceOperator = TEvQuota::EResourceOperator;
using EStatUpdatePolicy = TEvQuota::EStatUpdatePolicy;
using EUpdateState = TEvQuota::EUpdateState;

struct TRequestId {
    ui32 Value = Max<ui32>();

    operator ui32() const {
        return Value;
    }

    auto operator<=>(const TRequestId&) const = default;
};

struct TResourceLeafId {
    ui32 Value = Max<ui32>();

    operator ui32() const {
        return Value;
    }

    auto operator<=>(const TResourceLeafId&) const = default;
};

struct TResource;

NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets();

struct TRequest {
    TActorId Source = TActorId();
    ui64 EventCookie = 0;

    TInstant StartTime;
    EResourceOperator Operator = EResourceOperator::Unknown;
    TInstant Deadline = TInstant::Max();
    TResourceLeafId ResourceLeaf;

    TRequestId PrevDeadlineRequest;
    TRequestId NextDeadlineRequest;

    TRequestId PrevByOwner;
    TRequestId NextByOwner;

    // tracing
    mutable NLWTrace::TOrbit Orbit;
};

class TReqState {
private:
    TVector<TRequest> Requests;
    THashMap<TActorId, TRequestId> ByOwner;
    TVector<TRequestId> Unused;
public:
    TRequestId Idx(TRequest &request);
    TRequest& Get(TRequestId idx);
    TRequestId HeadByOwner(TActorId ownerId);
    TRequestId Allocate(TActorId source, ui64 eventCookie);
    void Free(TRequestId idx);
};

enum class EResourceState {
    Unknown,
    ResolveQuoter,
    ResolveResource,
    Wait,
    Cleared,
};

struct TResourceLeaf {
    TResource *Resource = nullptr;
    ui64 Amount = Max<ui64>();
    bool IsUsedAmount = false;

    TRequestId RequestIdx;

    TResourceLeafId NextInWaitQueue;
    TResourceLeafId PrevInWaitQueue;

    TResourceLeafId NextResourceLeaf;

    EResourceState State = EResourceState::Unknown;

    ui64 QuoterId = 0;
    ui64 ResourceId = 0;

    TString QuoterName; // optional
    TString ResourceName;

    TInstant StartQueueing = TInstant::Zero(); // optional phase
    TInstant StartCharging = TInstant::Zero(); // when resource is processed
};

class TResState {
private:
    TVector<TResourceLeaf> Leafs;
    TVector<TResourceLeafId> Unused;
public:
    TResourceLeaf& Get(TResourceLeafId idx);
    TResourceLeafId Allocate(TResource *resource, ui64 amount, bool isUsedAmount, TRequestId requestIdx);
    void FreeChain(TResourceLeafId headIdx);
};

struct TResource {
    const ui64 QuoterId;
    const ui64 ResourceId;
    const TString Quoter;
    const TString Resource;
    const TQuoterServiceConfig& QuoterServiceConfig;

    TInstant Activation = TInstant::Zero();
    TInstant NextTick = TInstant::Zero();
    TInstant LastTick = TInstant::Zero();

    TResourceLeafId QueueHead; // to resource leaf
    TResourceLeafId QueueTail;

    ui32 QueueSize = 0;
    double QueueWeight = 0;

    TInstant LastAllocated = TInstant::Max();

    double FreeBalance = 0.0; // could be used w/o pace limit
    double Balance = 0.0; // total balance, but under pace limit
    double TickRate = 0.0;

    TDuration TickSize = TDuration::Seconds(1);
    TMap<ui32, TEvQuota::TUpdateTick> QuotaChannels;

    // stats block
    TEvQuota::EStatUpdatePolicy StatUpdatePolicy = TEvQuota::EStatUpdatePolicy::Never;
    double AmountConsumed = 0.0; // consumed from last stats notification
    TTimeSeriesMap<double> History; // consumption history from last stats notification
    TInstant StartStarvationTime = TInstant::Zero();

    struct {
        ::NMonitoring::TDynamicCounters::TCounterPtr Consumed;
        ::NMonitoring::TDynamicCounters::TCounterPtr Requested;
        ::NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
        ::NMonitoring::TDynamicCounters::TCounterPtr ElapsedMicrosecInStarvation;
        NMonitoring::THistogramPtr RequestQueueTime;
        NMonitoring::THistogramPtr RequestTime;
    } Counters;

    TResource(ui64 quoterId, ui64 resourceId, const TString& quoter, const TString& resource, const TQuoterServiceConfig &quoterServiceConfig, const ::NMonitoring::TDynamicCounterPtr& quoterCounters)
        : QuoterId(quoterId)
        , ResourceId(resourceId)
        , Quoter(quoter)
        , Resource(resource)
        , QuoterServiceConfig(quoterServiceConfig)
    {
        auto counters = quoterCounters->GetSubgroup(RESOURCE_COUNTER_SENSOR_NAME, resource ? resource : "__StaticRatedResource");
        Counters.Consumed = counters->GetCounter(CONSUMED_COUNTER_NAME, true);
        Counters.Requested = counters->GetCounter(REQUESTED_COUNTER_NAME, true);
        Counters.RequestQueueTime = counters->GetHistogram(REQUEST_QUEUE_TIME_SENSOR_NAME, GetLatencyHistogramBuckets());
        Counters.RequestTime = counters->GetHistogram(REQUEST_TIME_SENSOR_NAME, GetLatencyHistogramBuckets());
        Counters.RequestsCount = counters->GetCounter(REQUESTS_COUNT_SENSOR_NAME, true);
        Counters.ElapsedMicrosecInStarvation = counters->GetCounter(ELAPSED_MICROSEC_IN_STARVATION_SENSOR_NAME, true);
    }

    void ApplyQuotaChannel(const TEvQuota::TUpdateTick &tick);
    TDuration Charge(double amount, TInstant now); // Zero - fullfiled, Max - not in current tick, Duration - in current tick, but not right now due to pace limit
    TDuration Charge(TRequest& request, TResourceLeaf& leaf, TInstant now);
    void ChargeUsedAmount(double amount, TInstant now);

    void MarkStartedCharging(TRequest& request, TResourceLeaf& leaf, TInstant now);
    void StartStarvation(TInstant now);
    void StopStarvation(TInstant now);
};

struct TScheduleTick {
    ui32 ActivationHead = Max<ui32>();
};

struct TQuoterState {
    const TString QuoterName;
    TActorId ProxyId;

    THashMap<ui64, THolder<TResource>> Resources;
    THashMap<TString, ui64> ResourcesIndex;

    TSet<TRequestId> WaitingQueueResolve; // => requests
    TMap<TString, TSet<TRequestId>> WaitingResource; // => requests

    struct {
        ::NMonitoring::TDynamicCounterPtr QuoterCounters;
    } Counters;

    TResource& GetOrCreate(ui64 quoterId, ui64 resId, const TString& quoter, const TString& resource, const TQuoterServiceConfig &quoterServiceConfig);
    bool Empty();

    void InitCounters(const ::NMonitoring::TDynamicCounterPtr& serviceCounters) {
        Counters.QuoterCounters = serviceCounters->GetSubgroup(QUOTER_COUNTER_SENSOR_NAME, QuoterName);
    }

    TQuoterState(const TString& quoterName, const ::NMonitoring::TDynamicCounterPtr& serviceCounters)
        : QuoterName(quoterName)
    {
        if (serviceCounters) {
            InitCounters(serviceCounters);
        }
    }
};

class TQuoterService : public TActorBootstrapped<TQuoterService> {
    TQuoterServiceConfig Config;
    TInstant LastProcessed;

    THashMap<TInstant, TSet<TResource*>> ScheduleAllocation; // some sort of linked list instead of TSet?
    THashMap<TInstant, TSet<TResource*>> ScheduleFeed;
    THashMap<TInstant, TRequestId> ScheduleDeadline;

    THashMap<ui64, TRequestId> Deadlines; // ticknum => req-idx
    TResState ResState;
    TReqState ReqState;

    THashMap<ui64, TQuoterState> Quoters;
    THashMap<TString, ui64> QuotersIndex;
    ui64 QuoterIdCounter = 1;

    TQuoterState StaticRatedQuoter; // ??? could be just static rated quoters, w/o all fancy quoter stuff

    bool TickScheduled;

    TMap<ui64, TDeque<TEvQuota::TProxyStat>> StatsToPublish; // quoterId -> stats

    struct {
        ::NMonitoring::TDynamicCounterPtr ServiceCounters;
        ::NMonitoring::TDynamicCounters::TCounterPtr ActiveQuoterProxies;
        ::NMonitoring::TDynamicCounters::TCounterPtr ActiveProxyResources;
        ::NMonitoring::TDynamicCounters::TCounterPtr KnownLocalResources;
        ::NMonitoring::TDynamicCounters::TCounterPtr RequestsInFly;
        ::NMonitoring::TDynamicCounters::TCounterPtr Requests;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultOk;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultDeadline;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultRpcDeadline;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultError;
        NMonitoring::THistogramPtr RequestLatency;
    } Counters;

    enum class EInitLeafStatus {
        Unknown,
        Forbid,
        Charged,
        Wait,
        GenericError,
    };

    void ScheduleNextTick(TInstant requested, TResource &quores);
    TInstant TimeToGranularity(TInstant rawTime);
    void TryTickSchedule(TInstant now = TInstant::Zero());

    void ReplyRequest(TRequest &request, TRequestId reqIdx, TEvQuota::TEvClearance::EResult resultCode);
    void ForgetRequest(TRequest &request, TRequestId reqIdx);
    void DeclineRequest(TRequest &request, TRequestId reqIdx);
    void FailRequest(TRequest &request, TRequestId reqIdx);
    void AllowRequest(TRequest &request, TRequestId reqIdx);
    void DeadlineRequest(TRequest &request, TRequestId reqIdx);

    EInitLeafStatus InitSystemLeaf(const TEvQuota::TResourceLeaf &leaf, TRequest &request, TRequestId reqIdx);
    EInitLeafStatus InitResourceLeaf(const TEvQuota::TResourceLeaf &leaf, TRequest &request, TRequestId reqIdx);
    EInitLeafStatus TryCharge(TResource& quores, ui64 quoterId, ui64 resourceId, const TEvQuota::TResourceLeaf &leaf, TRequest &request, TRequestId reqIdx);
    EInitLeafStatus NotifyUsed(TResource& quores, ui64 quoterId, ui64 resourceId, const TEvQuota::TResourceLeaf &leaf, TRequest &request);
    void MarkScheduleAllocation(TResource& quores, TDuration delay, TInstant now);

    void InitialRequestProcessing(TEvQuota::TEvRequest::TPtr &ev, const TRequestId reqIdx);

    void ForbidResource(TResource &quores);
    void CheckRequest(TRequestId reqIdx);
    void FillStats(TResource &quores);
    void FeedResource(TResource &quores);
    void AllocateResource(TResource &quores);
    void PublishStats();

    void Handle(NMon::TEvHttpInfo::TPtr &ev);
    void Handle(TEvQuota::TEvRequest::TPtr &ev);
    void Handle(TEvQuota::TEvCancelRequest::TPtr &ev);
    void Handle(TEvQuota::TEvProxySession::TPtr &ev);
    void Handle(TEvQuota::TEvProxyUpdate::TPtr &ev);
    void Handle(TEvQuota::TEvRpcTimeout::TPtr &ev);
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev);
    void HandleTick();

    void CreateKesusQuoter(NSchemeCache::TSchemeCacheNavigate::TEntry &navigate, decltype(QuotersIndex)::iterator indexIt, decltype(Quoters)::iterator quoterIt);
    void BreakQuoter(decltype(QuotersIndex)::iterator indexIt, decltype(Quoters)::iterator quoterIt);
    void BreakQuoter(decltype(Quoters)::iterator quoterIt);

    TString PrintEvent(const TEvQuota::TEvRequest::TPtr& ev);
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::QUOTER_SERVICE_ACTOR;
    }

    TQuoterService(const TQuoterServiceConfig &config);
    ~TQuoterService();

    void Bootstrap();

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NMon::TEvHttpInfo, Handle);
            hFunc(TEvQuota::TEvRequest, Handle);
            hFunc(TEvQuota::TEvCancelRequest, Handle);
            hFunc(TEvQuota::TEvProxySession, Handle);
            hFunc(TEvQuota::TEvProxyUpdate, Handle);
            hFunc(TEvQuota::TEvRpcTimeout, Handle);
            cFunc(TEvents::TEvWakeup::EventType, HandleTick);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        default:
            LOG_WARN_S(*TlsActivationContext, NKikimrServices::QUOTER_SERVICE, "TQuoterService::StateFunc unexpected event type# "
                << ev->GetTypeRewrite()
                << " event: "
                << ev->ToString());
            break;
        }

        PublishStats();
    }
};

}
}
