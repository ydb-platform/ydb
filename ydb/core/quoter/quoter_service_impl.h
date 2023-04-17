#pragma once
#include "defs.h"
#include "quoter_service.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/lwtrace/shuttle.h>

#include <util/generic/set.h>
#include <util/generic/deque.h>

namespace NKikimr {
namespace NQuoter {

extern const TString CONSUMED_COUNTER_NAME;
extern const TString REQUESTED_COUNTER_NAME;
extern const TString RESOURCE_COUNTER_SENSOR_NAME;
extern const TString QUOTER_COUNTER_SENSOR_NAME;
extern const TString QUOTER_SERVICE_COUNTER_SENSOR_NAME;
extern const TString RESOURCE_QUEUE_SIZE_COUNTER_SENSOR_NAME;
extern const TString RESOURCE_QUEUE_WEIGHT_COUNTER_SENSOR_NAME;
extern const TString RESOURCE_ALLOCATED_OFFLINE_COUNTER_SENSOR_NAME;
extern const TString RESOURCE_DROPPED_COUNTER_SENSOR_NAME;
extern const TString RESOURCE_ACCUMULATED_COUNTER_SENSOR_NAME;
extern const TString RESOURCE_RECEIVED_FROM_KESUS_COUNTER_SENSOR_NAME;
extern const TString REQUEST_QUEUE_TIME_SENSOR_NAME;
extern const TString REQUEST_TIME_SENSOR_NAME;
extern const TString REQUESTS_COUNT_SENSOR_NAME;
extern const TString ELAPSED_MICROSEC_IN_STARVATION_SENSOR_NAME;
extern const TString DISCONNECTS_COUNTER_SENSOR_NAME;

using EResourceOperator = TEvQuota::EResourceOperator;
using EStatUpdatePolicy = TEvQuota::EStatUpdatePolicy;
using EUpdateState = TEvQuota::EUpdateState;

struct TResource;

NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets();

struct TRequest {
    TActorId Source = TActorId();
    ui64 EventCookie = 0;

    TInstant StartTime;
    EResourceOperator Operator = EResourceOperator::Unknown;
    TInstant Deadline = TInstant::Max();
    ui32 ResourceLeaf = Max<ui32>();

    ui32 PrevDeadlineRequest = Max<ui32>();
    ui32 NextDeadlineRequest = Max<ui32>();

    ui32 PrevByOwner = Max<ui32>();
    ui32 NextByOwner = Max<ui32>();

    // tracing
    mutable NLWTrace::TOrbit Orbit;
};

class TReqState {
private:
    TVector<TRequest> Requests;
    THashMap<TActorId, ui32> ByOwner;
    TVector<ui32> Unused;
public:
    ui32 Idx(TRequest &request);
    TRequest& Get(ui32 idx);
    ui32 HeadByOwner(TActorId ownerId);
    ui32 Allocate(TActorId source, ui64 eventCookie);
    void Free(ui32 idx);
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

    ui32 RequestIdx = Max<ui32>();

    ui32 NextInWaitQueue = Max<ui32>();
    ui32 PrevInWaitQueue = Max<ui32>();

    ui32 NextResourceLeaf = Max<ui32>();

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
    TVector<ui32> Unused;
public:
    TResourceLeaf& Get(ui32 idx);
    ui32 Allocate(TResource *resource, ui64 amount, bool isUsedAmount, ui32 requestIdx);
    void FreeChain(ui32 headIdx);
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

    ui32 QueueHead = Max<ui32>(); // to resource leaf
    ui32 QueueTail = Max<ui32>();

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

    TSet<ui32> WaitingQueueResolve; // => requests
    TMap<TString, TSet<ui32>> WaitingResource; // => requests

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
    THashMap<TInstant, ui32> ScheduleDeadline;

    THashMap<ui64, ui32> Deadlines; // ticknum => req-idx
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

    void ReplyRequest(TRequest &request, ui32 reqIdx, TEvQuota::TEvClearance::EResult resultCode);
    void ForgetRequest(TRequest &request, ui32 reqIdx);
    void DeclineRequest(TRequest &request, ui32 reqIdx);
    void FailRequest(TRequest &request, ui32 reqIdx);
    void AllowRequest(TRequest &request, ui32 reqIdx);
    void DeadlineRequest(TRequest &request, ui32 reqIdx);

    EInitLeafStatus InitSystemLeaf(const TEvQuota::TResourceLeaf &leaf, TRequest &request, ui32 reqIdx);
    EInitLeafStatus InitResourceLeaf(const TEvQuota::TResourceLeaf &leaf, TRequest &request, ui32 reqIdx);
    EInitLeafStatus TryCharge(TResource& quores, ui64 quoterId, ui64 resourceId, const TEvQuota::TResourceLeaf &leaf, TRequest &request, ui32 reqIdx);
    EInitLeafStatus NotifyUsed(TResource& quores, ui64 quoterId, ui64 resourceId, const TEvQuota::TResourceLeaf &leaf, TRequest &request);
    void MarkScheduleAllocation(TResource& quores, TDuration delay, TInstant now);

    void InitialRequestProcessing(TEvQuota::TEvRequest::TPtr &ev, const ui32 reqIdx);

    void ForbidResource(TResource &quores);
    void CheckRequest(ui32 reqIdx);
    void FillStats(TResource &quores);
    void FeedResource(TResource &quores);
    void AllocateResource(TResource &quores);
    void PublishStats();

    void Handle(TEvQuota::TEvRequest::TPtr &ev);
    void Handle(TEvQuota::TEvCancelRequest::TPtr &ev);
    void Handle(TEvQuota::TEvProxySession::TPtr &ev);
    void Handle(TEvQuota::TEvProxyUpdate::TPtr &ev);
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
            hFunc(TEvQuota::TEvRequest, Handle);
            hFunc(TEvQuota::TEvCancelRequest, Handle);
            hFunc(TEvQuota::TEvProxySession, Handle);
            hFunc(TEvQuota::TEvProxyUpdate, Handle);
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
