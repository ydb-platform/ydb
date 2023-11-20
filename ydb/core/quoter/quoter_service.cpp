#include "quoter_service_impl.h"
#include "debug_info.h"
#include "kesus_quoter_proxy.h"
#include "probes.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/events.h>
#include <ydb/core/mon/mon.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <cmath>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR
#error log macro definition clash
#endif

#define BLOG_T(stream) LOG_TRACE_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_SERVICE, stream)
#define BLOG_D(stream) LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_SERVICE, stream)
#define BLOG_I(stream) LOG_INFO_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_SERVICE, stream)
#define BLOG_WARN(stream) LOG_WARN_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_SERVICE, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S((TlsActivationContext->AsActorContext()), NKikimrServices::QUOTER_SERVICE, stream)

LWTRACE_USING(QUOTER_SERVICE_PROVIDER);

namespace NKikimr {
namespace NQuoter {

constexpr double TICK_RATE_EPSILON = 0.0000000001;

static_assert(EventSpaceBegin(NKikimr::TKikimrEvents::ES_QUOTA) == EventSpaceBegin(NKikimr::TEvQuota::ES_QUOTA), "quoter event ids mismatch");

NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
    return NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000});
}

TRequest& TReqState::Get(TRequestId idx) {
    Y_ABORT_UNLESS(idx.Value < Requests.size());
    auto &x = Requests[idx.Value];
    Y_ABORT_UNLESS(x.Source);
    return Requests[idx.Value];
}

TRequestId TReqState::Idx(TRequest &request) {
    const TRequestId idx = TRequestId{static_cast<ui32>(&request - &*Requests.begin())};
    Y_ABORT_UNLESS(idx.Value < Requests.size());
    return idx;
}

TRequestId TReqState::HeadByOwner(TActorId ownerId) {
    if (TRequestId *x = ByOwner.FindPtr(ownerId))
        return *x;
    else
        return TRequestId{};
}

TRequestId TReqState::Allocate(TActorId source, ui64 eventCookie) {
    TRequestId idx;
    if (Unused) {
        idx = Unused.back();
        Unused.pop_back();
    } else {
        idx = TRequestId{static_cast<ui32>(Requests.size())};
        Requests.emplace_back();
    }

    auto &x = Requests[idx.Value];
    x.Source = source;
    x.EventCookie = eventCookie;
    x.StartTime = TActivationContext::Now();

    Y_DEBUG_ABORT_UNLESS(x.PrevByOwner == TRequestId{});
    Y_DEBUG_ABORT_UNLESS(x.NextByOwner == TRequestId{});
    Y_DEBUG_ABORT_UNLESS(x.PrevDeadlineRequest == TRequestId{});
    Y_DEBUG_ABORT_UNLESS(x.NextDeadlineRequest == TRequestId{});

    auto itpair = ByOwner.emplace(source, idx);
    if (!itpair.second) {
        TRequestId &other = itpair.first->second;
        x.NextByOwner = other;
        Requests[other].PrevByOwner = idx;
        other = idx;
    }

    return idx;
}

void TReqState::Free(TRequestId idx) {
    auto &x = Get(idx);

    bool lastEntry = true;
    if (x.NextByOwner != TRequestId{}) {
        lastEntry = false;
        Requests[x.NextByOwner].PrevByOwner = x.PrevByOwner;
    }

    if (x.PrevByOwner != TRequestId{}) {
        lastEntry = false;
        Requests[x.PrevByOwner].NextByOwner = x.NextByOwner;
    }

    if (lastEntry) {
        ByOwner.erase(x.Source);
    } else {
        auto byOwnerIt = ByOwner.find(x.Source);
        Y_DEBUG_ABORT_UNLESS(byOwnerIt != ByOwner.end());
        if (byOwnerIt->second == idx) {
            byOwnerIt->second = x.NextByOwner != TRequestId{} ? x.NextByOwner : x.PrevByOwner;
        }
    }

    x.NextByOwner = TRequestId{};
    x.PrevByOwner = TRequestId{};

    if (x.NextDeadlineRequest != TRequestId{}) {
        Requests[x.NextDeadlineRequest].PrevDeadlineRequest = x.PrevDeadlineRequest;
    }

    if (x.PrevDeadlineRequest != TRequestId{}) {
        Requests[x.PrevDeadlineRequest].NextDeadlineRequest = x.NextDeadlineRequest;
    }

    x.PrevDeadlineRequest = TRequestId{};
    x.NextDeadlineRequest = TRequestId{};

    x.Source = TActorId();
    x.Orbit.Reset();

    Unused.push_back(idx);
}

TResourceLeaf& TResState::Get(TResourceLeafId idx) {
    Y_ABORT_UNLESS(idx < Leafs.size());
    return Leafs[idx];
}

TResourceLeafId TResState::Allocate(TResource *resource, ui64 amount, bool isUsedAmount, TRequestId requestIdx) {
    TResourceLeafId idx;
    if (Unused) {
        idx = Unused.back();
        Unused.pop_back();
    } else {
        idx = TResourceLeafId{static_cast<ui32>(Leafs.size())};
        Leafs.emplace_back();
    }

    auto &x = Leafs[idx];
    x.Resource = resource;
    x.Amount = amount;
    x.IsUsedAmount = isUsedAmount;
    x.RequestIdx = requestIdx;

    Y_DEBUG_ABORT_UNLESS(x.NextInWaitQueue == TRequestId{});
    Y_DEBUG_ABORT_UNLESS(x.PrevInWaitQueue == TRequestId{});
    Y_DEBUG_ABORT_UNLESS(x.NextResourceLeaf == TRequestId{});

    Y_DEBUG_ABORT_UNLESS(x.State == EResourceState::Unknown);
    Y_DEBUG_ABORT_UNLESS(x.QuoterId == 0);
    Y_DEBUG_ABORT_UNLESS(x.ResourceId == 0);
    Y_DEBUG_ABORT_UNLESS(x.QuoterName.empty());
    Y_DEBUG_ABORT_UNLESS(x.ResourceName.empty());

    return idx;
}

void TResState::FreeChain(TResourceLeafId headIdx) {
    while (headIdx != TResourceLeafId{}) {
        auto &x = Get(headIdx);
        Y_DEBUG_ABORT_UNLESS(x.Resource == nullptr);
        Y_DEBUG_ABORT_UNLESS(x.NextInWaitQueue == TRequestId{});
        Y_DEBUG_ABORT_UNLESS(x.PrevInWaitQueue == TRequestId{});

        Unused.push_back(headIdx);
        headIdx = x.NextResourceLeaf;
        x.NextResourceLeaf = TResourceLeafId{};

        x.Amount = Max<ui64>();
        x.RequestIdx = TRequestId{};
        x.State = EResourceState::Unknown;
        x.QuoterId = 0;
        x.ResourceId = 0;
        TString().swap(x.ResourceName);
        TString().swap(x.QuoterName);
        x.StartQueueing = TInstant::Zero();
        x.StartCharging = TInstant::Zero();
    }
}

void TResource::ApplyQuotaChannel(const TEvQuota::TUpdateTick &tick) {
    // full rewrite
    // todo: incremental update (?) as-is would be applied on next tick
    QuotaChannels[tick.Channel] = tick;
}

void TResource::MarkStartedCharging(TRequest& request, TResourceLeaf& leaf, TInstant now) {
    if (leaf.StartCharging == TInstant::Zero()) {
        leaf.StartCharging = now;
        LWTRACK(StartCharging, request.Orbit, leaf.QuoterName, leaf.ResourceName, leaf.QuoterId, leaf.ResourceId);
        if (leaf.StartQueueing == TInstant::Zero()) { // was not in queue
            Counters.RequestQueueTime->Collect(0);
        } else {
            Counters.RequestQueueTime->Collect((now - leaf.StartQueueing).MilliSeconds());
        }
    }
}

void TResource::StartStarvation(TInstant now) {
    StopStarvation(now);
    StartStarvationTime = now;
}

void TResource::StopStarvation(TInstant now) {
    if (StartStarvationTime != TInstant::Zero()) {
        *Counters.ElapsedMicrosecInStarvation += (now - StartStarvationTime).MicroSeconds();
        StartStarvationTime = TInstant::Zero();
    }
}

TDuration TResource::Charge(TRequest& request, TResourceLeaf& leaf, TInstant now) {
    MarkStartedCharging(request, leaf, now);

    if (leaf.IsUsedAmount) {
        ChargeUsedAmount(leaf.Amount, now);
        Counters.RequestTime->Collect((now - request.StartTime).MilliSeconds());
        return TDuration::Zero();
    }

    const TDuration result = Charge(leaf.Amount, now);
    if (result == TDuration::Zero()) {
        Counters.RequestTime->Collect((now - request.StartTime).MilliSeconds());
    }
    return result;
}

void TResource::ChargeUsedAmount(double amount, TInstant now) {
    BLOG_T("ChargeUsedAmount \"" << Resource << "\" for " << amount
           << ". Balance: " << Balance
           << ". FreeBalance: " << FreeBalance
           << ". Now: " << now);
    LastAllocated = now;
    FreeBalance -= amount;
    Balance -= amount;
    AmountConsumed += amount;
    if (StatUpdatePolicy != EStatUpdatePolicy::Never) {
        History.Add(now, amount);
    }
    Counters.Consumed->Add(static_cast<i64>(amount));
    if (Balance >= 0.0) {
        StopStarvation(now);
        return;
    }
    StartStarvation(now);
}

TDuration TResource::Charge(double amount, TInstant now) {
// Zero - charged
// Max - not in current tick (or resource already queued)
// smth b/w - delayed by pace limit
    if (TickRate < TICK_RATE_EPSILON) { // zero
        return TDuration::Max();
    }

    // could be fullfilled right now?
    const double ticksToFullfill = amount / TickRate;
    const double durationToFullfillInUs = ticksToFullfill * static_cast<double>(TickSize.MicroSeconds());
    // TODO: calculate time for many requests (not for one). Now errors can be accumulated when big rates are used.
    const TInstant timeToFullfill = LastAllocated + TDuration::MicroSeconds(lround(durationToFullfillInUs));

    BLOG_T("Charge \"" << Resource << "\" for " << amount
           << ". Balance: " << Balance
           << ". FreeBalance: " << FreeBalance
           << ". TicksToFullfill: " << ticksToFullfill
           << ". DurationToFullfillInUs: " << durationToFullfillInUs
           << ". TimeToFullfill: " << timeToFullfill
           << ". Now: " << now
           << ". LastAllocated: " << LastAllocated);

    if (Balance >= 0.0) {
        if (timeToFullfill <= now) {
            LastAllocated = Max(now - QuoterServiceConfig.ScheduleTickSize * 2, timeToFullfill);
            Balance -= amount;
            AmountConsumed += amount;
            if (StatUpdatePolicy != EStatUpdatePolicy::Never) {
                History.Add(now, amount);
            }

            if (FreeBalance > Balance)
                FreeBalance = Balance;

            Counters.Consumed->Add(static_cast<i64>(amount));
            StopStarvation(now);
            return TDuration::Zero();
        }

        if (amount <= FreeBalance) {
            LastAllocated = now;
            FreeBalance -= amount;
            Balance -= amount;
            AmountConsumed += amount;
            if (StatUpdatePolicy != EStatUpdatePolicy::Never) {
                History.Add(now, amount);
            }

            Counters.Consumed->Add(static_cast<i64>(amount));
            StopStarvation(now);
            return TDuration::Zero();
        }
    }

    StartStarvation(now);
    const TDuration delay = timeToFullfill - now;
    return (delay > TDuration::Zero()) ? delay : TDuration::Max();
}

TResource& TQuoterState::GetOrCreate(ui64 quoterId, ui64 resId, const TString& quoter, const TString& resource, const TQuoterServiceConfig &quoterServiceConfig) {
    auto xpair = Resources.emplace(resId, nullptr);
    if (xpair.second)
        xpair.first->second.Reset(new TResource(quoterId, resId, quoter, resource, quoterServiceConfig, Counters.QuoterCounters));

    return *xpair.first->second;
}

bool TQuoterState::Empty() {
    return Resources.empty() && WaitingResource.empty() && WaitingQueueResolve.empty();
}

TQuoterService::TQuoterService(const TQuoterServiceConfig &config)
    : Config(config)
    , LastProcessed(TInstant::Zero())
    , StaticRatedQuoter("__StaticRatedQuoter", nullptr)
    , TickScheduled(false)
{
    QUOTER_SYSTEM_DEBUG(DebugInfo->QuoterService = this);
}

TQuoterService::~TQuoterService() {
    QUOTER_SYSTEM_DEBUG(DebugInfo->QuoterService = nullptr);
}

void TQuoterService::ScheduleNextTick(TInstant requested, TResource &quores) {
    TryTickSchedule();
    const TInstant next = TimeToGranularity(requested);
    const TInstant last = TimeToGranularity(quores.LastTick + quores.TickSize);
    const TInstant selected = Max(next, last, LastProcessed);
    quores.NextTick = selected;
    quores.LastTick = selected;
    BLOG_T("Schedule next tick for \"" << quores.Resource << "\". Tick size: " << quores.TickSize << ". Time: " << quores.NextTick);
    ScheduleFeed[quores.NextTick].emplace(&quores);
}

TInstant TQuoterService::TimeToGranularity(TInstant rawTime) {
    // up to next schedule tick
    const ui64 rawUs = rawTime.MicroSeconds();
    const ui64 schedUs = Config.ScheduleTickSize.MicroSeconds();
    const ui64 x = (rawUs + (schedUs - 1)) / schedUs * schedUs;
    return TInstant::MicroSeconds(x);
}

void TQuoterService::Bootstrap() {
    NActors::TMon *mon = AppData()->Mon;
    if (mon) {
        NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(actorsMonPage, "quoter_proxy", "QuoterProxy", false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
    }

    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = GetServiceCounters(AppData()->Counters, QUOTER_SERVICE_COUNTER_SENSOR_NAME);

    Counters.ActiveQuoterProxies = counters->GetCounter("ActiveQuoterProxies", false);
    Counters.ActiveProxyResources = counters->GetCounter("ActiveProxyResources", false);
    Counters.KnownLocalResources = counters->GetCounter("KnownLocalResources", false);
    Counters.RequestsInFly = counters->GetCounter("RequestsInFly", false);
    Counters.Requests = counters->GetCounter("Requests", true);
    Counters.ResultOk = counters->GetCounter("ResultOk", true);
    Counters.ResultDeadline = counters->GetCounter("ResultDeadline", true);
    Counters.ResultRpcDeadline = counters->GetCounter("ResultRpcDeadline", true);
    Counters.ResultError = counters->GetCounter("ResultError", true);
    Counters.RequestLatency = counters->GetHistogram("RequestLatencyMs", GetLatencyHistogramBuckets());

    Counters.ServiceCounters = std::move(counters);

    StaticRatedQuoter.InitCounters(Counters.ServiceCounters);

    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(QUOTER_SERVICE_PROVIDER));

    Become(&TThis::StateFunc);
}

void TQuoterService::TryTickSchedule(TInstant now) {
    if (!TickScheduled) {
        TickScheduled = true;
        LastProcessed = TimeToGranularity(now != TInstant::Zero() ? now : TActivationContext::Now());
        Schedule(Config.ScheduleTickSize, new TEvents::TEvWakeup());
    }
}

void TQuoterService::ReplyRequest(TRequest &request, TRequestId reqIdx, TEvQuota::TEvClearance::EResult resultCode) {
    LWTRACK(RequestDone, request.Orbit, resultCode, request.EventCookie);
    Send(request.Source, new TEvQuota::TEvClearance(resultCode), 0, request.EventCookie);

    ForgetRequest(request, reqIdx);
}

void TQuoterService::ForgetRequest(TRequest &request, TRequestId reqIdx) {
    // request must be replied
    // we must not stop track request while not replied or explicitly canceled
    // so only correct entry points are from ReplyRequest or from CancelRequest

    // cleanup from resource wait queue
    for (TResourceLeafId leafIdx = request.ResourceLeaf; leafIdx != TResourceLeafId{}; ) {
        TResourceLeaf &leaf = ResState.Get(leafIdx);

        switch (leaf.State) {
        case EResourceState::Unknown:
        case EResourceState::Cleared:
            break;
        case EResourceState::Wait:
            if (leaf.Resource) {
                if (leaf.NextInWaitQueue != Max<ui32>()) {
                    ResState.Get(leaf.NextInWaitQueue).PrevInWaitQueue = leaf.PrevInWaitQueue;
                } else {
                    Y_ABORT_UNLESS(leaf.Resource->QueueTail == leafIdx);
                    leaf.Resource->QueueTail = leaf.PrevInWaitQueue;
                }

                if (leaf.PrevInWaitQueue != Max<ui32>()) {
                    ResState.Get(leaf.PrevInWaitQueue).NextInWaitQueue = leaf.NextInWaitQueue;
                } else {
                    Y_ABORT_UNLESS(leaf.Resource->QueueHead == leafIdx);
                    leaf.Resource->QueueHead = leaf.NextInWaitQueue;
                }

                if (leaf.Resource->QueueHead == Max<ui32>()) {
                    leaf.Resource->QueueSize = 0;
                    leaf.Resource->QueueWeight = 0.0;
                } else {
                    leaf.Resource->QueueSize -= 1;
                    leaf.Resource->QueueWeight -= leaf.Amount;
                }

                // TODO: resource schedule update over new active entry

                leaf.Resource = nullptr;
                leaf.PrevInWaitQueue = TResourceLeafId{};
                leaf.NextInWaitQueue = TResourceLeafId{};
            }
            break;
        case EResourceState::ResolveQuoter:
            if (TQuoterState *quoter = Quoters.FindPtr(leaf.QuoterId))
                quoter->WaitingQueueResolve.erase(reqIdx);
            break;
        case EResourceState::ResolveResource:
            if (TQuoterState *quoter = Quoters.FindPtr(leaf.QuoterId))
                if (TSet<TRequestId> *resWaitMap = quoter->WaitingResource.FindPtr(leaf.ResourceName))
                    resWaitMap->erase(reqIdx);
            break;
        }

        leaf.State = EResourceState::Unknown;
        leafIdx = leaf.NextResourceLeaf;
    }

    ResState.FreeChain(request.ResourceLeaf);
    request.ResourceLeaf = TResourceLeafId{};

    // cleanup from deadline queue is inside of generic ReqState::Free
    ReqState.Free(reqIdx);

    Counters.RequestsInFly->Dec();
}

void TQuoterService::DeclineRequest(TRequest &request, TRequestId reqIdx) {
    Counters.ResultError->Inc();

    return ReplyRequest(request, reqIdx, TEvQuota::TEvClearance::EResult::UnknownResource);
}

void TQuoterService::FailRequest(TRequest &request, TRequestId reqIdx) {
    Counters.ResultError->Inc();

    return ReplyRequest(request, reqIdx, TEvQuota::TEvClearance::EResult::GenericError);
}

void TQuoterService::AllowRequest(TRequest &request, TRequestId reqIdx) {
    Counters.ResultOk->Inc();
    Counters.RequestLatency->Collect((TActivationContext::Now() - request.StartTime).MilliSeconds());

    return ReplyRequest(request, reqIdx, TEvQuota::TEvClearance::EResult::Success);
}

void TQuoterService::DeadlineRequest(TRequest &request, TRequestId reqIdx) {
    Counters.ResultDeadline->Inc();

    return ReplyRequest(request, reqIdx, TEvQuota::TEvClearance::EResult::Deadline);
}

TQuoterService::EInitLeafStatus TQuoterService::InitSystemLeaf(const TEvQuota::TResourceLeaf &leaf, TRequest &request, TRequestId reqIdx) {
    if (leaf.ResourceId == TEvQuota::TResourceLeaf::ResourceForbid) {
        return EInitLeafStatus::Forbid;
    }

    if (leaf.ResourceId == TEvQuota::TResourceLeaf::ResourceNocheck) {
        // do nothing, always allow
        return EInitLeafStatus::Charged;
    }

    if ((leaf.ResourceId & (0x3ULL << 62)) == (1ULL << 62)) {
        // static rated resource
        const ui32 rate = (leaf.ResourceId & 0x3FFFFFFF);
        auto &quores = StaticRatedQuoter.GetOrCreate(leaf.QuoterId, leaf.ResourceId, TString(), TString(), Config);
        if (quores.LastAllocated == TInstant::Max()) {
            Counters.KnownLocalResources->Inc();

            quores.NextTick = TInstant::Zero();
            quores.LastTick = TInstant::Zero();

            quores.QueueHead = TResourceLeafId{};
            quores.QueueTail = TResourceLeafId{};

            quores.LastAllocated = TInstant::Zero();
            quores.AmountConsumed = 0.0;
            // NOTE: do not change `History`: we dont need it for static rate

            quores.FreeBalance = 0.0;
            quores.TickRate = static_cast<double>(rate);
            quores.Balance = quores.TickRate;

            quores.TickSize = TDuration::Seconds(1);
            quores.StatUpdatePolicy = EStatUpdatePolicy::Never;

            quores.ApplyQuotaChannel(TEvQuota::TUpdateTick(0, Max<ui32>(), rate, TEvQuota::ETickPolicy::Ahead));
            FeedResource(quores);
        }

        if (quores.NextTick == TInstant::Zero()) {
            ScheduleNextTick(TActivationContext::Now(), quores);
        }

        return TryCharge(quores, TEvQuota::TResourceLeaf::QuoterSystem, QuoterIdCounter, leaf, request, reqIdx);
    }

    return EInitLeafStatus::Unknown;
}

TQuoterService::EInitLeafStatus TQuoterService::InitResourceLeaf(const TEvQuota::TResourceLeaf &leaf, TRequest &request, TRequestId reqIdx) {
    // resolve quoter
    ui64 quoterId = leaf.QuoterId;
    TQuoterState *quoter = quoterId ? Quoters.FindPtr(quoterId) : nullptr;
    if (quoter == nullptr) {
        if (!leaf.Quoter)
            return EInitLeafStatus::GenericError;

        auto qIndxIt = QuotersIndex.find(leaf.Quoter);
        if (qIndxIt == QuotersIndex.end()) {
            TVector<TString> path = NKikimr::SplitPath(leaf.Quoter);
            if (path.empty()) {
                BLOG_WARN("Empty path to quoter is provided: \"" << leaf.Quoter << "\"");
                return EInitLeafStatus::GenericError;
            }

            if (CanonizePath(path) != leaf.Quoter) {
                BLOG_WARN("Not canonized path to quoter is provided. Provided: \"" << leaf.Quoter << "\", but canonized is \"" << CanonizePath(path) << "\"");
                return EInitLeafStatus::GenericError;
            }

            quoterId = ++QuoterIdCounter;
            QuotersIndex.emplace(leaf.Quoter, quoterId);

            quoter = &Quoters.emplace(quoterId, TQuoterState(leaf.Quoter, Counters.ServiceCounters)).first->second;
            Counters.ActiveQuoterProxies->Inc();

            THolder<NSchemeCache::TSchemeCacheNavigate> req(new NSchemeCache::TSchemeCacheNavigate());
            req->ResultSet.emplace_back();
            req->ResultSet.back().Path.swap(path);
            req->ResultSet.back().Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(req), IEventHandle::FlagTrackDelivery, 0);

            BLOG_I("resolve new quoter " << leaf.Quoter);
        } else {
            // ok, got quoterId, proceed
            quoterId = qIndxIt->second;
            quoter = Quoters.FindPtr(quoterId);
            Y_ABORT_UNLESS(quoter != nullptr);
        }
    }

    if (!quoter->ProxyId) {
        quoter->WaitingQueueResolve.emplace(reqIdx);

        // todo: make generic 'leaf for resolve' helper
        const TResourceLeafId resLeafIdx = ResState.Allocate(nullptr, leaf.Amount, leaf.IsUsedAmount, reqIdx);
        TResourceLeaf& resLeaf = ResState.Get(resLeafIdx);

        resLeaf.QuoterId = quoterId;
        resLeaf.QuoterName = leaf.Quoter;
        resLeaf.ResourceId = leaf.ResourceId;
        resLeaf.ResourceName = leaf.Resource;

        resLeaf.NextResourceLeaf = request.ResourceLeaf;
        request.ResourceLeaf = resLeafIdx;

        resLeaf.State = EResourceState::ResolveQuoter;

        return EInitLeafStatus::Wait;
    }

    ui64 resourceId = leaf.ResourceId;
    THolder<TResource> *resHolder = leaf.ResourceId ? quoter->Resources.FindPtr(resourceId) : nullptr;
    if (resHolder == nullptr) {
        if (!leaf.Resource)
            return EInitLeafStatus::GenericError;

        if (const ui64 *rsId = quoter->ResourcesIndex.FindPtr(leaf.Resource)) {
            resourceId = *rsId;
            resHolder = quoter->Resources.FindPtr(resourceId);
        }

        if (resHolder == nullptr) {
            auto rIndxIt = quoter->WaitingResource.emplace(leaf.Resource, TSet<TRequestId>());
            rIndxIt.first->second.emplace(reqIdx);

            const TResourceLeafId resLeafIdx = ResState.Allocate(nullptr, leaf.Amount, leaf.IsUsedAmount, reqIdx);
            TResourceLeaf& resLeaf = ResState.Get(resLeafIdx);

            resLeaf.QuoterId = quoterId;
            resLeaf.QuoterName = leaf.Quoter;
            resLeaf.ResourceId = leaf.ResourceId;
            resLeaf.ResourceName = leaf.Resource;

            resLeaf.NextResourceLeaf = request.ResourceLeaf;
            request.ResourceLeaf = resLeafIdx;

            resLeaf.State = EResourceState::ResolveResource;

            if (rIndxIt.second) { // new resource, create resource session
                BLOG_I("resolve resource " << resLeaf.ResourceName << " on quoter " << quoter->QuoterName);
                Send(quoter->ProxyId, new TEvQuota::TEvProxyRequest(resLeaf.ResourceName));
            }

            return EInitLeafStatus::Wait;
        }
    }

    if ((*resHolder)->NextTick == TInstant::Zero()) {
        ScheduleNextTick(TActivationContext::Now(), **resHolder);
    }

    // ok, got resource
    const EInitLeafStatus chargeResult = TryCharge(**resHolder, quoterId, resourceId, leaf, request, reqIdx);

    switch (resHolder->Get()->StatUpdatePolicy) {
    case EStatUpdatePolicy::EveryTick:
    case EStatUpdatePolicy::EveryActiveTick:
    case EStatUpdatePolicy::OnActivity:
        FillStats(**resHolder);
        break;
    default:
        break;
    }

    return chargeResult;
}

void TQuoterService::MarkScheduleAllocation(TResource& quores, TDuration delay, TInstant now) {
    TryTickSchedule(now);
    Y_ABORT_UNLESS(quores.NextTick != TInstant::Zero() && quores.NextTick != TInstant::Max());
    Y_ABORT_UNLESS(delay > TDuration::Zero());

    if (delay == TDuration::Max()) {
        if (quores.Activation) {
            ScheduleAllocation[quores.Activation].erase(&quores);
            quores.Activation = TInstant::Zero();
        }

        return;
    }

    const TInstant newActivation = TimeToGranularity(now + delay);
    if (quores.Activation && quores.Activation != newActivation) {
        ScheduleAllocation[quores.Activation].erase(&quores);
        quores.Activation = TInstant::Zero();
    }

    if (newActivation < quores.NextTick) {
        ScheduleAllocation[newActivation].emplace(&quores);
        quores.Activation = newActivation;
    }
}

TQuoterService::EInitLeafStatus TQuoterService::TryCharge(TResource& quores, ui64 quoterId, ui64 resourceId, const TEvQuota::TResourceLeaf &leaf, TRequest &request, TRequestId reqIdx) {
    *quores.Counters.Requested += leaf.Amount;
    ++*quores.Counters.RequestsCount;

    const TInstant now = TActivationContext::Now();
    bool startedCharge = false;
    LWTRACK(ResourceQueueState, request.Orbit, leaf.Quoter, leaf.Resource, leaf.QuoterId, leaf.ResourceId, quores.QueueSize, quores.QueueWeight);
    if (leaf.IsUsedAmount) {
        quores.ChargeUsedAmount(leaf.Amount, now);
        LWTRACK(Charge, request.Orbit, leaf.Quoter, leaf.Resource, leaf.QuoterId, leaf.ResourceId);
        quores.Counters.RequestTime->Collect((now - request.StartTime).MilliSeconds());
        return EInitLeafStatus::Charged;
    }

    if (quores.QueueSize == 0) {
        startedCharge = true;
        const TDuration delay = quores.Charge(leaf.Amount, now);

        if (delay == TDuration::Zero()) {
            LWTRACK(StartCharging, request.Orbit, leaf.Quoter, leaf.Resource, leaf.QuoterId, leaf.ResourceId);
            quores.Counters.RequestTime->Collect((now - request.StartTime).MilliSeconds());
            return EInitLeafStatus::Charged;
        }

        MarkScheduleAllocation(quores, delay, now);
    }

    // need wait entry for resource
    const TResourceLeafId resLeafIdx = ResState.Allocate(&quores, leaf.Amount, leaf.IsUsedAmount, reqIdx);
    TResourceLeaf& resLeaf = ResState.Get(resLeafIdx);

    resLeaf.State = EResourceState::Wait;
    if (startedCharge) {
        quores.MarkStartedCharging(request, resLeaf, now);
    } else {
        resLeaf.StartQueueing = now;
    }

    quores.QueueSize += 1;
    quores.QueueWeight += leaf.Amount;

    resLeaf.QuoterId = quoterId;
    resLeaf.QuoterName = leaf.Quoter;
    resLeaf.ResourceId = resourceId;
    resLeaf.ResourceName = leaf.Resource;

    if (quores.QueueTail == Max<ui32>()) {
        quores.QueueTail = resLeafIdx;
        quores.QueueHead = resLeafIdx;
    } else {
        Y_DEBUG_ABORT_UNLESS(ResState.Get(quores.QueueTail).NextInWaitQueue == Max<ui32>());
        resLeaf.PrevInWaitQueue = quores.QueueTail;
        ResState.Get(quores.QueueTail).NextInWaitQueue = resLeafIdx;
        quores.QueueTail = resLeafIdx;
    }

    resLeaf.NextResourceLeaf = request.ResourceLeaf;
    request.ResourceLeaf = resLeafIdx;

    return EInitLeafStatus::Wait;
}

void TQuoterService::InitialRequestProcessing(TEvQuota::TEvRequest::TPtr &ev, const TRequestId reqIdx) {
    TryTickSchedule();

    TEvQuota::TEvRequest *msg = ev->Get();
    TRequest &request = ReqState.Get(reqIdx);

    request.Operator = msg->Operator;
    request.Deadline = TInstant::Max();
    Y_ABORT_UNLESS(request.Operator == EResourceOperator::And); // todo: support other modes

    Y_ABORT_UNLESS(msg->Reqs.size() >= 1);
    bool canAllow = true;
    for (const auto &leaf : msg->Reqs) {
        LWTRACK(RequestResource, request.Orbit, leaf.Amount, leaf.Quoter, leaf.Resource, leaf.QuoterId, leaf.ResourceId);
        const EInitLeafStatus initLeafStatus =
            (leaf.QuoterId == TEvQuota::TResourceLeaf::QuoterSystem) ?
            InitSystemLeaf(leaf, request, reqIdx) :
            InitResourceLeaf(leaf, request, reqIdx);

        switch (initLeafStatus) {
        case EInitLeafStatus::Unknown:
            return DeclineRequest(request, reqIdx);
        case EInitLeafStatus::GenericError:
            return FailRequest(request, reqIdx);
        case EInitLeafStatus::Forbid:
            return DeadlineRequest(request, reqIdx);
        case EInitLeafStatus::Charged:
            break;
        case EInitLeafStatus::Wait:
            canAllow = false;
            break;
        default:
            Y_ABORT("unkown initLeafStatus");
        }
    }

    if (canAllow) {
        Y_ABORT_UNLESS(request.ResourceLeaf == Max<ui32>());
        return AllowRequest(request, reqIdx);
    }

    if (msg->Deadline != TDuration::Max()) {
        const TDuration delay = Min(TDuration::Days(1), msg->Deadline);
        const TInstant now = TActivationContext::Now();
        TryTickSchedule(now);
        request.Deadline = TimeToGranularity(now + delay);

        auto deadlineIt = ScheduleDeadline.find(request.Deadline);
        if (deadlineIt == ScheduleDeadline.end()) {
            TInstant deadline = request.Deadline; // allocate could invalidate request&
            deadlineIt = ScheduleDeadline.emplace(deadline, ReqState.Allocate(TActorId(0, "placeholder"), 0)).first;
        }

        const TRequestId placeholderIdx = deadlineIt->second;
        TRequest &placeholder = ReqState.Get(placeholderIdx);
        TRequest &reqq = ReqState.Get(reqIdx);

        if (placeholder.NextDeadlineRequest != Max<ui32>()) {
            reqq.NextDeadlineRequest = placeholder.NextDeadlineRequest;
            ReqState.Get(placeholder.NextDeadlineRequest).PrevDeadlineRequest = reqIdx;
        }
        reqq.PrevDeadlineRequest = placeholderIdx;
        placeholder.NextDeadlineRequest = reqIdx;
    }
}

void TQuoterService::Handle(NMon::TEvHttpInfo::TPtr &ev) {
    const auto& httpRequest = ev->Get()->Request;
    if (auto quoterStr = httpRequest.GetParams().Get("quoter"); quoterStr) {
        ui64 quoterId = std::numeric_limits<ui64>::max();
        if (!TryFromString<ui64>(quoterStr, quoterId) || Quoters.find(quoterId) == Quoters.end()) {
            TStringStream str;
            str << NMonitoring::HTTPNOTFOUND;
            Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return;
        }
        Send(ev->Forward(Quoters.find(quoterId)->second.ProxyId));
        return;
    }
    TStringStream str;
    str << NMonitoring::HTTPOKHTML;
    HTML(str) {
        HEAD() {
            str << "<link rel='stylesheet' href='../cms/ext/bootstrap.min.css'>" << Endl
                << "<script language='javascript' type='text/javascript' src='../cms/ext/jquery.min.js'></script>" << Endl
                << "<script language='javascript' type='text/javascript' src='../cms/ext/bootstrap.bundle.min.js'></script>" << Endl;
        }

        DIV() {
            OL_CLASS("breadcrumb") {
                LI_CLASS("breadcrumb-item") {
                    str << "<a href='..' id='host-ref'>YDB Developer UI</a>" << Endl;
                }
                LI_CLASS("breadcrumb-item") {
                    str << "<a href='.'>Actors</a>" << Endl;
                }
                LI_CLASS("breadcrumb-item active") {
                    str << "QuoterService" << Endl;
                }
            }
        }
        DIV_CLASS("container") {
            str << "<a class='collapse-ref' data-toggle='collapse' data-target='#quoter-state'>"
                << "Quoter State</a><div id='quoter-state' class='collapse'>";
            PRE() {
                str << "LastProcessed: " << LastProcessed << "\n"
                    << "Quoters:\n";
                for (auto& [quoterId, quoterState] : Quoters) {
                    str << "  Name: " << quoterState.QuoterName << "\n"
                        << "  Id: " << quoterId << "\n"
                        << "  ProxyId: " << quoterState.ProxyId << "\n"
                        << "  Resources:\n";
                    for (auto& [resId, resPtr] : quoterState.Resources) {
                        str << "    Id: " << resId << "\n";
                        if (resPtr) {
                            str << "    Name: " << resPtr->Resource << "\n"
                                << "    Activation: " << resPtr->Activation << "\n"
                                << "    NextTick: " << resPtr->NextTick << "\n"
                                << "    LastTick: " << resPtr->LastTick << "\n"
                                << "    QueueSize: " << resPtr->QueueSize << "\n"
                                << "    LastAllocated: " << resPtr->LastAllocated << "\n"
                                << "    FreeBalance: " << resPtr->FreeBalance << "\n"
                                << "    Balance: " << resPtr->Balance << "\n"
                                << "    TickRate: " << resPtr->TickRate << "\n"
                                << "    TickSize: " << resPtr->TickSize << "\n"
                                << "    QuotaChannels:\n";
                            for (auto& [chId, ch] : resPtr->QuotaChannels) {
                                str << "      Id: " << chId << "\n"
                                    << "      Channel: " << ch.Channel << "\n"
                                    << "      Ticks: " << ch.Ticks << "\n"
                                    << "      Rate: " << ch.Rate << "\n"
                                    << "      Policy: " << (ui32)ch.Policy << "\n";
                            }
                            str << "    AmountConsumed: " << resPtr->AmountConsumed << "\n"
                                << "    StartStarvationTime: " << resPtr->StartStarvationTime << "\n";
                        }
                    }
                }
            }
            str << "</div>";
            UL_CLASS("list-group") {
                for (auto& [quoterId, quoterState] : Quoters) {
                    LI_CLASS("list-group-item") {
                        HREF(TStringBuilder{} << "?quoter=" << quoterId) {
                            str << quoterState.QuoterName;
                        }
                    }
                }
            }
        }
    }
    Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
}

void TQuoterService::Handle(TEvQuota::TEvRequest::TPtr &ev) {
    BLOG_T("Request(" << PrintEvent(ev) << ")");

    Counters.RequestsInFly->Inc();
    Counters.Requests->Inc();

    TEvQuota::TEvRequest *msg = ev->Get();
    const TRequestId reqIdx = ReqState.Allocate(ev->Sender, ev->Cookie);
    TRequest &request = ReqState.Get(reqIdx);
    LWTRACK(StartRequest, request.Orbit, msg->Operator, msg->Deadline, ev->Cookie);

    if (msg->Reqs.empty()) // request nothing? most probably is error so decline
        return DeclineRequest(request, reqIdx);

    // dirty processing of simple embedded resources
    if (msg->Reqs.size() == 1) {
        const TEvQuota::TResourceLeaf &leaf = msg->Reqs[0];
        switch (msg->Operator) {
        case EResourceOperator::And:
        // only one case supported right now
        {
            if (leaf.QuoterId == TEvQuota::TResourceLeaf::QuoterSystem) {
                switch (leaf.ResourceId) {
                case TEvQuota::TResourceLeaf::ResourceForbid:
                    LWTRACK(RequestResource, request.Orbit, leaf.Amount, leaf.Quoter, leaf.Resource, leaf.QuoterId, leaf.ResourceId);
                    return DeadlineRequest(request, reqIdx);
                case TEvQuota::TResourceLeaf::ResourceNocheck:
                    LWTRACK(RequestResource, request.Orbit, leaf.Amount, leaf.Quoter, leaf.Resource, leaf.QuoterId, leaf.ResourceId);
                    return AllowRequest(request, reqIdx);
                }
            }
        }
        break;
        // not supported yet modes
        default:
            LWTRACK(RequestResource, request.Orbit, leaf.Amount, leaf.Quoter, leaf.Resource, leaf.QuoterId, leaf.ResourceId);
            return DeclineRequest(request, reqIdx);
        }
    }
    // ok, simple processing failed, make full processing
    InitialRequestProcessing(ev, reqIdx);
}

void TQuoterService::Handle(TEvQuota::TEvCancelRequest::TPtr &ev) {
    const ui64 cookie = ev->Cookie;

    const TRequestId headByOwner = ReqState.HeadByOwner(ev->Sender);
    if (headByOwner == Max<ui32>())
        return;

    TRequest &headRequest = ReqState.Get(headByOwner);

    TRequestId nextReqIdx = headRequest.NextByOwner;
    while (nextReqIdx != Max<ui32>()) {
        const TRequestId reqIdx = nextReqIdx;
        TRequest &req = ReqState.Get(nextReqIdx);
        nextReqIdx = req.NextByOwner;

        if (cookie == 0 || req.EventCookie == cookie)
            ForgetRequest(req, reqIdx);
    }

    if (cookie == 0 || headRequest.EventCookie == cookie)
        ForgetRequest(headRequest, headByOwner);
}

void TQuoterService::Handle(TEvQuota::TEvProxySession::TPtr &ev) {
    TEvQuota::TEvProxySession *msg = ev->Get();

    const ui64 quoterId = msg->QuoterId;
    const TString &resourceName = msg->Resource;

    auto quoterIt = Quoters.find(quoterId);
    if (quoterIt == Quoters.end())
        return;

    TQuoterState &quoter = quoterIt->second;
    if (quoter.ProxyId != ev->Sender)
        return;

    auto resIt = quoter.WaitingResource.find(resourceName);
    Y_ABORT_UNLESS(resIt != quoter.WaitingResource.end());

    TSet<TRequestId> waitingRequests = std::move(resIt->second);
    quoter.WaitingResource.erase(resIt);

    const bool isError = msg->Result != msg->Success;
    if (isError) {
        BLOG_I("resource sesson failed: " << quoter.QuoterName << ":" << resourceName);

        for (TRequestId reqIdx : waitingRequests) {
            if (msg->Result == TEvQuota::TEvProxySession::UnknownResource) {
                DeclineRequest(ReqState.Get(reqIdx), reqIdx);
            } else {
                FailRequest(ReqState.Get(reqIdx), reqIdx);
            }
        }

        return;
    }

    const ui64 resourceId = msg->ResourceId;

    BLOG_I("resource session established: " << quoter.QuoterName << ":" << resourceName << " as " << resourceId);

    // success, create resource
    auto resPairIt = quoter.Resources.emplace(resourceId, new TResource(quoterId, resourceId, quoter.QuoterName, resourceName, Config, quoter.Counters.QuoterCounters));
    Y_ABORT_UNLESS(resPairIt.second, "must be no duplicating resources");
    quoter.ResourcesIndex.emplace(resourceName, resourceId);

    Counters.ActiveProxyResources->Inc();

    TResource &quores = *resPairIt.first->second;
    quores.TickSize = msg->TickSize;
    quores.StatUpdatePolicy = msg->StatUpdatePolicy;
    quores.LastAllocated = TInstant::Zero();

    // move requests to 'wait resource' state
    for (TRequestId reqId : waitingRequests) {
        TRequest &req = ReqState.Get(reqId);
        TResourceLeafId resIdx = req.ResourceLeaf;
        while (resIdx != TResourceLeafId{}) {
            TResourceLeaf &leaf = ResState.Get(resIdx);
            Y_ABORT_UNLESS(leaf.RequestIdx == reqId);
            if (leaf.State == EResourceState::ResolveResource
                    && leaf.QuoterId == quoterId
                    && leaf.ResourceName == resourceName)
            {
                leaf.Resource = &quores;
                leaf.State = EResourceState::Wait;
                leaf.ResourceId = resourceId;

                quores.QueueSize += 1;
                quores.QueueWeight += leaf.Amount;
                quores.Counters.Requested->Add(leaf.Amount);

                if (quores.QueueTail == Max<ui32>()) {
                    quores.QueueTail = resIdx;
                    quores.QueueHead = resIdx;
                } else {
                    Y_DEBUG_ABORT_UNLESS(ResState.Get(quores.QueueTail).NextInWaitQueue == TResourceLeafId{});
                    leaf.PrevInWaitQueue = quores.QueueTail;
                    ResState.Get(quores.QueueTail).NextInWaitQueue = resIdx;
                    quores.QueueTail = resIdx;
                }
            }
            // initial charge would be in first session update
            resIdx = leaf.NextResourceLeaf;
        }
    }

    switch (quores.StatUpdatePolicy) {
    case EStatUpdatePolicy::OnActivity:
        if (quores.QueueSize > 0)
            FillStats(quores);
        break;
    case EStatUpdatePolicy::Never:
    case EStatUpdatePolicy::EveryTick:
    case EStatUpdatePolicy::EveryActiveTick:
        break;
    default:
        Y_ABORT("not implemented");
    }
}

void TQuoterService::Handle(TEvQuota::TEvProxyUpdate::TPtr &ev) {
    TEvQuota::TEvProxyUpdate *msg = ev->Get();
    const ui64 quoterId = msg->QuoterId;
    auto quoterIt = Quoters.find(quoterId);
    if (quoterIt == Quoters.end())
        return;

    TQuoterState &quoter = quoterIt->second;
    if (quoter.ProxyId != ev->Sender)
        return;

    if (msg->QuoterState == EUpdateState::Broken || (msg->QuoterState == EUpdateState::Evict && quoter.Empty())) {
        BLOG_I("closing quoter on ProxyUpdate " << quoter.QuoterName);
        return BreakQuoter(quoterIt);
    }

    BLOG_D("ProxyUpdate for quoter " << quoter.QuoterName);

    for (auto &resUpdate : msg->Resources) {
        auto resourceIt = quoter.Resources.find(resUpdate.ResourceId);
        if (resourceIt == quoter.Resources.end())
            continue;

        TResource &quores = *resourceIt->second;

        if (resUpdate.ResourceState == EUpdateState::Broken
            || (resUpdate.ResourceState == EUpdateState::Evict && quores.QueueHead == Max<ui32>()))
        {
            BLOG_I("closing resource on ProxyUpdate " << quoter.QuoterName << ":" << quores.Resource);
            Send(quoter.ProxyId, new TEvQuota::TEvProxyCloseSession(quores.Resource, quores.ResourceId));

            ForbidResource(quores);
            quoter.ResourcesIndex.erase(quores.Resource);
            quoter.Resources.erase(resourceIt);
            continue;
        }

        for (auto &update : resUpdate.Update) {
            if (update.Ticks == 0) {
                quores.QuotaChannels.erase(update.Channel);
            } else {
                Y_ABORT_UNLESS(update.Rate >= 0.0);
                quores.QuotaChannels[update.Channel] = update;
            }
        }

        if (quores.NextTick == TInstant::Zero()) {
            FeedResource(quores);
            TryTickSchedule();
        }
    }

    if (quoter.Empty()) {
        BLOG_I("closing quoter on ProxyUpdate as no activity left " << quoter.QuoterName);
        return BreakQuoter(quoterIt);
    }
}

void TQuoterService::Handle(TEvQuota::TEvRpcTimeout::TPtr &ev) {
    Y_UNUSED(ev);
    Counters.ResultRpcDeadline->Inc();
}

void TQuoterService::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev) {
    THolder<NSchemeCache::TSchemeCacheNavigate> navigate(ev->Get()->Request.Release());
    Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);

    auto &navEntry = navigate->ResultSet.front();
    const TString &path = CanonizePath(navEntry.Path);

    auto quotersIndexIt = QuotersIndex.find(path);
    if (quotersIndexIt == QuotersIndex.end())
        return;

    auto quoterIt = Quoters.find(quotersIndexIt->second);
    Y_ABORT_UNLESS(quoterIt != Quoters.end());
    if (quoterIt->second.ProxyId)
        return;

    switch (navEntry.Kind) {
    case NSchemeCache::TSchemeCacheNavigate::KindKesus:
        BLOG_I("path resolved as Kesus " << path);
        return CreateKesusQuoter(navEntry, quotersIndexIt, quoterIt);
    default:
        BLOG_I("path not resolved as known entity " << path);
        return BreakQuoter(quotersIndexIt, quoterIt);
    }
}

void TQuoterService::CreateKesusQuoter(NSchemeCache::TSchemeCacheNavigate::TEntry &navigate, decltype(QuotersIndex)::iterator indexIt, decltype(Quoters)::iterator quoterIt) {
    // todo: create quoter
    TQuoterState &quoter = quoterIt->second;
    const ui64 quoterId = quoterIt->first;

    if (quoter.ProxyId) {
        return BreakQuoter(indexIt, quoterIt);
    }

    quoter.ProxyId = Register(CreateKesusQuoterProxy(quoterId, navigate, SelfId()), TMailboxType::HTSwap, AppData()->UserPoolId);

    TSet<TRequestId> waitingQueueResolve(std::move(quoter.WaitingQueueResolve));
    for (TRequestId reqIdx : waitingQueueResolve) {
        TRequest &req = ReqState.Get(reqIdx);
        for (TResourceLeafId resLeafIdx = req.ResourceLeaf; resLeafIdx != Max<ui32>(); ) {
            TResourceLeaf &leaf = ResState.Get(resLeafIdx);
            if (leaf.QuoterId == quoterId) {
                Y_ABORT_UNLESS(leaf.State == EResourceState::ResolveQuoter);
                Y_ABORT_UNLESS(leaf.ResourceName);

                auto itpair = quoter.WaitingResource.emplace(leaf.ResourceName, TSet<TRequestId>());
                itpair.first->second.emplace(reqIdx);

                if (itpair.second) { // new resolve entry, request
                    BLOG_I("resolve resource " << leaf.ResourceName << " on quoter " << quoter.QuoterName);
                    Send(quoter.ProxyId, new TEvQuota::TEvProxyRequest(leaf.ResourceName));
                }

                leaf.State = EResourceState::ResolveResource;
            }
            resLeafIdx = leaf.NextResourceLeaf;
        }
    }
}

void TQuoterService::BreakQuoter(decltype(Quoters)::iterator quoterIt) {
    return BreakQuoter(QuotersIndex.find(quoterIt->second.QuoterName), quoterIt);
}

void TQuoterService::BreakQuoter(decltype(QuotersIndex)::iterator indexIt, decltype(Quoters)::iterator quoterIt) {
    // quoter is broken, fail everything and cleanup
    TQuoterState &quoter = quoterIt->second;

    if (quoter.ProxyId) {
        Send(quoter.ProxyId, new TEvents::TEvPoisonPill());
        quoter.ProxyId = TActorId();
    }

    TSet<TRequestId> waitingQueueResolve(std::move(quoter.WaitingQueueResolve));
    for (TRequestId reqIdx : waitingQueueResolve) {
        DeclineRequest(ReqState.Get(reqIdx), reqIdx);
    }

    TMap<TString, TSet<TRequestId>> waitingResource(std::move(quoter.WaitingResource));
    for (auto &xpair : waitingResource) {
        for (TRequestId reqIdx : xpair.second)
            DeclineRequest(ReqState.Get(reqIdx), reqIdx);
    }

    for (auto &xpair : quoter.Resources) {
        ForbidResource(*xpair.second);
    }

    Quoters.erase(quoterIt);
    QuotersIndex.erase(indexIt);

    Counters.ActiveQuoterProxies->Dec();
}

void TQuoterService::ForbidResource(TResource &quores) {
    while (quores.QueueHead != Max<ui32>()) {
        const TRequestId reqIdx = ResState.Get(quores.QueueHead).RequestIdx;
        DeclineRequest(ReqState.Get(reqIdx), reqIdx);
    }

    if (quores.Activation != TInstant::Zero()) {
        ScheduleAllocation[quores.Activation].erase(&quores);
        quores.Activation = TInstant::Zero();
    }

    if (quores.NextTick != TInstant::Zero()) {
        ScheduleFeed[quores.NextTick].erase(&quores);
        quores.NextTick = TInstant::Zero();
    }

    Counters.ActiveProxyResources->Dec();

    // cleanup is outside
}

void TQuoterService::CheckRequest(TRequestId reqIdx) {
    TRequest &request = ReqState.Get(reqIdx);

    for (TResourceLeafId nextLeaf = request.ResourceLeaf; nextLeaf != Max<ui32>(); ) {
        auto &leaf = ResState.Get(nextLeaf);
        if (leaf.State != EResourceState::Cleared)
            return;
        nextLeaf = leaf.NextResourceLeaf;
    }

    // ok, no uncleared resources, process request
    AllowRequest(request, reqIdx);
}

void TQuoterService::FillStats(TResource &quores) {
    auto &dq = StatsToPublish[quores.QuoterId];
    const double expectedRate = -1.0;
    const double cap = -1.0;
    dq.emplace_back(quores.ResourceId, 0, quores.AmountConsumed, quores.History, quores.QueueSize, quores.QueueWeight, expectedRate, cap);
    quores.AmountConsumed = 0.0;
    quores.History.Clear();
}

void TQuoterService::FeedResource(TResource &quores) {
    quores.Balance = 0.0;
    quores.FreeBalance = 0.0;
    quores.TickRate = 0.0;

    for (auto it = quores.QuotaChannels.begin(), end = quores.QuotaChannels.end(); it != end;) {
        auto &quota = it->second;

        switch (quota.Policy) {
        case TEvQuota::ETickPolicy::Front:
            quores.Balance += quota.Rate;
            quores.FreeBalance += quota.Rate;
            break;
        case TEvQuota::ETickPolicy::Sustained:
        case TEvQuota::ETickPolicy::Ahead:
            quores.Balance += quota.Rate;
            break;
        }

        quores.TickRate += quota.Rate;

        if (quota.Ticks == 1) {
            it = quores.QuotaChannels.erase(it);
        } else {
            if (quota.Ticks != Max<ui32>()) // Max<ui32> means forever
                --quota.Ticks;
            ++it;
        }
    }

    BLOG_T("Feed resource \"" << quores.Resource << "\". Balance: " << quores.Balance << ". FreeBalance: " << quores.FreeBalance);
    LWPROBE(FeedResource,
            quores.Quoter,
            quores.Resource,
            quores.QuoterId,
            quores.ResourceId,
            quores.Balance,
            quores.FreeBalance);

    if (quores.QueueTail == Max<ui32>()) {
        quores.NextTick = TInstant::Zero();
    } else {
        // must recheck resource allocation
        ScheduleNextTick(quores.NextTick ? quores.NextTick + quores.TickSize : TActivationContext::Now(), quores);
        AllocateResource(quores);
    }

    switch (quores.StatUpdatePolicy) {
    case EStatUpdatePolicy::EveryTick:
        FillStats(quores);
        break;
    case EStatUpdatePolicy::EveryActiveTick:
        if (quores.QueueSize || quores.AmountConsumed > 0)
            FillStats(quores);
        break;
    case EStatUpdatePolicy::OnActivity:
        if (quores.AmountConsumed > 0)
            FillStats(quores);
        break;
    default:
        break;
    }
}

void TQuoterService::AllocateResource(TResource &quores) {
    BLOG_T("Allocate resource \"" << quores.Resource << "\"");
    const TInstant now = TActivationContext::Now();
    ui64 requestsProcessed = 0;
    const double prevAmountConsumed = quores.AmountConsumed;
    while (quores.QueueHead != Max<ui32>()) {
        TResourceLeaf &leaf = ResState.Get(quores.QueueHead);
        TDuration delay = quores.Charge(ReqState.Get(leaf.RequestIdx), leaf, now);

        if (delay == TDuration::Zero()) {
            // resource available and charged
            // detach from Resource request queue
            Y_ABORT_UNLESS(leaf.PrevInWaitQueue == Max<ui32>());
            quores.QueueHead = leaf.NextInWaitQueue;

            if (quores.QueueHead != Max<ui32>()) {
                TResourceLeaf &nextLeaf = ResState.Get(quores.QueueHead);
                nextLeaf.PrevInWaitQueue = TResourceLeafId{};

                quores.QueueSize -= 1;
                quores.QueueWeight -= leaf.Amount;
            } else {
                // last entry in queue
                quores.QueueTail = TResourceLeafId{};

                quores.QueueSize = 0;
                quores.QueueWeight = 0.0;
            }

            leaf.NextInWaitQueue = TResourceLeafId{};
            leaf.PrevInWaitQueue = TResourceLeafId{};
            leaf.Resource = nullptr;
            leaf.State = EResourceState::Cleared;

            CheckRequest(leaf.RequestIdx);
            ++requestsProcessed;
        } else {
            MarkScheduleAllocation(quores, delay, now);
            break;
        }
    }
    LWPROBE(AllocateResource,
            quores.Quoter,
            quores.Resource,
            quores.QuoterId,
            quores.ResourceId,
            requestsProcessed,
            quores.AmountConsumed - prevAmountConsumed,
            quores.QueueSize,
            quores.QueueWeight,
            quores.Balance,
            quores.FreeBalance);
}

void TQuoterService::HandleTick() {
    const TInstant until = TimeToGranularity(TActivationContext::Now());
    while (LastProcessed < until) {
        // process resource allocation
        auto allocIt = ScheduleAllocation.find(LastProcessed);
        if (allocIt != ScheduleAllocation.end()) {
            while (allocIt->second) {
                auto xset = std::move(allocIt->second);
                for (TResource* quores : xset) {
                    quores->Activation = TInstant::Zero();
                    AllocateResource(*quores);
                }
            }
            ScheduleAllocation.erase(allocIt);
        }

        // process resource feeding
        auto feedIt = ScheduleFeed.find(LastProcessed);
        if (feedIt != ScheduleFeed.end()) {
            while (feedIt->second) {
                auto xset = std::move(feedIt->second);
                for (TResource* quores : xset)
                    FeedResource(*quores);
            }

            ScheduleFeed.erase(feedIt);
        }

        // process deadlines
        auto deadlineIt = ScheduleDeadline.find(LastProcessed);
        if (deadlineIt != ScheduleDeadline.end()) {
            TRequest &placeholder = ReqState.Get(deadlineIt->second);
            Y_ABORT_UNLESS(placeholder.Source.NodeId() == 0);
            while (placeholder.NextDeadlineRequest != Max<ui32>()) {
                TRequest &reqToCancel = ReqState.Get(placeholder.NextDeadlineRequest);
                DeadlineRequest(reqToCancel, placeholder.NextDeadlineRequest);
            }

            // TODO: return placeholder request
            ReqState.Free(deadlineIt->second);
            ScheduleDeadline.erase(deadlineIt);
        }

        LastProcessed += Config.ScheduleTickSize;
    }

    if (ScheduleAllocation || ScheduleFeed || ScheduleDeadline) {
        Schedule(Config.ScheduleTickSize, new TEvents::TEvWakeup());
    } else {
        TickScheduled = false;
    }
}

void TQuoterService::PublishStats() {
    for (auto &xpair : StatsToPublish) {
        if (TQuoterState *qs = Quoters.FindPtr(xpair.first)) {
            Send(qs->ProxyId, new TEvQuota::TEvProxyStats(std::move(xpair.second)));
        }
    }
    StatsToPublish.clear();
}

TString TQuoterService::PrintEvent(const TEvQuota::TEvRequest::TPtr& ev) {
    const auto& req = *ev->Get();
    TStringBuilder ret;
    ret << "{ Operator: " << req.Operator
        << " Deadline: ";
    if (req.Deadline == TDuration::Max()) {
        ret << "no";
    } else if (req.Deadline == TDuration::Zero()) {
        ret << "0";
    } else {
        ret << req.Deadline;
    }
    ret << " Cookie: " << ev->Cookie;
    ret << " [";
    for (size_t i = 0; i < req.Reqs.size(); ++i) {
        const auto& leaf = req.Reqs[i];
        if (i > 0) {
            ret << ",";
        }
        ret << " { " << leaf.Amount << ", ";
        if (leaf.Quoter) {
            ret << "\"" << leaf.Quoter << "\":\"" << leaf.Resource << "\"";
        } else {
            ret << leaf.QuoterId << ":" << leaf.ResourceId;
        }
        ret << " }";
    }
    ret << " ] }";
    return std::move(ret);
}

} // namespace NQuoter

IActor* CreateQuoterService(const TQuoterServiceConfig &config) {
    return new NQuoter::TQuoterService(config);
}

} // namespace NKikimr
