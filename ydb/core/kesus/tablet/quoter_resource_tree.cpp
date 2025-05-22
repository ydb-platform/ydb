#include "quoter_resource_tree.h"

#include "probes.h"
#include "quoter_constants.h"

#include <ydb/core/base/path.h>

#include <util/string/builder.h>
#include <util/generic/maybe.h>

#include <algorithm>
#include <cmath>
#include <iterator>
#include <vector>
#include <limits>

LWTRACE_USING(KESUS_QUOTER_PROVIDER);

namespace NKikimr {
namespace NKesus {

TString CanonizeQuoterResourcePath(const TVector<TString>& path) {
    return JoinPath(path); // Like canonic kikimr path, but without first slash
}

TString CanonizeQuoterResourcePath(const TString& path) {
    return CanonizeQuoterResourcePath(SplitPath(path));
}

namespace {

static constexpr double TICKS_PER_SECOND = 10.0; // every 100 ms
static constexpr double RESOURCE_BURST_COEFFICIENT = 0.0;
static constexpr double EPSILON_COEFFICIENT = 0.000001;
static constexpr int64_t ULPS_ACCURACY = 4;
static const TString RESOURCE_COUNTERS_LABEL = "resource";
static const TString ALLOCATED_COUNTER_NAME = "Allocated";
static const TString SESSIONS_COUNTER_NAME = "Sessions";
static const TString ACTIVE_SESSIONS_COUNTER_NAME = "ActiveSessions";
static const TString LIMIT_COUNTER_NAME = "Limit";
static const TString RESOURCE_SUBSCRIPTIONS_COUNTER_NAME = "ResourceSubscriptions";
static const TString UNKNOWN_RESOURCE_SUBSCRIPTIONS_COUNTER_NAME = "UnknownResourceSubscriptions";
static const TString RESOURCE_CONSUMPTION_STARTS_COUNTER_NAME = "ResourceConsumptionStarts";
static const TString RESOURCE_CONSUMPTION_STOPS_COUNTER_NAME = "ResourceConsumptionStops";
static const TString ELAPSED_MICROSEC_ON_RESOURCE_ALLOCATION_COUNTER_NAME = "ElapsedMicrosecOnResourceAllocation";
static const TString TICK_PROCESSOR_TASKS_PROCESSED_COUNTER_NAME = "TickProcessorTasksProcessed";
static const TString ELAPSED_MICROSEC_WHEN_RESOURCE_ACTIVE_COUNTER_NAME = "ElapsedMicrosecWhenResourceActive";

bool ValidResourcePathSymbols[256] = {};

bool MakeValidResourcePathSymbols() {
    char symbols[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-/:#";
    for (size_t i = 0; i < Y_ARRAY_SIZE(symbols) - 1; ++i) {
        ValidResourcePathSymbols[static_cast<unsigned char>(symbols[i])] = true;
    }
    return true;
}

const bool ValidResourcePathSymbolsAreInitialized = MakeValidResourcePathSymbols();

TInstant NextTick(TInstant time, TDuration tickSize) {
    const ui64 timeUs = time.MicroSeconds();
    const ui64 tickUs = tickSize.MicroSeconds();
    const ui64 r = timeUs % tickUs;
    const TInstant next = TInstant::MicroSeconds(timeUs - r + tickUs);
    Y_ASSERT(next > time);
    return next;
}

// Doubles equality comparison
// See details in https://randomascii.wordpress.com/2012/02/25/comparing-floating-point-numbers-2012-edition/
union TDoubleUnion {
    TDoubleUnion(double value)
        : FloatValue(value)
    {
    }

    bool IsNegative() const {
        return IntValue < 0;
    }

    int64_t IntValue;
    double FloatValue;
    static_assert(sizeof(IntValue) == sizeof(FloatValue));
};

bool AlmostEqualUlpsAndAbs(double a, double b, double maxDiff, int64_t maxUlpsDiff) {
    // Check if the numbers are really close -- needed
    // when comparing numbers near zero.
    const double absDiff = std::abs(a - b);
    if (absDiff <= maxDiff)
        return true;

    const TDoubleUnion uA(a);
    const TDoubleUnion uB(b);

    // Different signs means they do not match.
    if (uA.IsNegative() != uB.IsNegative())
        return false;

    // Find the difference in ULPs.
    const int64_t ulpsDiff = std::abs(uA.IntValue - uB.IntValue);
    return ulpsDiff <= maxUlpsDiff;
}

class TRoundRobinListItem {
public:
    TRoundRobinListItem()
        : Prev(this)
        , Next(this)
    {
    }

    void DeleteFromRoundRobinList() {
        Prev->Next = Next;
        Next->Prev = Prev;
        Prev = this;
        Next = this;
    }

    void InsertBeforeInRoundRobinList(TRoundRobinListItem* item) {
        item->Prev = Prev;
        item->Next = this;
        Prev->Next = item;
        Prev = item;
    }

    template <class T>
    T* GetNext() const {
        return static_cast<T*>(Next);
    }

protected:
    TRoundRobinListItem* Prev;
    TRoundRobinListItem* Next;
};

// Child resource or session for Hierarchical DRR algorithm.
class THierarchicalDRRResourceConsumer : public TRoundRobinListItem {
public:
    virtual ~THierarchicalDRRResourceConsumer() = default;

    virtual double AccumulateResource(double amount, TInstant now) = 0; // returns spent amount of resource.

    virtual ui32 GetWeight() const = 0;
};

// Resource in case of hierarchical DRR algorithm.
class THierarchicalDRRQuoterResourceTree : public TQuoterResourceTree, public THierarchicalDRRResourceConsumer {
public:
    using TQuoterResourceTree::TQuoterResourceTree;

    THierarchicalDRRQuoterResourceTree* GetParent() {
        return static_cast<THierarchicalDRRQuoterResourceTree*>(Parent);
    }

    const THierarchicalDRRQuoterResourceTree* GetParent() const {
        return static_cast<const THierarchicalDRRQuoterResourceTree*>(Parent);
    }

    bool ValidateProps(const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage) override;

    void CalcParameters() override;
    void CalcParametersForAccounting();
    void CalcParametersForReplication();

    THolder<TQuoterSession> DoCreateSession(const NActors::TActorId& clientId, ui32 clientVersion) override;

    void ReportConsumed(double consumed, TTickProcessorQueue& queue, TInstant now) override;

    void HtmlDebugInfo(IOutputStream& out) const override {
        if (ReplicationEnabled) {
            out << "TotalConsumed: " << GetTotalConsumed() << "\n";
            out << "Available: " << GetAvailable() << "\n";
            out << "BucketSize: " << GetBucketSize() << "\n";
            out << "ReplicatedBucketPouringFraction: " << GetReplicatedBucketPouringFraction() << "\n";
        }
    }

    void FillSubscribeResult(NKikimrKesus::TEvSubscribeOnResourcesResult::TResourceSubscribeResult& result) const override {
        TQuoterResourceTree::FillSubscribeResult(result);
        result.SetInitialAvailable(GetAvailable());
    }

    double GetReplicatedBucketPouringFraction() const {
        return ActiveChildrenCount ? (double)ActiveV1ChildrenCount / ActiveChildrenCount : 0;
    }

    void AddActiveChild(THierarchicalDRRResourceConsumer* child, ui32 version, TTickProcessorQueue& queue, TInstant now);
    void RemoveActiveChild(THierarchicalDRRResourceConsumer* child, ui32 version);

    double GetBurst() const {
        return Burst;
    }

    bool IsFull() const {
        return FreeResource >= Burst || AlmostEqualUlpsAndAbs(FreeResource, Burst, ResourceFillingEpsilon, ULPS_ACCURACY);
    }

    double AccumulateResource(double amount, TInstant now) override;
    void DoProcess(TTickProcessorQueue& queue, TInstant now) override;

    double GetResourceTickQuantum() const {
        return ResourceTickQuantum;
    }

    double GetResourceFillingEpsilon() const {
        return ResourceFillingEpsilon;
    }

    TDuration GetTickSize() const {
        return TickSize;
    }

    double GetMaxUnitsPerSecond() const {
        return MaxUnitsPerSecond;
    }

    ui32 GetWeight() const override {
        return Weight;
    }

    void ScheduleNextTick(TTickProcessorQueue& queue, TInstant now);

    bool HasActiveChildren() const {
        return CurrentActiveChild != nullptr;
    }

    void DeactivateIfFull(TInstant now);

    void SetResourceCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> resourceCounters) override;

    void SetLimitCounter();

    void RemoveChild(TQuoterResourceTree* child) override;

    TInstant Report(const NActors::TActorId& clientId, ui64 resourceId, TInstant start, TDuration interval, const double* values, size_t size, TTickProcessorQueue& queue, TInstant now);
    void RunAccounting();
    void RunReplication();

    double GetTotalConsumed() const {
        return TotalConsumed;
    }

    double GetAvailable() const {
        return Available;
    }

    double GetBucketSize() const {
        return BucketSize;
    }

    bool GetReplicationEnabled() const {
        return ReplicationEnabled;
    }

    TDuration GetReplicationPeriod() const {
        auto interval = GetEffectiveProps().GetHierarchicalDRRResourceConfig().GetReplicatedBucket().GetReportIntervalMs();
        return TDuration::MilliSeconds(interval ? interval : NKesus::NQuoter::REPLICATION_PERIOD_MS_DEFAULT);
    }

private:
    double MaxUnitsPerSecond = 0.0;
    double PrefetchCoefficient = 0.0;
    double PrefetchWatermark = 0.0;
    ui32 Weight = 1;
    TDuration TickSize;
    ui64 ActiveChildrenWeight = 0;

    double ResourceTickQuantum = 0.0; // incoming quantum

    double Burst = 0.0;

    double ResourceFillingEpsilon = 0.0;
    double FreeResource = 0.0;
    double ImmediatelyFillUpTo = 0.0;

    bool ReplicationEnabled = false;
    double TotalConsumed = 0.0;
    double Available = 0.0;
    double BucketSize = 0.0;

    bool Active = false;
    THierarchicalDRRResourceConsumer* CurrentActiveChild = nullptr;
    size_t ActiveChildrenCount = 0;
    size_t ActiveV1ChildrenCount = 0;

    THolder<TRateAccounting> RateAccounting;
    bool ActiveAccounting = false;
};

THolder<TQuoterResourceTree> CreateResource(ui64 resourceId, ui64 parentId, NActors::TActorId kesus, const IBillSink::TPtr& billSink, const NKikimrKesus::TStreamingQuoterResource& props) {
    Y_ABORT_UNLESS(resourceId != parentId);
    return MakeHolder<THierarchicalDRRQuoterResourceTree>(resourceId, parentId, kesus, billSink, props);
}

// Session in case of hierarchical DRR algorithm.
class THierarchicalDRRQuoterSession : public TQuoterSession, public THierarchicalDRRResourceConsumer {
public:
    THierarchicalDRRQuoterSession(const NActors::TActorId& clientId, ui32 clientVersion, THierarchicalDRRQuoterResourceTree* resource)
        : TQuoterSession(clientId, clientVersion, resource)
    {
    }

    THierarchicalDRRQuoterResourceTree* GetResource() {
        return static_cast<THierarchicalDRRQuoterResourceTree*>(Resource);
    }

    const THierarchicalDRRQuoterResourceTree* GetResource() const {
        return static_cast<const THierarchicalDRRQuoterResourceTree*>(Resource);
    }

    void UpdateConsumptionState(bool consume, double amount, TTickProcessorQueue& queue, TInstant now) override;
    TInstant Account(TInstant start, TDuration interval, const double* values, size_t size, TTickProcessorQueue& queue, TInstant now) override;
    void DoProcess(TTickProcessorQueue& queue, TInstant now) override;

    void ScheduleNextTick(TTickProcessorQueue& queue, TInstant now);

    double AccumulateResource(double amount, TInstant now) override;

    ui32 GetWeight() const override {
        return 1;
    }

    size_t GetLevel() const override {
        return GetResource()->GetLevel() + 1;
    }

    TTickProcessorId GetTickProcessorId() const override {
        return {ClientId, Resource->GetResourceId()};
    }

    void Activate(TTickProcessorQueue& queue, TInstant now) {
        Y_ABORT_UNLESS(!Active);
        LWPROBE(SessionActivate,
                GetResource()->GetQuoterPath(),
                GetResource()->GetPath(),
                ClientId);
        Active = true;
        GetResource()->AddActiveChild(this, ClientVersion, queue, now);
        const ::NMonitoring::TDynamicCounters::TCounterPtr& activeSessions = GetResource()->GetCounters().ActiveSessions;
        if (activeSessions) {
            activeSessions->Inc();
        }
    }

    void Deactivate() {
        Y_ABORT_UNLESS(Active);
        LWPROBE(SessionDeactivate,
                GetResource()->GetQuoterPath(),
                GetResource()->GetPath(),
                ClientId);
        Active = false;
        AmountRequested = 0.0;
        GetResource()->RemoveActiveChild(this, ClientVersion);
        const ::NMonitoring::TDynamicCounters::TCounterPtr& activeSessions = GetResource()->GetCounters().ActiveSessions;
        if (activeSessions) {
            activeSessions->Dec();
        }
    }

    bool IsFull() const {
        const double burst = GetBurst();
        return FreeResource >= burst || AlmostEqualUlpsAndAbs(FreeResource, burst, GetResource()->GetResourceFillingEpsilon(), ULPS_ACCURACY);
    }

    double GetBurst() const {
        return GetResource()->GetBurst();
    }

    void CloseSession(Ydb::StatusIds::StatusCode status, const TString& reason) override;

    void SendAvailableResource();

    void OnPropsChanged() override;

private:
    double FreeResource = 0.0;
    TInstant LastReplication;
};

double THierarchicalDRRQuoterSession::AccumulateResource(double amount, TInstant now) {
    const double newFreeResource = Min(FreeResource + amount, AmountRequested + GetBurst());
    double spent = newFreeResource - FreeResource;
    FreeResource = newFreeResource;
    if (spent < GetResource()->GetResourceFillingEpsilon()) {
        spent = 0.0;
    }

    LWPROBE(SessionAccumulateResource,
            GetResource()->GetQuoterPath(),
            GetResource()->GetPath(),
            ClientId,
            now,
            Active,
            spent);

    if (AmountRequested < GetResource()->GetResourceFillingEpsilon() && IsFull()) {
        Deactivate();
    }

    return spent;
}

void THierarchicalDRRQuoterSession::CloseSession(Ydb::StatusIds::StatusCode status, const TString& reason) {
    TQuoterSession::CloseSession(status, reason);
    if (Active) {
        Deactivate();
    }
}

void THierarchicalDRRQuoterSession::UpdateConsumptionState(bool consume, double amount, TTickProcessorQueue& queue, TInstant now) {
    LWPROBE(SessionUpdateConsumptionState,
            GetResource()->GetQuoterPath(),
            GetResource()->GetPath(),
            ClientId,
            consume,
            amount);
    if (consume) {
        AmountRequested = Max(amount, 2.0 * GetResource()->GetResourceFillingEpsilon());
        if (!Active) {
            Activate(queue, now);
            ScheduleNextTick(queue, now);
        }
        SendAvailableResource();
    } else {
        AmountRequested = 0.0;
        const bool full = IsFull();
        if (Active && full) {
            Deactivate();
        } else if (!Active && !full) {
            Activate(queue, now);
            ScheduleNextTick(queue, now);
        }
    }

    if (ClientVersion >= 1 && GetResource()->GetReplicationEnabled()) {
        Sync(GetResource()->GetAvailable());
        ScheduleNextTick(queue, now);
    }
}

TInstant THierarchicalDRRQuoterSession::Account(TInstant start, TDuration interval, const double* values, size_t size, TTickProcessorQueue& queue, TInstant now) {
    return GetResource()->Report(ClientId, GetResource()->GetResourceId(), start, interval, values, size, queue, now);
}

void THierarchicalDRRQuoterSession::SendAvailableResource() {
    if (FreeResource >= GetResource()->GetResourceFillingEpsilon()) {
        if (AmountRequested >= GetResource()->GetResourceFillingEpsilon()) {
            const double spent = Min(AmountRequested, FreeResource);
            Send(spent);
            AmountRequested -= spent;
            FreeResource -= spent;
        }
        if (AmountRequested < GetResource()->GetResourceFillingEpsilon()) {
            AmountRequested = 0.0;
            FreeResource = Min(FreeResource, GetBurst());
            if (IsFull()) {
                Deactivate();
            }
        }
    }
}

void THierarchicalDRRQuoterSession::DoProcess(TTickProcessorQueue& queue, TInstant now) {
    LWPROBE(SessionProcess,
            GetResource()->GetQuoterPath(),
            GetResource()->GetPath(),
            ClientId,
            now,
            Active);
    if (Active) {
        SendAvailableResource();
        if (Active) {
            ScheduleNextTick(queue, now);
        }
    }

    if (ClientVersion >= 1 && GetResource()->GetReplicationEnabled() && (LastReplication + GetResource()->GetReplicationPeriod()) <= now) {
        Sync(GetResource()->GetAvailable());
        LastReplication = now;
        Schedule(queue, NextTick(now + GetResource()->GetReplicationPeriod(), GetResource()->GetTickSize()));
    }
}

void THierarchicalDRRQuoterSession::ScheduleNextTick(TTickProcessorQueue& queue, TInstant now) {
    Schedule(queue, NextTick(now, GetResource()->GetTickSize()));
}

void THierarchicalDRRQuoterSession::OnPropsChanged() {
    FreeResource = Min(FreeResource, AmountRequested + GetBurst());
    TQuoterSession::OnPropsChanged();
}

} // anonymous namespace

TQuoterSession::TQuoterSession(const NActors::TActorId& clientId, ui32 clientVersion, TQuoterResourceTree* resource)
    : Resource(resource)
    , ClientId(clientId)
    , ClientVersion(clientVersion)
{
}

void TQuoterSession::CloseSession(Ydb::StatusIds::StatusCode status, const TString& reason) {
    ResourceSink->CloseSession(GetResource()->GetResourceId(), status, reason);
}

void TQuoterSession::Send(double spent) {
    LWPROBE(SessionSend,
            GetResource()->GetQuoterPath(),
            GetResource()->GetPath(),
            ClientId,
            spent);
    ResourceSink->Send(Resource->GetResourceId(), spent, NeedSendChangedProps ? &GetResource()->GetEffectiveProps() : nullptr);
    NeedSendChangedProps = false;
    TotalSent += spent;
    AddAllocatedCounter(spent);
}

void TQuoterSession::Sync(double available) {
    ResourceSink->Sync(Resource->GetResourceId(), LastReportId, available);
}

void TQuoterSession::AddAllocatedCounter(double spent) {
    TQuoterResourceTree* resource = GetResource();
    Y_ASSERT(resource != nullptr);
    do {
        resource->GetCounters().AddAllocated(spent);
        resource = resource->GetParent();
    } while (resource != nullptr);
}

double TQuoterSession::ReportConsumed(ui32 reportId, double consumed, TTickProcessorQueue& queue, TInstant now) {
    double diff = std::clamp(consumed - TotalConsumed, 0.0, std::numeric_limits<double>::max());
    TotalConsumed = consumed;
    LastReportId = reportId;
    Resource->ReportConsumed(diff, queue, now);
    return diff;
}

TQuoterResourceTree::TQuoterResourceTree(ui64 resourceId, ui64 parentId, NActors::TActorId kesus, const IBillSink::TPtr& billSink, const NKikimrKesus::TStreamingQuoterResource& props)
    : ResourceId(resourceId)
    , ParentId(parentId)
    , Kesus(kesus)
    , BillSink(billSink)
    , Props(props)
    , EffectiveProps(props)
{
}

void TQuoterResourceTree::AddChild(TQuoterResourceTree* child) {
    Y_ABORT_UNLESS(child->Parent == nullptr);
    Children.insert(child);
    child->Parent = this;
}

void TQuoterResourceTree::RemoveChild(TQuoterResourceTree* child) {
    Y_ABORT_UNLESS(child->Parent == this);
    const auto childIt = Children.find(child);
    Y_ABORT_UNLESS(childIt != Children.end());
    Children.erase(childIt);
    child->Parent = nullptr;
}

bool TQuoterResourceTree::Update(const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage) {
    if (!ValidateProps(props, errorMessage)) {
        return false;
    }
    const ui64 id = GetResourceId();
    const TString path = GetPath();
    Props = props;
    Props.SetResourceId(id);
    Props.SetResourcePath(path);
    EffectiveProps = Props;
    CalcParameters();
    return true;
}

bool TQuoterResourceTree::ValidateProps(const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage) {
    Y_UNUSED(props, errorMessage);
    return true;
}

void TQuoterResourceTree::CalcParameters() {
    ResourceLevel = 0;
    if (Parent) {
        ResourceLevel = Parent->ResourceLevel + 1;
    }

    // Recurse into children
    for (TQuoterResourceTree* child : Children) {
        child->CalcParameters();
    }
}

void TQuoterResourceTree::SetResourceCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> resourceCounters) {
    Counters.SetResourceCounters(std::move(resourceCounters));
}

void TQuoterResourceTree::UpdateActiveTime(TInstant now) {
    if (StartActiveTime && Counters.ElapsedMicrosecWhenResourceActive && now > StartActiveTime) {
        const TDuration diff = now - StartActiveTime;
        *Counters.ElapsedMicrosecWhenResourceActive += diff.MicroSeconds();
    }
    StartActiveTime = now;
}

void TQuoterResourceTree::StopActiveTime(TInstant now) {
    UpdateActiveTime(now);
    StartActiveTime = TInstant::Zero();
}

void TQuoterResourceTree::TCounters::SetResourceCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> resourceCounters) {
    ResourceCounters = std::move(resourceCounters);
    if (ResourceCounters) {
        Allocated = ResourceCounters->GetCounter(ALLOCATED_COUNTER_NAME, true);
        Sessions = ResourceCounters->GetExpiringCounter(SESSIONS_COUNTER_NAME, false);
        ActiveSessions = ResourceCounters->GetExpiringCounter(ACTIVE_SESSIONS_COUNTER_NAME, false);
        ElapsedMicrosecWhenResourceActive = ResourceCounters->GetCounter(ELAPSED_MICROSEC_WHEN_RESOURCE_ACTIVE_COUNTER_NAME, true);
    } else {
        Allocated = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        Sessions = MakeIntrusive<NMonitoring::TCounterForPtr>(false);
        ActiveSessions = MakeIntrusive<NMonitoring::TCounterForPtr>(false);
        ElapsedMicrosecWhenResourceActive = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
    }
}

void TQuoterResourceTree::TCounters::AddAllocated(double allocated) {
    if (Allocated) {
        allocated += AllocatedRemainder;
        const double counterIncrease = std::floor(allocated);
        AllocatedRemainder = allocated - counterIncrease;
        Allocated->Add(counterIncrease);
    }
}

void TQuoterResourceTree::TCounters::SetLimit(TMaybe<double> limit) {
    if (ResourceCounters) {
        if (limit) {
            if (!Limit) {
                Limit = ResourceCounters->GetExpiringCounter(LIMIT_COUNTER_NAME, false);
            }
            *Limit = static_cast<i64>(*limit);
        } else {
            Limit = nullptr;
        }
    }
}

bool THierarchicalDRRQuoterResourceTree::ValidateProps(const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage) {
    if (!props.HasHierarchicalDRRResourceConfig()) {
        errorMessage = "No HierarchicalDRRResourceConfig specified.";
        return false;
    }
    const auto& hdrrConfig = props.GetHierarchicalDRRResourceConfig();
    const double maxUnitsPerSecond = hdrrConfig.GetMaxUnitsPerSecond() ?
        hdrrConfig.GetMaxUnitsPerSecond() : hdrrConfig.GetSpeedSettings().GetMaxUnitsPerSecond();
    if (!std::isfinite(maxUnitsPerSecond)) {
        errorMessage = "MaxUnitsPerSecond must be finite.";
        return false;
    }
    if (maxUnitsPerSecond < 0.0) {
        errorMessage = "MaxUnitsPerSecond can't be less than 0.";
        return false;
    }

    // Validate prefetch settings
    const double prefetchCoefficient = hdrrConfig.GetPrefetchCoefficient();
    if (!std::isfinite(prefetchCoefficient)) {
        errorMessage = "PrefetchCoefficient must be finite.";
        return false;
    }
    const double prefetchWatermark = hdrrConfig.GetPrefetchWatermark();
    if (!std::isfinite(prefetchWatermark)) {
        errorMessage = "PrefetchWatermark must be finite.";
        return false;
    }
    if (prefetchWatermark < 0.0) {
        errorMessage = "PrefetchWatermark can't be less than 0.";
        return false;
    }
    if (prefetchWatermark > 1.0) {
        errorMessage = "PrefetchWatermark can't be greater than 1.";
        return false;
    }

    if (!ParentId && !maxUnitsPerSecond) {
        errorMessage = "No MaxUnitsPerSecond parameter in root resource.";
        return false;
    }

    if (!TRateAccounting::ValidateProps(props, errorMessage)) {
        return false;
    }

    return TQuoterResourceTree::ValidateProps(props, errorMessage);
}

void THierarchicalDRRQuoterResourceTree::CalcParameters() {
    // compatibility
    if (!Props.GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond() && Props.GetHierarchicalDRRResourceConfig().GetSpeedSettings().GetMaxUnitsPerSecond()) {
        Props.MutableHierarchicalDRRResourceConfig()->SetMaxUnitsPerSecond(Props.GetHierarchicalDRRResourceConfig().GetSpeedSettings().GetMaxUnitsPerSecond());
    }

    // speed settings
    THierarchicalDRRQuoterResourceTree* const parent = GetParent();
    const auto& config = GetProps().GetHierarchicalDRRResourceConfig();
    if (config.GetMaxUnitsPerSecond()) {
        MaxUnitsPerSecond = config.GetMaxUnitsPerSecond();
    } else if (parent) {
        MaxUnitsPerSecond = parent->MaxUnitsPerSecond;
    }

    if (parent && MaxUnitsPerSecond > parent->MaxUnitsPerSecond) {
        MaxUnitsPerSecond = parent->MaxUnitsPerSecond;
    }

    // prefetch settings
    if (config.GetPrefetchCoefficient()) {
        PrefetchCoefficient = config.GetPrefetchCoefficient();
    } else if (parent) {
        PrefetchCoefficient = parent->PrefetchCoefficient;
    }
    if (config.GetPrefetchWatermark()) {
        PrefetchWatermark = config.GetPrefetchWatermark();
    } else if (parent) {
        PrefetchWatermark = parent->PrefetchWatermark;
    }

    ResourceTickQuantum = MaxUnitsPerSecond >= 0.0 ? MaxUnitsPerSecond / TICKS_PER_SECOND : 0.0;
    ResourceFillingEpsilon = ResourceTickQuantum * EPSILON_COEFFICIENT;
    TickSize = TDuration::Seconds(1) / TICKS_PER_SECOND;

    Burst = ResourceTickQuantum * RESOURCE_BURST_COEFFICIENT;
    if (config.HasImmediatelyFillUpTo()) {
        ImmediatelyFillUpTo = config.GetImmediatelyFillUpTo();
    } else {
        ImmediatelyFillUpTo = 0.0;
    }

    const ui32 oldWeight = Weight;
    Weight = config.GetWeight() ? config.GetWeight() : 1;
    const i64 weightDiff = static_cast<i64>(Weight) - static_cast<i64>(oldWeight);
    if (Active && parent && weightDiff) {
        parent->ActiveChildrenWeight += weightDiff;
    }

    FreeResource = Min(FreeResource, HasActiveChildren() ? ResourceTickQuantum : GetBurst());

    // Update in props
    auto* effectiveConfig = EffectiveProps.MutableHierarchicalDRRResourceConfig();
    effectiveConfig->SetMaxUnitsPerSecond(MaxUnitsPerSecond);
    effectiveConfig->SetWeight(Weight);
    effectiveConfig->SetMaxBurstSizeCoefficient(1);
    effectiveConfig->SetPrefetchCoefficient(PrefetchCoefficient);
    effectiveConfig->SetPrefetchWatermark(PrefetchWatermark);
    if (effectiveConfig->HasImmediatelyFillUpTo()) {
        effectiveConfig->SetImmediatelyFillUpTo(ImmediatelyFillUpTo);
    }

    SetLimitCounter();

    CalcParametersForAccounting();

    CalcParametersForReplication();

    TQuoterResourceTree::CalcParameters(); // recalc for children
}

void THierarchicalDRRQuoterResourceTree::CalcParametersForAccounting() {
    const auto* accCfgParent = Parent ? &Parent->GetEffectiveProps().GetAccountingConfig() : nullptr;
    auto* accCfg = EffectiveProps.MutableAccountingConfig();

    // Calc rate accouting effective props
    if (!accCfg->GetReportPeriodMs()) {
        accCfg->SetReportPeriodMs(accCfgParent ? accCfgParent->GetReportPeriodMs() : 5000);
    }

    if (!accCfg->GetAccountPeriodMs()) {
        accCfg->SetAccountPeriodMs(accCfgParent ? accCfgParent->GetAccountPeriodMs() : 1000);
    }

    if (!accCfg->GetCollectPeriodSec()) {
        accCfg->SetCollectPeriodSec(accCfgParent ? accCfgParent->GetCollectPeriodSec() : 30);
    }

    if (!accCfg->GetProvisionedCoefficient()) {
        accCfg->SetProvisionedCoefficient(accCfgParent ? accCfgParent->GetProvisionedCoefficient() : 60);
    }

    if (!accCfg->GetOvershootCoefficient()) {
        accCfg->SetOvershootCoefficient(accCfgParent ? accCfgParent->GetOvershootCoefficient() : 1.1);
    }

    auto calcMetricsParams = [] (auto* cfg, const auto* parent) {
        // NOTE: `Enabled` is not inherited, skipped here
        if (!cfg->GetBillingPeriodSec()) {
            cfg->SetBillingPeriodSec(parent ? parent->GetBillingPeriodSec() : 60);
        }
        if (!cfg->GetVersion() && parent) { cfg->SetVersion(parent->GetVersion()); }
        if (!cfg->GetSchema() && parent) { cfg->SetSchema(parent->GetSchema()); }
        if (!cfg->GetCloudId() && parent) { cfg->SetCloudId(parent->GetCloudId()); }
        if (!cfg->GetFolderId() && parent) { cfg->SetFolderId(parent->GetFolderId()); }
        if (!cfg->GetResourceId() && parent) { cfg->SetResourceId(parent->GetResourceId()); }
        if (!cfg->GetSourceId() && parent) { cfg->SetSourceId(parent->GetSourceId()); }
        if (cfg->GetTags().empty() && parent) { *cfg->MutableTags() = parent->GetTags(); }
        if (cfg->GetLabels().empty() && parent) { *cfg->MutableLabels() = parent->GetLabels(); }
    };
    calcMetricsParams(accCfg->MutableProvisioned(), accCfgParent ? &accCfgParent->GetProvisioned() : nullptr);
    calcMetricsParams(accCfg->MutableOnDemand(), accCfgParent ? &accCfgParent->GetOnDemand() : nullptr);
    calcMetricsParams(accCfg->MutableOvershoot(), accCfgParent ? &accCfgParent->GetOvershoot() : nullptr);

    // Create/update/delete rate accounting
    if (accCfg->GetEnabled()) {
        if (!RateAccounting) { // create
            RateAccounting.Reset(new TRateAccounting(Kesus, BillSink, EffectiveProps, QuoterPath));
            RateAccounting->SetResourceCounters(Counters.ResourceCounters);
        } else { // update
            RateAccounting->Configure(EffectiveProps);
        }
    } else if (RateAccounting) { // delete
        RateAccounting->Stop();
        RateAccounting.Destroy();
    }
}

void THierarchicalDRRQuoterResourceTree::CalcParametersForReplication() {
    // Create/update/delete replication
    if (EffectiveProps.GetHierarchicalDRRResourceConfig().HasReplicatedBucket()) {
        if (!EffectiveProps.GetHierarchicalDRRResourceConfig().GetReplicatedBucket().HasReportIntervalMs()) {
            EffectiveProps.MutableHierarchicalDRRResourceConfig()->MutableReplicatedBucket()->SetReportIntervalMs(5000);
        }
        ReplicationEnabled = true;
        const auto& cfg = EffectiveProps.GetHierarchicalDRRResourceConfig();
        const double speed = cfg.GetMaxUnitsPerSecond();
        const double prefetch = cfg.GetPrefetchCoefficient() ? cfg.GetPrefetchCoefficient() : NQuoter::PREFETCH_COEFFICIENT_DEFAULT;
        BucketSize = Max(0.0, speed * prefetch) + 1;
        // Available can be equal exactly zero only at init. It's almost impossible to have exactly zero in other cases
        // So threat this state as initial and fill up bucket.
        // It is better than always filling full quota because it  will help us avoid abuse with continuous quota change to refill
        Available = Available == 0.0 ? BucketSize : Min(Available, BucketSize); // TODO: add explicit behavior for alter

        for (auto& [_, session] : Sessions) {
            session->ResetTotalConsumed();
        }
    } else {
        ReplicationEnabled = false;
        BucketSize = 0.0;
        Available = 0.0;
        TotalConsumed = 0.0;
    }
}

void THierarchicalDRRQuoterResourceTree::RemoveChild(TQuoterResourceTree* childBase) {
    THierarchicalDRRQuoterResourceTree* child = static_cast<THierarchicalDRRQuoterResourceTree*>(childBase);
    if (child->Active) {
        child->Active = false;
        RemoveActiveChild(child, 1);
    }
    TQuoterResourceTree::RemoveChild(childBase);
}

void THierarchicalDRRQuoterResourceTree::DeactivateIfFull(TInstant now) {
    if (!HasActiveChildren() && IsFull()) {
        Active = false;
        LWPROBE(ResourceDeactivate,
                QuoterPath,
                GetPath());
        StopActiveTime(now);
        if (GetParent()) {
            GetParent()->RemoveActiveChild(this, 1);
        }
    }
}

double THierarchicalDRRQuoterResourceTree::AccumulateResource(double amount, TInstant now) {
    amount = Min(amount, ResourceTickQuantum);
    const double newFreeResource = Min(FreeResource + amount, HasActiveChildren() ? ResourceTickQuantum : GetBurst());
    double spent = newFreeResource - FreeResource;
    FreeResource = newFreeResource;
    if (spent < ResourceFillingEpsilon) {
        spent = 0.0;
    }

    LWPROBE(ResourceAccumulateResource,
            QuoterPath,
            GetPath(),
            now,
            Active,
            spent);

    DeactivateIfFull(now);
    return spent;
}

void THierarchicalDRRQuoterResourceTree::DoProcess(TTickProcessorQueue& queue, TInstant now) {
    LWPROBE(ResourceProcess,
            QuoterPath,
            GetPath(),
            now,
            Active,
            ActiveChildrenCount);
    if (Active) {
        if (Parent == nullptr) { // Root resource
            AccumulateResource(ResourceTickQuantum, now);
        }

        UpdateActiveTime(now);
        if (HasActiveChildren()) {
            const ui64 sumWeights = ActiveChildrenWeight;
            const double quantum = Max(FreeResource / static_cast<double>(sumWeights), ResourceFillingEpsilon);
            const size_t activeChildrenCount = ActiveChildrenCount; // This count will be nonincreasing during cycle.
            size_t childrenProcessed = 0;
            double freeResourceBeforeCycle = FreeResource;
            while (FreeResource >= ResourceFillingEpsilon && HasActiveChildren()) {
                THierarchicalDRRResourceConsumer* child = CurrentActiveChild;
                CurrentActiveChild = CurrentActiveChild->GetNext<THierarchicalDRRResourceConsumer>();
                const ui32 weight = child->GetWeight();
                double amount = quantum;
                if (weight != 1) {
                    amount *= static_cast<double>(weight);
                }
                const double giveAmount = std::clamp(amount, ResourceFillingEpsilon, FreeResource);
                LWPROBE(ResourceGiveToChild,
                        QuoterPath,
                        GetPath(),
                        now,
                        giveAmount,
                        weight);
                const double spent = child->AccumulateResource(giveAmount, now);
                FreeResource -= spent;

                ++childrenProcessed;
                if (childrenProcessed == activeChildrenCount) { // All children are processed, check whether FreeResource didn't change (so, there was no progress).
                    if (AlmostEqualUlpsAndAbs(FreeResource, freeResourceBeforeCycle, ResourceFillingEpsilon, ULPS_ACCURACY)) {
                        // Nothing has changed when all sessions/resources were processed. Break cycle.
                        break;
                    }
                    childrenProcessed = 0;
                    freeResourceBeforeCycle = FreeResource;
                }
            }
        }

        DeactivateIfFull(now);
    }

    if (ActiveAccounting) {
        RunAccounting();
    }

    if (ReplicationEnabled) {
        RunReplication();
    }

    if (Active || ActiveAccounting || ReplicationEnabled) {
        ScheduleNextTick(queue, now);
    }
}

TInstant THierarchicalDRRQuoterResourceTree::Report(
    const NActors::TActorId& clientId,
    ui64 resourceId,
    TInstant start,
    TDuration interval,
    const double* values,
    size_t size,
    TTickProcessorQueue& queue,
    TInstant now)
{
    if (RateAccounting) {
        TInstant result = RateAccounting->Report(clientId, resourceId, start, interval, values, size);
        ActiveAccounting = true;
        ScheduleNextTick(queue, now);
        return result;
    } else if (GetParent()) {
        return GetParent()->Report(clientId, resourceId, start, interval, values, size, queue, now);
    } else {
        // We have no rate accounting enabled -- skip data
        return TInstant::Zero();
    }
}

void THierarchicalDRRQuoterResourceTree::RunAccounting() {
    if (RateAccounting) {
        ActiveAccounting = RateAccounting->RunAccounting();
    } else {
        ActiveAccounting = false;
    }
}

void THierarchicalDRRQuoterResourceTree::RunReplication() {
    if (ReplicationEnabled) {
        if (Available < BucketSize) {
            Available = Min(Available + ResourceTickQuantum * GetReplicatedBucketPouringFraction(), BucketSize);
        }
    }
}

void THierarchicalDRRQuoterResourceTree::AddActiveChild(THierarchicalDRRResourceConsumer* child, ui32 version, TTickProcessorQueue& queue, TInstant now) {
    UpdateActiveTime(now);
    if (!HasActiveChildren()) {
        CurrentActiveChild = child;
        ActiveChildrenCount = 1;
        ActiveV1ChildrenCount = version >= 1 ? 1 : 0;

        Active = true;
        LWPROBE(ResourceActivate,
                QuoterPath,
                GetPath());

        ScheduleNextTick(queue, now);
        if (GetParent()) {
            GetParent()->AddActiveChild(this, version, queue, now);
        }

        // Update sum of active children weights
        Y_ASSERT(ActiveChildrenWeight == 0);
        ActiveChildrenWeight = child->GetWeight();

    } else {
        if (child->GetNext<THierarchicalDRRResourceConsumer>() == child && CurrentActiveChild != child) { // Not in list.
            CurrentActiveChild->InsertBeforeInRoundRobinList(child);
            ++ActiveChildrenCount;
            ActiveV1ChildrenCount += version >= 1 ? 1 : 0;

            // Update sum of active children weights
            ActiveChildrenWeight += child->GetWeight();
        }
    }
}

void THierarchicalDRRQuoterResourceTree::RemoveActiveChild(THierarchicalDRRResourceConsumer* child, ui32 version) {
    if (HasActiveChildren()) {
        if (child == CurrentActiveChild) {
            CurrentActiveChild = CurrentActiveChild->GetNext<THierarchicalDRRResourceConsumer>();
        }
        child->DeleteFromRoundRobinList();
        --ActiveChildrenCount;
        if (child == CurrentActiveChild) {
            CurrentActiveChild = nullptr;
            Y_ASSERT(ActiveChildrenCount == 0);
        }

        // Update sum of active children weights
        Y_ASSERT(ActiveChildrenWeight >= child->GetWeight());
        ActiveChildrenWeight -= child->GetWeight();
        ActiveV1ChildrenCount -= version >= 1 ? 1 : 0;

        Y_ASSERT(ActiveChildrenCount > 0 || ActiveChildrenWeight == 0);
    }
}

void THierarchicalDRRQuoterResourceTree::ScheduleNextTick(TTickProcessorQueue& queue, TInstant now) {
    Schedule(queue, NextTick(now, TickSize));
}

void THierarchicalDRRQuoterResourceTree::ReportConsumed(double consumed, TTickProcessorQueue& queue, TInstant now) {
    if (ReplicationEnabled) {
        TotalConsumed += consumed;
        Available -= consumed;
        if (ImmediatelyFillUpTo < 0.0) {
            Available = Max(Available, ImmediatelyFillUpTo);
        }
        ScheduleNextTick(queue, now);
    }
}

THolder<TQuoterSession> THierarchicalDRRQuoterResourceTree::DoCreateSession(const NActors::TActorId& clientId, ui32 clientVersion) {
    return MakeHolder<THierarchicalDRRQuoterSession>(clientId, clientVersion, this);
}

void THierarchicalDRRQuoterResourceTree::SetResourceCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> resourceCounters) {
    TQuoterResourceTree::SetResourceCounters(std::move(resourceCounters));
    if (RateAccounting) {
        RateAccounting->SetResourceCounters(Counters.ResourceCounters);
    }
    SetLimitCounter();
}

void THierarchicalDRRQuoterResourceTree::SetLimitCounter() {
    const double speedLimit = GetProps().GetHierarchicalDRRResourceConfig().GetMaxUnitsPerSecond();
    if (speedLimit) {
        Counters.SetLimit(speedLimit);
    } else {
        Counters.SetLimit(Nothing());
    }
}

bool TQuoterResources::Exists(ui64 resourceId) const {
    return ResourcesById.find(resourceId) != ResourcesById.end();
}

TQuoterResourceTree* TQuoterResources::LoadResource(ui64 resourceId, ui64 parentId, const NKikimrKesus::TStreamingQuoterResource& props) {
    auto resource = CreateResource(resourceId, parentId, Kesus, BillSink, props);
    Y_ABORT_UNLESS(!Exists(resource->GetResourceId()),
         "Resource \"%s\" has duplicated id: %" PRIu64, resource->GetPath().c_str(), resourceId);
    Y_ABORT_UNLESS(!props.GetResourcePath().empty(),
         "Resource %" PRIu64 " has empty path", resourceId);
    TQuoterResourceTree* res = resource.Get();
    ResourcesByPath.emplace(props.GetResourcePath(), resource.Get());
    ResourcesById.emplace(resourceId, std::move(resource));
    SetResourceCounters(res);
    res->SetQuoterPath(QuoterPath);
    return res;
}

TQuoterResourceTree* TQuoterResources::AddResource(ui64 resourceId, const NKikimrKesus::TStreamingQuoterResource& props, TString& errorMessage) {
    // validate
    if (ResourcesById.find(resourceId) != ResourcesById.end()) {
        errorMessage = TStringBuilder() << "Resource with id " << resourceId << " already exists.";
        return nullptr;
    }

    const TVector<TString> path = SplitPath(props.GetResourcePath());
    if (path.empty()) {
        errorMessage = "Empty resource path is specified.";
        return nullptr;
    }
    const TString& canonPath = CanonizeQuoterResourcePath(path);

    if (ResourcesByPath.find(canonPath) != ResourcesByPath.end()) {
        errorMessage = TStringBuilder() << "Resource with path \"" << canonPath << "\" already exists.";
        return nullptr;
    }

    // find parent
    TQuoterResourceTree* parent = nullptr;
    if (path.size() > 1) {
        const TVector<TString> parentPath(path.begin(), path.end() - 1);
        TString canonParentPath = CanonizeQuoterResourcePath(parentPath);
        parent = FindPathImpl(canonParentPath);
        if (!parent) {
            errorMessage = TStringBuilder() << "Parent resource \"" << canonParentPath << "\" doesn't exist.";
            return nullptr;
        }
    }

    // create and finally validate props
    NKikimrKesus::TStreamingQuoterResource resProps = props;
    resProps.SetResourceId(resourceId);
    resProps.SetResourcePath(canonPath);
    const ui64 parentId = parent ? parent->GetResourceId() : 0;
    THolder<TQuoterResourceTree> resource = CreateResource(resourceId, parentId, Kesus, BillSink, resProps);
    if (!resource->ValidateProps(resProps, errorMessage)) {
        return nullptr;
    }

    // insert
    TQuoterResourceTree* resourcePtr = resource.Get();
    if (parent) {
        parent->AddChild(resourcePtr);
    }
    ResourcesByPath[canonPath] = resourcePtr;
    ResourcesById[resourceId] = std::move(resource);
    SetResourceCounters(resourcePtr);
    resourcePtr->SetQuoterPath(QuoterPath);
    resourcePtr->CalcParameters();

    return resourcePtr;
}

bool TQuoterResources::DeleteResource(TQuoterResourceTree* resource, TString& errorMessage) {
    if (!resource->GetChildren().empty()) {
        errorMessage = TStringBuilder() << "Resource \"" << resource->GetPath() << "\" has children.";
        return false;
    }

    if (resource->GetParent()) {
        resource->GetParent()->RemoveChild(resource);
    }

    const auto sessions = resource->GetSessions();
    TStringBuilder closeReason;
    closeReason << "Resource \"" << resource->GetPath() << "\" was deleted.";
    for (const auto& [clientId, _] : sessions) {
        const auto sessionId = TQuoterSessionId{clientId, resource->GetResourceId()};
        const auto sessionIt = Sessions.find(sessionId);
        Y_ABORT_UNLESS(sessionIt != Sessions.end());
        TQuoterSession* session = sessionIt->second.Get();
        session->CloseSession(Ydb::StatusIds::NOT_FOUND, closeReason);
        const NActors::TActorId pipeServerId = session->SetPipeServerId({});
        SetPipeServerId(sessionId, pipeServerId, {}); // Erase pipeServerId from index.
        Sessions.erase(sessionIt);
    }

    const auto resByPathIt = ResourcesByPath.find(resource->GetPath());
    Y_ABORT_UNLESS(resByPathIt != ResourcesByPath.end());
    Y_ABORT_UNLESS(resByPathIt->second == resource);
    ResourcesByPath.erase(resByPathIt);

    const auto resByIdIt = ResourcesById.find(resource->GetResourceId());
    Y_ABORT_UNLESS(resByIdIt != ResourcesById.end());
    Y_ABORT_UNLESS(resByIdIt->second.Get() == resource);
    ResourcesById.erase(resByIdIt);
    return true;
}

void TQuoterResources::SetupBilling(NActors::TActorId kesus, const IBillSink::TPtr& billSink) {
    Kesus = kesus;
    BillSink = billSink;
}

void TQuoterResources::ConstructTrees() {
    // connect with parents
    std::vector<TQuoterResourceTree*> roots;
    for (auto&& [id, resource] : ResourcesById) {
        if (resource->GetParentId()) {
            const auto parent = ResourcesById.find(resource->GetParentId());
            Y_ABORT_UNLESS(parent != ResourcesById.end(),
                 "Parent %" PRIu64 " was not found for resource %" PRIu64 " (\"%s\")",
                     resource->GetParentId(), resource->GetResourceId(), resource->GetPath().c_str());
            parent->second->AddChild(resource.Get());
        } else {
            roots.push_back(resource.Get());
        }
    }
    for (TQuoterResourceTree* root : roots) {
        root->CalcParameters();
    }
}

bool TQuoterResources::IsResourcePathValid(const TString& path) {
    for (const char c : path) {
        if (!ValidResourcePathSymbols[static_cast<unsigned char>(c)]) {
            return false;
        }
    }
    return true;
}

TQuoterResourceTree* TQuoterResources::FindPath(const TString& resourcePath) {
    return FindPathImpl(CanonizeQuoterResourcePath(resourcePath));
}

TQuoterResourceTree* TQuoterResources::FindId(ui64 resourceId) {
    const auto res = ResourcesById.find(resourceId);
    return res != ResourcesById.end() ? res->second.Get() : nullptr;
}

TQuoterResourceTree* TQuoterResources::FindPathImpl(const TString& resourcePath) {
    const auto res = ResourcesByPath.find(resourcePath);
    return res != ResourcesByPath.end() ? res->second : nullptr;
}

void TQuoterResources::ProcessTick(const TTickProcessorTask& task, TTickProcessorQueue& queue) {
    TTickProcessor* processor = nullptr;
    if (task.Processor.first) { // session
        auto sessionIt = Sessions.find(task.Processor);
        if (sessionIt != Sessions.end()) {
            processor = sessionIt->second.Get();
        }
    } else { // resource
        processor = FindId(task.Processor.second);
    }
    if (processor) {
        processor->Process(queue, task.Time);
    }
}

TQuoterSession* TQuoterResources::GetOrCreateSession(const NActors::TActorId& clientId, ui32 clientVersion, TQuoterResourceTree* resource) {
    const ui64 resourceId = resource->GetResourceId();
    if (TQuoterSession* session = FindSession(clientId, resourceId)) {
        return session;
    } else {
        const auto newSessionIt = Sessions.emplace(TQuoterSessionId{clientId, resourceId}, resource->CreateSession(clientId, clientVersion)).first;
        return newSessionIt->second.Get();
    }
}

TQuoterSession* TQuoterResources::FindSession(const NActors::TActorId& clientId, ui64 resourceId) {
    const auto sessionIt = Sessions.find(TQuoterSessionId{clientId, resourceId});
    return sessionIt != Sessions.end() ? sessionIt->second.Get() : nullptr;
}

const TQuoterSession* TQuoterResources::FindSession(const NActors::TActorId& clientId, ui64 resourceId) const {
    const auto sessionIt = Sessions.find(TQuoterSessionId{clientId, resourceId});
    return sessionIt != Sessions.end() ? sessionIt->second.Get() : nullptr;
}

void TQuoterResources::OnUpdateResourceProps(TQuoterResourceTree* rootResource) {
    const ui64 resId = rootResource->GetResourceId();
    for (const auto& [sessionActor, _] : rootResource->GetSessions()) {
        TQuoterSession* session = FindSession(sessionActor, resId);
        Y_ABORT_UNLESS(session);
        session->OnPropsChanged();
    }
    for (TQuoterResourceTree* child : rootResource->GetChildren()) {
        OnUpdateResourceProps(child);
    }
}

void TQuoterResources::EnableDetailedCountersMode(bool enable) {
    Counters.DetailedCountersMode = enable;

    ReinitResourceCounters();
}

void TQuoterResources::SetResourceCounters(TQuoterResourceTree* res) {
    res->SetResourceCounters(
        Counters.QuoterCounters && (Counters.DetailedCountersMode || res->GetParentId() == 0) ?
        Counters.QuoterCounters->GetSubgroup(RESOURCE_COUNTERS_LABEL, res->GetProps().GetResourcePath()) :
        nullptr
    );
}

void TQuoterResources::SetQuoterCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> quoterCounters) {
    Counters.QuoterCounters = std::move(quoterCounters);

    ReinitResourceCounters();
}

void TQuoterResources::ReinitResourceCounters() {
    if (Counters.QuoterCounters) {
        Counters.ResourceSubscriptions = Counters.QuoterCounters->GetCounter(RESOURCE_SUBSCRIPTIONS_COUNTER_NAME, true);
        Counters.UnknownResourceSubscriptions = Counters.QuoterCounters->GetCounter(UNKNOWN_RESOURCE_SUBSCRIPTIONS_COUNTER_NAME, true);
        Counters.ResourceConsumptionStarts = Counters.QuoterCounters->GetCounter(RESOURCE_CONSUMPTION_STARTS_COUNTER_NAME, true);
        Counters.ResourceConsumptionStops = Counters.QuoterCounters->GetCounter(RESOURCE_CONSUMPTION_STOPS_COUNTER_NAME, true);
        Counters.ElapsedMicrosecOnResourceAllocation = Counters.QuoterCounters->GetCounter(ELAPSED_MICROSEC_ON_RESOURCE_ALLOCATION_COUNTER_NAME, true);
        Counters.TickProcessorTasksProcessed = Counters.QuoterCounters->GetCounter(TICK_PROCESSOR_TASKS_PROCESSED_COUNTER_NAME, true);
    } else {
        Counters.ResourceSubscriptions = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        Counters.UnknownResourceSubscriptions = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        Counters.ResourceConsumptionStarts = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        Counters.ResourceConsumptionStops = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        Counters.ElapsedMicrosecOnResourceAllocation = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
        Counters.TickProcessorTasksProcessed = MakeIntrusive<NMonitoring::TCounterForPtr>(true);
    }

    for (auto&& [id, res] : ResourcesById) {
        SetResourceCounters(res.Get());
    }
}

void TQuoterResources::FillCounters(NKikimrKesus::TEvGetQuoterResourceCountersResult& counters) {
    for (auto&& [path, res] : ResourcesByPath) {
        auto* resCounter = counters.AddResourceCounters();
        resCounter->SetResourcePath(path);
        resCounter->SetAllocated(res->GetCounters().GetAllocated());
    }
}

void TQuoterResources::SetPipeServerId(TQuoterSessionId sessionId, const NActors::TActorId& prevId, const NActors::TActorId& id) {
    if (prevId) {
        auto [prevIt, prevItEnd] = PipeServerIdToSession.equal_range(prevId);
        for (; prevIt != prevItEnd; ++prevIt) {
            if (prevIt->second.second == sessionId.second) { // compare resource id
                PipeServerIdToSession.erase(prevIt);
                break;
            }
        }
    }
    if (id) {
        PipeServerIdToSession.emplace(id, sessionId);
    }
}

void TQuoterResources::DisconnectSession(const NActors::TActorId& pipeServerId) {
    auto [pipeToSessionItBegin, pipeToSessionItEnd] = PipeServerIdToSession.equal_range(pipeServerId);
    for (auto pipeToSessionIt = pipeToSessionItBegin; pipeToSessionIt != pipeToSessionItEnd; ++pipeToSessionIt) {
        const TQuoterSessionId sessionId = pipeToSessionIt->second;
        const NActors::TActorId sessionClientId = sessionId.first;

        {
            const auto sessionIter = Sessions.find(sessionId);
            Y_ABORT_UNLESS(sessionIter != Sessions.end());
            TQuoterSession* session = sessionIter->second.Get();
            session->GetResource()->OnSessionDisconnected(sessionClientId);
            session->CloseSession(Ydb::StatusIds::SESSION_EXPIRED, "Disconected.");
            Sessions.erase(sessionIter);
        }
    }
    PipeServerIdToSession.erase(pipeToSessionItBegin, pipeToSessionItEnd);
}

void TQuoterResources::SetQuoterPath(const TString& quoterPath) {
    QuoterPath = quoterPath;
    for (auto&& [id, resource] : ResourcesById) {
        resource->SetQuoterPath(QuoterPath);
    }
}

void TTickProcessorQueue::Push(const TTickProcessorTask& task) {
    if (!Empty()) {
        if (Sorted) {
            if (task < Tasks.back()) {
                Sorted = false;
            }
        }
        if (!Sorted && task < Top()) {
            TopIndex = Tasks.size();
        }
    }
    Tasks.push_back(task);
}

void TTickProcessorQueue::Pop() {
    ++FirstIndex;
    ++TopIndex;
    Y_ASSERT(FirstIndex <= Tasks.size());
}

const TTickProcessorTask& TTickProcessorQueue::Top() const {
    return Tasks[TopIndex];
}

bool TTickProcessorQueue::Empty() const {
    return FirstIndex == Tasks.size();
}

void TTickProcessorQueue::Merge(TTickProcessorQueue&& from) {
    Y_ASSERT(from.FirstIndex == 0);
    Sort();
    from.Sort();

    if (Empty()) {
        std::swap(Tasks, from.Tasks);
        FirstIndex = 0;
        TopIndex = 0;
        return;
    } else if (from.Empty()) {
        return;
    }

    if (Tasks.back() <= from.Tasks.front()) {
        if (FirstIndex > 0) {
            auto to = Tasks.begin();
            auto from = Tasks.begin() + FirstIndex;
            const size_t count = Tasks.size() - FirstIndex;
            if (2 * count < Tasks.size()) {
                for (size_t i = 0; i < count; ++i, ++to, ++from) {
                    *to = std::move(*from);
                }
                FirstIndex = 0;
                TopIndex = 0;
                Tasks.resize(count);
            }
        }
        Tasks.reserve(Tasks.size() + from.Tasks.size());
        Tasks.insert(Tasks.end(), std::make_move_iterator(from.Tasks.begin()), std::make_move_iterator(from.Tasks.end()));
        return;
    }

    std::vector<TTickProcessorTask> dest;
    dest.reserve(Tasks.size() - FirstIndex + from.Tasks.size());
    auto current = Tasks.begin() + FirstIndex;
    auto end = Tasks.end();
    auto fromCurrent = from.Tasks.begin();
    auto fromEnd = from.Tasks.end();
    std::merge(std::make_move_iterator(current),
               std::make_move_iterator(end),
               std::make_move_iterator(fromCurrent),
               std::make_move_iterator(fromEnd),
               std::back_inserter(dest));
    std::swap(Tasks, dest);
    TopIndex = 0;
    FirstIndex = 0;
}

void TTickProcessorQueue::Sort() {
    if (!Sorted) {
        std::sort(Tasks.begin() + FirstIndex, Tasks.end());
        TopIndex = FirstIndex;
        Sorted = true;
    }
}

}
}
