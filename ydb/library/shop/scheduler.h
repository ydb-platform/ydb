#pragma once

#include "counters.h"
#include "probes.h"
#include "resource.h"
#include "schedulable.h"

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <algorithm>

namespace NShop {

static constexpr i64 IsActive = -1;
static constexpr i64 IsDetached = -2;

struct TConsumerCounters;
template <class TRes = TSingleResource> class TConsumer;
template <class TRes = TSingleResource> class TFreezable;
template <class TRes = TSingleResource> class TScheduler;

///////////////////////////////////////////////////////////////////////////////

template <class TRes>
class TConsumer {
private:
    using TCost = typename TRes::TCost;
    using TTag = typename TRes::TTag;
    using FreezableT = TFreezable<TRes>;
    using SchedulerT = TScheduler<TRes>;

    // Configuration
    TString Name;
    TWeight Weight = 1;
    SchedulerT* Scheduler = nullptr;
    FreezableT* Freezable = nullptr;

    // Scheduling state
    TTag HolTag = TTag(); // Tag (aka virtual time) of head-of-line schedulable
    ui64 GlobalBusyPeriod = 0;
    i64 DeactIdx = IsDetached; // Index of deactivated consumer in parent freezable (or special values)
    TCost Underestimation = TCost(); // sum over all errors (realCost - estCost) to be fixed

    // Monitoring
    TTag Consumed = TTag();
    static TConsumerCounters FakeCounters;
    TConsumerCounters* Counters = &FakeCounters;

public:
    virtual ~TConsumer()
    {
        Detach();
    }

    void SetName(const TString& name) { Name = name; }
    const TString& GetName() const { return Name; }

    void SetWeight(TWeight weight) { Y_ABORT_UNLESS(weight > 0); Weight = weight; }
    TWeight GetWeight() const { return Weight; }

    void SetScheduler(SchedulerT* scheduler) { Scheduler = scheduler; }
    SchedulerT* GetScheduler() const { return Scheduler; }

    void SetFreezable(FreezableT* freezable) { Freezable = freezable; }
    FreezableT* GetFreezable() const { return Freezable; }

    void SetCounters(TConsumerCounters* counters) { Counters = counters; }
    TConsumerCounters* GetCounters() { return Counters; }

    TTag GetTag() const { return HolTag; }

    virtual TSchedulable<typename TRes::TCost>* PopSchedulable() = 0;
    virtual bool Empty() const = 0;
    virtual void UpdateCounters() {}

    void Activate(); // NOTE: it also attaches consumer if it hasn't been done yet
    void Deactivate();
    void Detach();
    void AddUnderestimation(TCost cost) {
        Underestimation += cost;
    }

    TTag ResetConsumed();
    void Print(IOutputStream& out) const;

    friend class TFreezable<TRes>;
    friend class TScheduler<TRes>;
};

template <class TRes>
TConsumerCounters TConsumer<TRes>::FakeCounters;

template <class TRes>
class TFreezable {
private:
    using TCost = typename TRes::TCost;
    using TTag = typename TRes::TTag;
    using SchedulerT = TScheduler<TRes>;
    using ConsumerT = TConsumer<TRes>;

private:
    struct THeapItem {
        typename TRes::TKey Key;
        ConsumerT* Consumer;

        explicit THeapItem(ConsumerT* consumer)
            : Consumer(consumer)
        {
            UpdateKey();
        }

        void UpdateKey()
        {
            TRes::SetKey(Key, Consumer);
        }

        bool operator<(const THeapItem& rhs) const
        {
            return rhs.Key < Key; // swapped for min-heap
        }
    };

private:
    // Configuration
    TString Name;
    SchedulerT* Scheduler = nullptr;

    // Scheduling state
    TVector<THeapItem> Heap;
    i64 HeapEndIdx = 0;
    bool Frozen = false;
    TTag LastFreeze = TTag();
    TTag Offset = TTag();
    ui64 GlobalBusyPeriod = 0;
public:
    void SetName(const TString& name) { Name = name; }
    TString GetName() { return Name; }

    TSchedulable<typename TRes::TCost>* PopSchedulable();
    bool Empty() const;

    void SetScheduler(SchedulerT* scheduler) { Scheduler = scheduler; }
    SchedulerT* GetScheduler() { return Scheduler; }

    // Activates consumers for which `predicate' holds true
    template <class TPredicate>
    size_t ActivateConsumers(TPredicate&& predicate);

    // Should be called before delete or just for detaching freezable
    void Deactivate();

    bool IsFrozen() const { return Frozen; }
    void Freeze();
    void Unfreeze();

    // Defines scheduling order of freezables
    bool operator<(const TFreezable& o) const
    {
        Y_ASSERT(HeapEndIdx > 0);
        Y_ASSERT(o.HeapEndIdx > 0);
        Y_ASSERT(!Heap.empty());
        Y_ASSERT(!o.Heap.empty());
        return TRes::OffsetKey(Heap.front().Key, Offset)
             < TRes::OffsetKey(o.Heap.front().Key, o.Offset);
    }
private:
    void Activate(ConsumerT* consumer);
    void Insert(ConsumerT* consumer);
    void Deactivate(ConsumerT* consumer);
    void Detach(ConsumerT* consumer);

    friend class TScheduler<TRes>;
    friend class TConsumer<TRes>;
};

template <class TRes>
class TScheduler: public TConsumer<TRes> {
private:
    using TCost = typename TRes::TCost;
    using TTag = typename TRes::TTag;
    using FreezableT = TFreezable<TRes>;
    using ConsumerT = TConsumer<TRes>;

private:
    struct TCmp {
        bool operator()(FreezableT* lhs, FreezableT* rhs) const
        {
            return *lhs < *rhs;
        }
    };
    TVector<FreezableT*> Freezables;
    ui64 FrozenCount = 0;
    TTag VirtualTime = TTag();
    TTag LatestFinish = TTag();
    ui64 GlobalBusyPeriod = 0;
    TCost GlobalBusyPeriodCost = TCost();
    ui64 GlobalBusyPeriodPops = 0;
    ui64 GlobalPeriodTs = 0; // Idle or busy period start time (in cycles)
    ui64 GlobalIdlePeriodDuration = ui64(-1);
    ui64 LocalBusyPeriod = 0;
    TCost LocalBusyPeriodCost = TCost();
    ui64 LocalBusyPeriodPops = 0;
    ui64 LocalPeriodTs = 0; // Idle or busy period start time (in cycles)
    ui64 LocalIdlePeriodDuration = ui64(-1);
public:
    TSchedulable<typename TRes::TCost>* PopSchedulable() override;
    bool Empty() const override;
    void Clear();
    void UpdateCounters() override;
    void Print(IOutputStream& out) const;
    void DebugPrint(IOutputStream& out) const;
private:
    void Activate(FreezableT* freezable);
    void Deactivate(FreezableT* freezable);
    void StartGlobalBusyPeriod();
    void StartGlobalIdlePeriod();
    void StartLocalBusyPeriod();
    void StartLocalIdlePeriod();

    friend class TFreezable<TRes>;
    friend class TConsumer<TRes>;
};

///////////////////////////////////////////////////////////////////////////////

template <class TRes>
inline
void TConsumer<TRes>::Activate()
{
    if (DeactIdx != IsActive) { // Avoid double activation
        Freezable->Activate(this);
    }
}

template <class TRes>
inline
void TConsumer<TRes>::Deactivate()
{
    if (DeactIdx == IsActive) {
        Freezable->Deactivate(this);
    }
}

template<class TRes>
void TConsumer<TRes>::Detach()
{
    if (DeactIdx == IsActive) {
        Freezable->Deactivate(this);
        Freezable->Detach(this);
    } else if (DeactIdx != IsDetached) {
        Freezable->Detach(this);
    }
    DeactIdx = IsDetached;
}

template <class TRes>
inline
typename TRes::TTag TConsumer<TRes>::ResetConsumed()
{
    TTag result = Consumed;
    Consumed = 0;
    return result;
}

template <class TRes>
inline
void TConsumer<TRes>::Print(IOutputStream& out) const
{
    out << "Weight = " << Weight << Endl
        << "HolTag = " << HolTag << Endl
        << "GlobalBusyPeriod = " << GlobalBusyPeriod << Endl
        << "Borrowed = " << Counters->Borrowed << Endl
        << "Donated = " << Counters->Donated << Endl
        << "Usage = " << Counters->Usage << Endl
        << "DeactIdx = " << DeactIdx << Endl;
}

template <class TRes>
inline
TSchedulable<typename TRes::TCost>* TFreezable<TRes>::PopSchedulable()
{
    if (!Empty() && !Frozen) {
        PopHeap(Heap.begin(), Heap.begin() + HeapEndIdx);
        HeapEndIdx--;
        THeapItem& item = Heap[HeapEndIdx];
        ConsumerT* consumer = item.Consumer;

        if (TSchedulable<typename TRes::TCost>* schedulable = consumer->PopSchedulable()) {
            Scheduler->VirtualTime = consumer->HolTag + Offset;

            // Try to immediatly fix any discrepancy between real and estimated costs
            // (as long as it doesn't lead to negative cost)
            TCost cost = schedulable->Cost + consumer->Underestimation;
            if (cost >= 0) {
                consumer->Underestimation = 0;
            } else {
                // lower consumer overestimation by schedulable cost, and allow "free" usage
                consumer->Underestimation = cost;
                cost = 0;
            }
            TTag duration = cost / consumer->Weight;
            consumer->HolTag += duration;
            consumer->Consumed += duration;
            Scheduler->LatestFinish = Max(Scheduler->LatestFinish, consumer->HolTag);

            if (consumer->Empty()) {
                consumer->DeactIdx = HeapEndIdx;
            } else {
                item.UpdateKey();
                HeapEndIdx++;
                PushHeap(Heap.begin(), Heap.begin() + HeapEndIdx);
            }

            GLOBAL_LWPROBE(SHOP_PROVIDER, PopSchedulable,
                           Scheduler->GetName(), Name, consumer->Name,
                           Scheduler->VirtualTime, Scheduler->GlobalBusyPeriod,
                           consumer->HolTag + Offset, schedulable->Cost,
                           cost - schedulable->Cost, consumer->Weight, duration);

            return schedulable;
        } else {
            // Consumer turns to be inactive -- just deactivate it and repeat
            GLOBAL_LWPROBE(SHOP_PROVIDER, DeactivateImplicit,
                Scheduler->GetName(), Name, consumer->Name,
                Scheduler->VirtualTime, Scheduler->GlobalBusyPeriod,
                consumer->HolTag, Frozen);
            consumer->DeactIdx = HeapEndIdx;
            return nullptr;
        }
    } else {
        return nullptr;
    }
}

template <class TRes>
inline
bool TFreezable<TRes>::Empty() const
{
    return HeapEndIdx == 0;
}

template <class TRes>
template <class TPredicate>
inline
size_t TFreezable<TRes>::ActivateConsumers(TPredicate&& predicate)
{
    size_t prevHeapEndIdx = HeapEndIdx;
    for (auto i = Heap.begin() + HeapEndIdx, e = Heap.end(); i != e; ++i) {
        ConsumerT* consumer = i->Consumer;
        if (predicate(consumer)) { // Consumer should be activated
            Y_ASSERT(consumer->DeactIdx >= 0);
            Insert(consumer);
        }
    }
    size_t inserted = HeapEndIdx - prevHeapEndIdx;
    if (prevHeapEndIdx == 0 && inserted > 0 && !Frozen) {
        Scheduler->Activate(this);
    }
    return inserted;
}

template <class TRes>
inline
void TFreezable<TRes>::Deactivate()
{
    if (Frozen) {
        Unfreeze();
    }
    if (!Empty()) {
        Scheduler->Deactivate(this);
    }
}

template <class TRes>
inline
void TFreezable<TRes>::Freeze()
{
    Y_ASSERT(!Frozen);
    Scheduler->FrozenCount++;
    if (!Empty()) {
        Scheduler->Deactivate(this);
    }
    Frozen = true;
    LastFreeze = Scheduler->VirtualTime;
    GLOBAL_LWPROBE(SHOP_PROVIDER, Freeze,
        Scheduler->GetName(), Name,
        Scheduler->VirtualTime, Scheduler->GlobalBusyPeriod, GlobalBusyPeriod,
        Scheduler->FrozenCount, Offset);
}

template <class TRes>
inline
void TFreezable<TRes>::Unfreeze()
{
    Y_ASSERT(Frozen);
    Frozen = false;
    Offset = Offset + Scheduler->VirtualTime - LastFreeze;
    GLOBAL_LWPROBE(SHOP_PROVIDER, Unfreeze,
        Scheduler->GetName(), Name,
        Scheduler->VirtualTime, Scheduler->GlobalBusyPeriod, GlobalBusyPeriod,
        Scheduler->FrozenCount, Offset);
    if (!Empty()) {
        Scheduler->Activate(this);
        Scheduler->FrozenCount--;
    } else {
        Scheduler->FrozenCount--;
        if (Scheduler->Empty()) {
            Scheduler->StartGlobalIdlePeriod();
        }
    }
}

template <class TRes>
inline
void TFreezable<TRes>::Activate(ConsumerT* consumer)
{
    bool wasEmpty = Empty();
    Insert(consumer);
    if (!Frozen && wasEmpty) {
        Scheduler->Activate(this);
    }
}

template <class TRes>
inline
void TFreezable<TRes>::Insert(ConsumerT* consumer)
{
    // Update consumer's tag
    if (consumer->GlobalBusyPeriod != Scheduler->GlobalBusyPeriod) {
        consumer->GlobalBusyPeriod = Scheduler->GlobalBusyPeriod;
        consumer->HolTag = TTag();
        // Estimation errors of pervious busy period does not matter any more
        consumer->Underestimation = TCost();
        if (GlobalBusyPeriod != Scheduler->GlobalBusyPeriod) {
            GlobalBusyPeriod = Scheduler->GlobalBusyPeriod;
            Offset = TTag();
        }
    }
    TTag vtime = (Frozen? LastFreeze: Scheduler->VirtualTime);
    TTag selfvtime = vtime - Offset;
    consumer->HolTag = Max(consumer->HolTag, selfvtime);

    // Insert consumer into attached vector (if detached)
    Y_ASSERT(consumer->DeactIdx != IsActive);
    if (consumer->DeactIdx == IsDetached) {
        consumer->DeactIdx = Heap.size();
        Heap.emplace_back(consumer);
    }

    // Swap consumer in place to push into heap
    i64 deactIdx = consumer->DeactIdx;
    THeapItem& place = Heap[HeapEndIdx];
    if (deactIdx != HeapEndIdx) {
        THeapItem& oldItem = Heap[deactIdx];
        DoSwap(place, oldItem);
        oldItem.Consumer->DeactIdx = deactIdx;
    }
    place.UpdateKey();

    // Push into active consumers heap
    HeapEndIdx++;
    PushHeap(Heap.begin(), Heap.begin() + HeapEndIdx);
    consumer->DeactIdx = IsActive;
    GLOBAL_LWPROBE(SHOP_PROVIDER, Activate,
        Scheduler->GetName(), Name, consumer->Name,
        Scheduler->VirtualTime, Scheduler->GlobalBusyPeriod,
        consumer->HolTag, Frozen);
}

template <class TRes>
inline
void TFreezable<TRes>::Deactivate(ConsumerT* consumer)
{
    GLOBAL_LWPROBE(SHOP_PROVIDER, Deactivate,
        Scheduler->GetName(), Name, consumer->Name,
        Scheduler->VirtualTime, Scheduler->GlobalBusyPeriod,
        consumer->HolTag, Frozen);

    Y_ASSERT(consumer->DeactIdx == IsActive);
    for (auto i = Heap.begin(), e = Heap.begin() + HeapEndIdx; i != e; ++i) {
        if (i->Consumer == consumer) {
            HeapEndIdx--;
            i64 idx = i - Heap.begin();
            if (HeapEndIdx != idx) {
                DoSwap(Heap[HeapEndIdx], Heap[idx]);
            }
            consumer->DeactIdx = HeapEndIdx;
            if (!Empty()) {
                MakeHeap(Heap.begin(), Heap.begin() + HeapEndIdx);
            } else {
                Scheduler->Deactivate(this);
            }
            return;
        }
    }
    Y_ABORT_UNLESS("trying to deactivate unknown consumer");
}

template <class TRes>
inline
void TFreezable<TRes>::Detach(ConsumerT* consumer)
{
    Y_ASSERT(consumer->DeactIdx >= 0);
    i64 oldIdx = consumer->DeactIdx;
    i64 newIdx = Heap.size() - 1;
    if (oldIdx != newIdx) {
        DoSwap(Heap[oldIdx], Heap[newIdx]);
        Heap[oldIdx].Consumer->DeactIdx = oldIdx;
    }
    Heap.pop_back();
}

template <class TRes>
inline
TSchedulable<typename TRes::TCost>* TScheduler<TRes>::PopSchedulable()
{
    while (!Empty()) {
        auto iter = std::min_element(Freezables.begin(), Freezables.end(), TCmp());

        FreezableT* freezable = *iter;

        TSchedulable<typename TRes::TCost>* schedulable = freezable->PopSchedulable();
        if (schedulable) {
            GlobalBusyPeriodCost = GlobalBusyPeriodCost + schedulable->Cost;
            GlobalBusyPeriodPops++;
            LocalBusyPeriodCost = LocalBusyPeriodCost + schedulable->Cost;
            LocalBusyPeriodPops++;
        }

        if (freezable->Empty()) {
            if (Freezables.size() > 1) {
                DoSwap(*iter, Freezables.back());
                Freezables.pop_back();
            } else {
                Freezables.pop_back();
                StartGlobalIdlePeriod();
                StartLocalIdlePeriod();
                if (this->GetFreezable()) { // Deactivate parent if it is not root scheduler
                    TConsumer<TRes>::Deactivate();
                }
            }
        }

        if (schedulable) {
            return schedulable;
        }
    }
    return nullptr;
}

template <class TRes>
inline
bool TScheduler<TRes>::Empty() const
{
    return Freezables.empty();
}

template <class TRes>
inline
void TScheduler<TRes>::Clear()
{
    TVector<ConsumerT*> consumers;
    for (FreezableT* freezable : Freezables) {
        for (auto item : freezable->Heap) {
            // Delay modification of Freezables (we are iterating over it)
            consumers.push_back(item.Consumer);
        }
    }
    for (ConsumerT* consumer : consumers) {
        consumer->Detach();
    }
    Y_ASSERT(Freezables.empty());
}

template <class TRes>
inline
void TScheduler<TRes>::UpdateCounters()
{
    TCountersAggregator<TConsumer<TRes>, TTag> aggr;
    for (FreezableT* freezable : Freezables) {
        for (auto item : freezable->Heap) {
            aggr.Add(item.Consumer);
            item.Consumer->UpdateCounters(); // Recurse into children
        }
    }
    aggr.Apply();
}

template <class TRes>
inline
void TScheduler<TRes>::Print(IOutputStream& out) const
{
    out << "FrozenCount = " << FrozenCount << Endl
        << "VirtualTime = " << VirtualTime << Endl
        << "LatestFinish = " << LatestFinish << Endl
        << "GlobalBusyPeriod = " << GlobalBusyPeriod << Endl
        << "GlobalBusyPeriodCost = " << GlobalBusyPeriodCost << Endl
        << "GlobalBusyPeriodPops = " << GlobalBusyPeriodPops << Endl
        << "GlobalPeriodTs = " << GlobalPeriodTs << Endl
        << "GlobalIdlePeriodDuration = " << GlobalIdlePeriodDuration << Endl
        << "LocalBusyPeriod = " << LocalBusyPeriod << Endl
        << "LocalBusyPeriodCost = " << LocalBusyPeriodCost << Endl
        << "LocalBusyPeriodPops = " << LocalBusyPeriodPops << Endl
        << "LocalPeriodTs = " << LocalPeriodTs << Endl
        << "LocalIdlePeriodDuration = " << LocalIdlePeriodDuration << Endl;
    if (this->GetFreezable()) {
        TConsumer<TRes>::Print(out);
    }
}

template <class TRes>
void TScheduler<TRes>::DebugPrint(IOutputStream& out) const
{
    for (const auto& freezable : Freezables) {
        i64 idx = 0;
        for (const auto& item: freezable->Heap) {
            out << (idx < freezable->HeapEndIdx? "* ": "  ")
                << "key:" << item.Key
                << " tag:" << item.Consumer->HolTag
                << " didx:" << item.Consumer->DeactIdx << Endl;
            idx++;
        }
    }
    out << Endl;
}

template <class TRes>
inline
void TScheduler<TRes>::Activate(FreezableT* freezable)
{
    Y_ASSERT(!freezable->Frozen);
    bool wasEmpty = Empty();
    Freezables.push_back(freezable);
    if (wasEmpty) {
        StartGlobalBusyPeriod();
        StartLocalBusyPeriod();
        if (this->GetFreezable()) { // Activate parent if it is not root scheduler
            TConsumer<TRes>::Activate();
        }
    }
}

template <class TRes>
inline
void TScheduler<TRes>::Deactivate(FreezableT* freezable)
{
    Y_ASSERT(!freezable->Frozen);
    Freezables.erase(std::remove(Freezables.begin(), Freezables.end(), freezable), Freezables.end());
    if (Empty()) {
        StartGlobalIdlePeriod();
        StartLocalIdlePeriod();
        if (this->GetFreezable()) { // Deactivate parent if it is not root scheduler
            TConsumer<TRes>::Deactivate();
        }
    }
}

template <class TRes>
inline
void TScheduler<TRes>::StartGlobalBusyPeriod()
{
    if (FrozenCount == 0) {
        Y_ASSERT(GlobalIdlePeriodDuration == ui64(-1));
        ui64 now = GetCycleCount();
        GlobalIdlePeriodDuration = GlobalPeriodTs? Duration(GlobalPeriodTs, now): 0;
        GlobalPeriodTs = now;
    }
}

template <class TRes>
inline
void TScheduler<TRes>::StartGlobalIdlePeriod()
{
    if (FrozenCount == 0 && GlobalIdlePeriodDuration != ui64(-1)) {
        ui64 now = GetCycleCount();
        ui64 busyPeriodDuration = Duration(GlobalPeriodTs, now);
        GlobalPeriodTs = now;
        GLOBAL_LWPROBE(SHOP_PROVIDER, GlobalBusyPeriod, this->GetName(),
            VirtualTime, GlobalBusyPeriod,
            GlobalBusyPeriodPops, GlobalBusyPeriodCost,
            CyclesToMs(GlobalIdlePeriodDuration), CyclesToMs(busyPeriodDuration),
            double(busyPeriodDuration) / (GlobalIdlePeriodDuration + busyPeriodDuration));
        GlobalBusyPeriod++;
        GlobalBusyPeriodCost = TCost();
        GlobalBusyPeriodPops = 0;
        GlobalIdlePeriodDuration = ui64(-1);

        VirtualTime = TTag();
        LatestFinish = TTag();
    }
}

template <class TRes>
inline
void TScheduler<TRes>::StartLocalBusyPeriod()
{
    Y_ASSERT(LocalIdlePeriodDuration == ui64(-1));
    ui64 now = GetCycleCount();
    LocalIdlePeriodDuration = LocalPeriodTs? Duration(LocalPeriodTs, now): 0;
    LocalPeriodTs = now;
}

template <class TRes>
inline
void TScheduler<TRes>::StartLocalIdlePeriod()
{
    Y_ASSERT(LocalIdlePeriodDuration != ui64(-1));
    ui64 now = GetCycleCount();
    ui64 busyPeriodDuration = Duration(LocalPeriodTs, now);
    LocalPeriodTs = now;
    GLOBAL_LWPROBE(SHOP_PROVIDER, LocalBusyPeriod, this->GetName(),
        VirtualTime, LocalBusyPeriod,
        LocalBusyPeriodPops, LocalBusyPeriodCost,
        CyclesToMs(LocalIdlePeriodDuration), CyclesToMs(busyPeriodDuration),
        double(busyPeriodDuration) / (LocalIdlePeriodDuration + busyPeriodDuration));
    LocalBusyPeriod++;
    LocalBusyPeriodCost = TCost();
    LocalBusyPeriodPops = 0;
    LocalIdlePeriodDuration = ui64(-1);

    VirtualTime = LatestFinish;
}

}
