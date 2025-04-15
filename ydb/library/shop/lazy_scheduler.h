#pragma once

#include "counters.h"
#include "probes.h"
#include "resource.h"
#include "schedulable.h"
#include "shop_state.h"

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

///
/// Lazy scheduler is SFQ-scheduler with almost zero freezing cost (in term of CPU cycles).
/// Note that it is true even for hierarchical scheduling. Laziness means that hierarchy,
/// schedulers and consumers are not modified to freeze/unfreeze a machine. There is a global
/// shop state that reflects current state of machines, and every consumer has set of machines
/// from which at least one must be warm (not frozen) for consumer to be scheduled.
///
/// In terms of resulting schedule (and fairness) there is no difference between shop/scheduler.h
/// and lazy scheduler, but there are the following issues:
/// - lazy scheduler has no layer of TFreezable objects between scheduler and consumers, but
///   freeze control is done through global (shared between schedulers) shop state;
/// - lazy scheduler has no heap to find consumer with minimal tag, but it just uses vector and
///   scans it on every scheduling event (it's okay if there are few consumers per node);
/// - it is more compute-intensive but less memory-intensive, because frozen key are pulled
///   on every scheduling event, but data required for it is dense;
/// - lazy scheduler uses small stack-vectors to remove one memory hop.
///
/// Note that there is no thread-safety precautions (except for atomic monitoring counters).
///

namespace NShop {
namespace NLazy {

template <class TRes = TSingleResourceDense> class TConsumer;
template <class TRes = TSingleResourceDense> class TScheduler;
template <class TRes = TSingleResourceDense> class TRootScheduler;

using TConsumerIdx = ui16;

constexpr TMachineIdx DoNotActivate = TMachineIdx(-1);
constexpr TMachineIdx DoNotDeactivate = TMachineIdx(-1);

///////////////////////////////////////////////////////////////////////////////

constexpr size_t ResOptCount = 2; // optimal resource count (for stack vec)
constexpr size_t ConOptCount = 6; // optimal consumers count (for stack vec)

///////////////////////////////////////////////////////////////////////////////

struct TCtx {
    // Warm machines on current subtree (include overrides on local states)
    TMachineMask Warm;

    // Create root context using shop state
    explicit TCtx(const TShopState& shopState)
        : Warm(shopState.Warm)
    {}

    // Create subcontext on subtree with override and localstate
    // NOTE: overrides bit marked in `override` using values from `state`
    explicit TCtx(const TCtx& ctx, TMachineMask override, TMachineMask state)
        : Warm((ctx.Warm & ~override) | (state & override))
    {}

    bool IsRunnable(TMachineMask active, TMachineMask allowed) const
    {
        return Warm & active & allowed;
    }
};

///////////////////////////////////////////////////////////////////////////////

template <class TRes>
class TConsumer: public virtual TThrRefBase {
private:
    using TCost = typename TRes::TCost;
    using TTag = typename TRes::TTag;
    using TSch = TScheduler<TRes>;

private:
    // Configuration
    TString Name;
    TWeight Weight = 1;
    TSch* Scheduler = nullptr;

    // Scheduling state
    TCost Underestimation = TCost(); // sum over all errors (realCost - estCost) to be fixed
    TConsumerIdx Idx = TConsumerIdx(-1); // Index of consumer in parent scheduler
    ui64 BusyPeriod = ui64(-1);

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

    TSch* GetScheduler() const { return Scheduler; }

    void SetCounters(TConsumerCounters* counters) { Counters = counters; }
    TConsumerCounters* GetCounters() { return Counters; }

    // Returns the next thing to schedule or nullptr (denial)
    // WARNING: in case of denial, this scheduler will NOT be called with the
    // same set (or its subset) of warm machines, till either
    //  - Allow(mi) is called on this or CHILD scheduler with machine from the set
    //  - DropDenials() is called on any PARENT scheduler (including root one)
    virtual TSchedulable<typename TRes::TCost>* PopSchedulable(const TCtx& ctx) = 0;

    // Recursive counters update (nothing to do on leafs)
    virtual void UpdateCounters() {}

    // Activates consumer to compete for resources on given machine
    void Activate(TMachineIdx mi);

    // Removes consumer from competition for resources on given machine
    // NOTE: Scheduler keeps track of resource consumption
    // NOTE: till busy period is over (in case deactivated consumer will return)
    void Deactivate(TMachineIdx mi);

    // Notify about unfreeze
    virtual void Allow(TMachineIdx mi);

    // Clear every cached denial
    virtual void DropDenials() {}

    // Removes consumer from competition for resources on every machine
    virtual void DeactivateAll() = 0;

    // Unlinks scheduler and consumer
    void Detach();

    // Adds estimation error (real - est) cost to be fixed on this consumer
    void AddUnderestimation(TCost cost) {
        Underestimation += cost;
    }

    // Monitoring
    TTag ResetConsumed();
    virtual void DebugPrint(IOutputStream& out, const TString& indent = {}) const;
    TString DebugString() const;

    friend class TScheduler<TRes>;
};

template <class TRes>
TConsumerCounters TConsumer<TRes>::FakeCounters;

template <class TRes>
class TScheduler: public TConsumer<TRes> {
protected:
    using TCost = typename TRes::TCost;
    using TTag = typename TRes::TTag;
    using TKey = typename TRes::TKey;
    using TCon = TConsumer<TRes>;

protected:
    // Configuration
    TShopState* ShopState = nullptr;
    TMachineMask* Override = nullptr;
    TMachineMask* LocalState = nullptr;
    TConsumerIdx Active = 0; // Total number of consumers currently active on at least on machine
    ui64 BusyPeriod = 0;
    TTag VirtualTime = TTag();

    // Scheduling state
    TStackVec<TCon*, ConOptCount> Consumers; // consumerIdx -> consumer-object
    TStackVec<TMachineMask, ConOptCount> Masks; // consumerIdx -> machineIdx -> (1=active | 0=empty)
    TStackVec<TMachineMask, ConOptCount> Allowed; // consumerIdx -> machineIdx -> (1=allowed | 0=frozen)
    TStackVec<TKey, ConOptCount> Keys; // consumerIdx -> TKey (e.g. amount of consumed resource so far)
    TStackVec<TConsumerIdx, ConOptCount> FrozenTmp; // temporary array for consumers we have to pull
    TStackVec<TConsumerIdx, ResOptCount> ActivePerMachine; // machineIdx -> number of active consumers
public:
    // NOTE: Shop state should be set before scheduling begins
    // NOTE: Shop state can be shared by multiple schedulers
    void SetShopState(TShopState* value);
    TShopState* GetShopState() const { return ShopState; }

    // Do not use `ShopState` bits marked by `override` mask, instead use corresponding bits from `local` mask
    void OverrideState(TMachineMask* override, TMachineMask* local) { Override = override; LocalState = local; }
    TMachineMask* GetOverride() const { return Override; }
    TMachineMask* GetLocalState() const { return LocalState; }

    // Attaches new consumer to scheduler
    void Attach(TCon* consumer);

    // Returns the next thing to schedule or nullptr (if empty and/or frozen)
    TSchedulable<typename TRes::TCost>* PopSchedulable();
    TSchedulable<typename TRes::TCost>* PopSchedulable(const TCtx& ctx) override;

    // Return true iff there is no active consumers for warm machines
    bool Empty() const; // DEPRECATED: use TRootScheduler::IsRunnable()

    // Activates every consumer for which `machineIdx(consumer)'
    // call returns idx different from `DoNotActivate'.
    // Returns number of activatied consumers
    template <class TFunc>
    size_t ActivateConsumers(TFunc&& machineIdx);

    // Deactivates every consumer for which `machineIdx(consumer)'
    // call returns idx different from `DoNotActivate'.
    // Returns number of activatied consumers
    template <class TFunc>
    size_t DeactivateConsumers(TFunc&& machineIdx);

    // Recursively DeactivateAll() every active consumer
    void DeactivateAll() override;

    // Clears every cached denial
    void DropDenials() override;

    // Detaches all consumers
    void Clear();

    // Monitoring
    void UpdateCounters() override;
    void DebugPrint(IOutputStream& out, const TString& indent = {}) const override;
private:
    TSchedulable<typename TRes::TCost>* PopSchedulableImpl(const TCtx& ctx);
    bool IsNotFrozen(TConsumerIdx ci) const;
    void Swap(TConsumerIdx ci1, TConsumerIdx ci2);
    void Activate(TConsumerIdx ci, TMachineIdx mi);
    TConsumerIdx Deactivate(TConsumerIdx ci, TMachineIdx mi);
    void Deny(TConsumerIdx ci, TMachineMask machines);
    void AllowChild(TConsumerIdx ci, TMachineIdx mi);
    void Detach(TConsumerIdx ci);

    friend class TConsumer<TRes>;
};

///////////////////////////////////////////////////////////////////////////////

template <class TRes>
class TRootScheduler: public TScheduler<TRes> {
protected:
    TMachineMask RootAllowed;
public:
    bool IsRunnable() const;
    TSchedulable<typename TRes::TCost>* PopSchedulable(const TCtx& ctx) override;
    void Allow(TMachineIdx mi) override;
};

///////////////////////////////////////////////////////////////////////////////

template <class TRes>
inline
void TConsumer<TRes>::Activate(TMachineIdx mi)
{
    Scheduler->Activate(Idx, mi);
}

template <class TRes>
inline
void TConsumer<TRes>::Deactivate(TMachineIdx mi)
{
    Scheduler->Deactivate(Idx, mi);
}

template <class TRes>
void TConsumer<TRes>::Allow(TMachineIdx mi)
{
    if (Scheduler) {
        Scheduler->AllowChild(Idx, mi);
    }
}

template <class TRes>
inline
void TConsumer<TRes>::Detach()
{
    if (Scheduler) {
        Scheduler->Detach(Idx);
    }
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
void TConsumer<TRes>::DebugPrint(IOutputStream& out, const TString& indent) const
{
    out << indent << "Name: " << Name << Endl
        << indent << "Weight: " << Weight << Endl
        << indent << "BusyPeriod: " << BusyPeriod << Endl
        << indent << "Borrowed: " << Counters->Borrowed << Endl
        << indent << "Donated: " << Counters->Donated << Endl
        << indent << "Usage: " << Counters->Usage << Endl
        << indent << "Idx: " << Idx << Endl;
}

template <class TRes>
inline
TString TConsumer<TRes>::DebugString() const
{
    TStringStream ss;
    DebugPrint(ss);
    return ss.Str();
}

///////////////////////////////////////////////////////////////////////////////

template <class TRes>
inline
void TScheduler<TRes>::SetShopState(TShopState* value)
{
    ShopState = value;
    // NOTE: machines can be added and removed on flight, but cannot change its machineIds
    ActivePerMachine.resize(ShopState->MachineCount, 0);
}

template <class TRes>
inline
TSchedulable<typename TRes::TCost>* TScheduler<TRes>::PopSchedulable()
{
    return PopSchedulable(TCtx(*ShopState));
}


template <class TRes>
inline
TSchedulable<typename TRes::TCost>* TScheduler<TRes>::PopSchedulable(const TCtx& ctx)
{
    if (Override) {
        Y_ASSERT(LocalState);
        return PopSchedulableImpl(TCtx(ctx, *Override, *LocalState));
    } else {
        return PopSchedulableImpl(ctx);
    }
}

template <class TRes>
inline
TSchedulable<typename TRes::TCost>* TScheduler<TRes>::PopSchedulableImpl(const TCtx& ctx)
{
    while (true) {
        // Select next warm consumer to be scheduled in a fair way
        // And collect frozen consumer keys (to pull later)
        TKey minKey = TRes::MaxKey();
        TConsumerIdx ci = TConsumerIdx(-1);
        FrozenTmp.clear();
        for (TConsumerIdx cj = 0; cj < Active; cj++) {
            if (ctx.IsRunnable(Masks[cj], Allowed[cj])) {
                // If consumer is active on at least one warm machine -- it can be scheduled
                TKey& key = Keys[cj];
                if (key < minKey) {
                    minKey = key;
                    ci = cj;
                }
            } else {
                // Cannot be scheduled, but is active on at least one frozen machine
                FrozenTmp.emplace_back(cj);
            }
        }

        if (ci == TConsumerIdx(-1)) { // If all warm and active consumers turned to be inactive
            GLOBAL_LWPROBE(SHOP_PROVIDER, LazyIdlePeriod,
                           this->Name, VirtualTime, BusyPeriod);

            // Start new idle period (and new busy period that will follow it)
            BusyPeriod++;
            for (TConsumerIdx cj : FrozenTmp) { // Frozen consumers should NOT have idle periods
                Consumers[cj]->BusyPeriod = BusyPeriod;
                TKey& frozenKey = Keys[cj];
                frozenKey = TRes::OffsetKey(frozenKey, -VirtualTime);
            }
            VirtualTime = TTag();

            if (!this->GetScheduler()) {
                GLOBAL_LWPROBE(SHOP_PROVIDER, LazyWasted,
                               this->Name, VirtualTime, BusyPeriod,
                               this->DebugString());
            }

            return nullptr;
        }

        // Schedule consumer
        TCon* consumer = Consumers[ci];
        ui64 busyPeriod = BusyPeriod;

        if (TSchedulable<typename TRes::TCost>* schedulable = consumer->PopSchedulable(ctx)) {
            // Update idx in case consumer was deactivated in PopSchedulable()
            // NOTE: FrozenKeys cannot invalidate even if consumer was deactivated
            ci = consumer->Idx;

            // Propagate virtual time (if idle period has not been started)
            if (busyPeriod == BusyPeriod) {
                TTag vtime0 = VirtualTime;
                VirtualTime = TRes::GetTag(minKey);
                TTag vtimeDelta = VirtualTime - vtime0;

                // Pull all frozen consumers
                for (TConsumerIdx cj : FrozenTmp) {
                    TKey& frozenKey = Keys[cj];
                    frozenKey = TRes::OffsetKey(frozenKey, vtimeDelta);
                }
            }

            // Try to immediatly fix any discrepancy between real and estimated costs
            // (as long as it doesn't lead to negative cost)
            TCost cost = schedulable->Cost + consumer->Underestimation;
            if (cost >= 0) {
                consumer->Underestimation = 0;
            } else {
                // Lower consumer overestimation by schedulable cost, and allow "free" usage
                consumer->Underestimation = cost;
                cost = 0;
            }

            // Update consumer key
            TTag duration = cost / consumer->Weight;
            TKey& key = Keys[ci];
            key = TRes::OffsetKey(key, duration);
            consumer->Consumed += duration;

            GLOBAL_LWPROBE(SHOP_PROVIDER, LazyPopSchedulable,
                           this->Name, consumer->Name,
                           VirtualTime, BusyPeriod,
                           TRes::GetTag(key), schedulable->Cost,
                           cost - schedulable->Cost, consumer->Weight, duration);

            return schedulable;
        } else {
            // Consumer refused to return schedulable
            // Deny entrance into it with currently warm machines to avoid hang up
            Deny(ci, ctx.Warm);
        }
    }
}

template <class TRes>
inline
bool TScheduler<TRes>::Empty() const
{
    for (TConsumerIdx cj = 0; cj < Active; cj++) {
        if (IsNotFrozen(cj)) {
            return false; // There is an active consumer on at least one warm machine
        }
    }
    return true; // There is no active consumers for warm machines
}

template <class TRes>
inline
void TScheduler<TRes>::Clear()
{
    auto consumers = Consumers;
    for (TCon* c : consumers) {
        c->Detach();
    }
    Y_ASSERT(Masks.empty());
    Y_ASSERT(Allowed.empty());
    Y_ASSERT(Keys.empty());
    Y_ASSERT(Consumers.empty());
}

template <class TRes>
inline
void TScheduler<TRes>::UpdateCounters()
{
    TCountersAggregator<TCon, TTag> aggr;
    for (TCon* consumer : Consumers) {
        aggr.Add(consumer);
        consumer->UpdateCounters(); // Recurse into children
    }
    aggr.Apply();
}

template <class TRes>
inline
void TScheduler<TRes>::DebugPrint(IOutputStream& out, const TString& indent) const
{
    out << indent << "VirtualTime: " << VirtualTime << Endl
        << indent << "BusyPeriod: " << BusyPeriod << Endl;
    if (this->GetScheduler()) {
        // Print consumer-related part of scheduler, if it is not root scheudler
        TCon::DebugPrint(out, indent);
    }
    for (size_t ci = 0; ci < Consumers.size(); ci++) {
        const TCon& c = *Consumers[ci];
        TString indent2 = indent + "  ";
        out << indent << "Consumer {" << Endl
            << indent2 << (ci < Active? "[A]": "[I]") << "Mask: " << Masks[ci].ToString(ShopState->MachineCount) << Endl
            << indent2 << "Allowed: " << Allowed[ci].ToString(ShopState->MachineCount) << Endl
            << indent2 << "Key: " << Keys[ci] << Endl;
        c.DebugPrint(out, indent2);
        out << indent << "}" << Endl;
    }
}

template <class TRes>
inline
void TScheduler<TRes>::Attach(TCon* consumer)
{
    // Reset state in case consumer was used in another scheduler
    consumer->BusyPeriod = size_t(-1);
    consumer->Underestimation = TCost();
    consumer->Idx = Consumers.size();
    consumer->Scheduler = this;

    // Attach consumer
    Consumers.emplace_back(consumer);
    Masks.emplace_back(0);
    Allowed.emplace_back(0);
    Keys.emplace_back(TRes::ZeroKey(consumer));

    Y_ABORT_UNLESS(Consumers.size() < (1ull << (8 * sizeof(TConsumerIdx))), "too many consumers");
}

template <class TRes>
inline
bool TScheduler<TRes>::IsNotFrozen(TConsumerIdx ci) const
{
    if (Override && LocalState) {
        TMachineMask override = *Override;
        TMachineMask state = *LocalState;
        // Override bit marked in `Override` using values from `LocalState`
        // and then check if there is at least one warm and active machine
        return ((ShopState->Warm & ~override)
                        | (state &  override)) & Masks[ci];
    } else {
        // Just check if there is at least one warm and active machine
        return ShopState->Warm & Masks[ci];
    }
}

template <class TRes>
inline
void TScheduler<TRes>::Swap(TConsumerIdx ci1, TConsumerIdx ci2)
{
    DoSwap(Consumers[ci1], Consumers[ci2]);
    DoSwap(Masks[ci1], Masks[ci2]);
    DoSwap(Allowed[ci1], Allowed[ci2]);
    DoSwap(Keys[ci1], Keys[ci2]);
    Consumers[ci1]->Idx = ci1;
    Consumers[ci2]->Idx = ci2;
}

template <class TRes>
inline
void TScheduler<TRes>::Activate(TConsumerIdx ci, TMachineIdx mi)
{
    AllowChild(ci, mi); // Activation is worthless without allowment

    TMachineMask& mask = Masks[ci];
    if (mask.Get(mi)) {
        return; // Avoid double activation
    }

    // Recursively activate machine in parent schedulers (if required)
    if (this->GetScheduler() && ActivePerMachine[mi] == 0) {
        TCon::Activate(mi);
    }

    // Activate machine for consumer
    mask.Set(mi);
    ActivePerMachine[mi]++;

    // Update consumer's key
    TCon* consumer = Consumers[ci];
    TKey& key = Keys[ci];
    if (consumer->BusyPeriod != BusyPeriod) {
        consumer->BusyPeriod = BusyPeriod;
        // Memoryless property: consumption history must be reset on new busy period
        key = TRes::ZeroKey(consumer);
        // Estimation errors of pervious busy period does not matter any more
        consumer->Underestimation = TCost();
    }
    TRes::ActivateKey(key, VirtualTime); // Do not reclaim unused resource from past

    GLOBAL_LWPROBE(SHOP_PROVIDER, LazyActivate,
                   this->Name, consumer->Name,
                   VirtualTime, BusyPeriod,
                   TRes::GetTag(Keys[ci]), mask.ToString(ShopState->MachineCount),
                   mi, ActivePerMachine[mi]);

    // Rearrange consumers to have completely deactivated (on every machine) in separate range
    if (ci >= Active) {
        if (ci != Active) {
            Swap(ci, Active);
        }
        Active++;
    }
}

template <class TRes>
template <class TFunc>
inline
size_t TScheduler<TRes>::ActivateConsumers(TFunc&& machineIdx)
{
    size_t prevActiveCount = Active;
    auto i = Consumers.begin() + Active;
    auto e = Consumers.end();
    for (TConsumerIdx ci = Active; i != e; ++i, ci++) {
        TCon* consumer = *i;
        TMachineIdx mi = machineIdx(consumer);
        if (mi != DoNotActivate) {
            Activate(ci, mi);
        }
    }
    return Active - prevActiveCount;
}

template <class TRes>
template <class TFunc>
inline
size_t TScheduler<TRes>::DeactivateConsumers(TFunc&& machineIdx)
{
    size_t prevActiveCount = Active;
    auto i = Consumers.rend() - Active;
    auto e = Consumers.rend();
    for (TConsumerIdx ci = Active - 1; i != e; ++i, ci--) {
        TCon* consumer = *i;
        TMachineIdx mi = machineIdx(consumer);
        if (mi != DoNotDeactivate) {
            Deactivate(ci, mi);
        }
    }
    return prevActiveCount - Active;
}

template <class TRes>
inline
void TScheduler<TRes>::DeactivateAll()
{
    while (Active > 0) {
        TCon* consumer = Consumers[Active - 1];
        consumer->DeactivateAll();
        Y_ABORT_UNLESS(Masks[consumer->Idx].IsZero(),
                "unable to deactivate consumer '%s' of scheduler '%s'",
                consumer->GetName().c_str(), this->GetName().c_str());
    }
}

template <class TRes>
void TScheduler<TRes>::DropDenials()
{
    // every active consumed is allowed recursively
    for (TConsumerIdx ci = 0; ci < Active; ci++) {
        Allowed[ci] = Masks[ci];
        Consumers[ci]->DropDenials();
    }
}

template <class TRes>
inline
TConsumerIdx TScheduler<TRes>::Deactivate(TConsumerIdx ci, TMachineIdx mi)
{
    TMachineMask& mask = Masks[ci];
    if (!mask.Get(mi)) {
        return ci; // Avoid double deactivation
    }

    // Deactivate machine for consumer
    mask.Reset(mi);
    // NOTE: Deny is not required, because only (Allowed & Masks) matters and mask is reset
    ActivePerMachine[mi]--;

    GLOBAL_LWPROBE(SHOP_PROVIDER, LazyDeactivate,
                   this->Name, Consumers[ci]->Name,
                   VirtualTime, BusyPeriod,
                   TRes::GetTag(Keys[ci]), mask.ToString(ShopState->MachineCount),
                   mi, ActivePerMachine[mi]);

    // Recursively deactivate machine in parent schedulers (if required)
    if (this->GetScheduler() && ActivePerMachine[mi] == 0) {
        TCon::Deactivate(mi);
    }

    // Rearrange consumers to have completely deactivated (on every machine) in separate range
    // (to be able to quickly iterate through them)
    if (ci < Active && mask.IsZero()) {
        Active--;
        Swap(ci, Active);
        ci = Active;
    }

    return ci;
}

template <class TRes>
void TScheduler<TRes>::AllowChild(TConsumerIdx ci, TMachineIdx mi)
{
    TMachineMask& allowed = Allowed[ci];
    if (allowed.Get(mi)) {
        return; // Avoid double allow
    }

    // Recursively allow machine in parent schedulers
    this->Allow(mi);

    GLOBAL_LWPROBE(SHOP_PROVIDER, LazyAllow,
                   this->GetName(), Consumers[ci]->GetName(),
                   VirtualTime, BusyPeriod,
                   mi);

    allowed.Set(mi);
}

template <class TRes>
void TScheduler<TRes>::Deny(TConsumerIdx ci, TMachineMask machines)
{
    GLOBAL_LWPROBE(SHOP_PROVIDER, LazyDeny,
                   this->GetName(), Consumers[ci]->GetName(),
                   VirtualTime, BusyPeriod,
                   machines.ToString(ShopState->MachineCount));

    Allowed[ci].ResetAll(machines);
}

template <class TRes>
inline
void TScheduler<TRes>::Detach(TConsumerIdx ci)
{
    // Completely deactivate consumer on every machine
    for (TMachineIdx mi = 0; mi < ShopState->MachineCount; mi++) {
        ci = Deactivate(ci, mi);
    }
    Y_ASSERT(ci >= Active);

    // Unlink consumer and scheduler
    if (ci != Consumers.size() - 1) { // Consumer should be the last one to be detached
        Swap(ci, Consumers.size() - 1);
    }
    Consumers.back()->Scheduler = nullptr;
    Consumers.pop_back();
    Masks.pop_back();
    Allowed.pop_back();
    Keys.pop_back();
}


///////////////////////////////////////////////////////////////////////////////

template <class TRes>
bool TRootScheduler<TRes>::IsRunnable() const
{
    return RootAllowed & this->GetShopState()->Warm;
}

template <class TRes>
TSchedulable<typename TRes::TCost>* TRootScheduler<TRes>::PopSchedulable(const TCtx& ctx)
{
    if (TSchedulable<typename TRes::TCost>* schedulable = TScheduler<TRes>::PopSchedulable(ctx)) {
        return schedulable;
    } else { // Deny
        GLOBAL_LWPROBE(SHOP_PROVIDER, LazyRootDeny,
                       this->GetName(), ctx.Warm.ToString(this->ShopState->MachineCount));
        RootAllowed.ResetAll(ctx.Warm);
        return nullptr;
    }
}

template <class TRes>
void TRootScheduler<TRes>::Allow(TMachineIdx mi)
{
    if (RootAllowed.Get(mi)) {
        return; // Avoid double allow
    }

    GLOBAL_LWPROBE(SHOP_PROVIDER, LazyRootAllow,
                   this->GetName(), mi);

    RootAllowed.Set(mi);
}

}
}
