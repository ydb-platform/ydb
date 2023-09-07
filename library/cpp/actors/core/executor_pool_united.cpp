#include "executor_pool_united.h"
#include "executor_pool_united_workers.h"

#include "actor.h"
#include "balancer.h"
#include "cpu_state.h"
#include "executor_thread.h"
#include "probes.h"
#include "mailbox.h"
#include "scheduler_queue.h"
#include <library/cpp/actors/util/affinity.h>
#include <library/cpp/actors/util/cpu_load_log.h>
#include <library/cpp/actors/util/datetime.h>
#include <library/cpp/actors/util/futex.h>
#include <library/cpp/actors/util/intrinsics.h>
#include <library/cpp/actors/util/timerfd.h>

#include <util/system/datetime.h>
#include <util/system/hp_timer.h>

#include <algorithm>

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    struct TUnitedWorkers::TWorker: public TNonCopyable {
        TAutoPtr<TExecutorThread> Thread;
        volatile TThreadId ThreadId = UnknownThreadId;
        NSchedulerQueue::TQueueType SchedulerQueue;
    };

    struct TUnitedWorkers::TPool: public TNonCopyable {
        TAtomic Waiters = 0; // Number of idle cpus, waiting for activations in this pool
        char Padding[64 - sizeof(TAtomic)];

        TPerfectActivationQueue Activations; // MPMC-queue for mailbox activations
        TAtomic Active = 0; // Number of mailboxes ready for execution or currently executing
        TAtomic Tokens = 0; // Pending tokens (token is required for worker to start execution, guarantees concurrency limit and activation availability)
        volatile bool StopFlag = false;

        // Configuration
        TPoolId PoolId;
        TAtomicBase Concurrency; // Max concurrent workers running this pool
        IExecutorPool* ExecutorPool;
        TMailboxTable* MailboxTable;
        ui64 TimePerMailboxTs;
        ui32 EventsPerMailbox;

        // Cpus this pool is allowed to run on
        // Cpus are specified in wake order
        TStackVec<TCpu*, 15> WakeOrderCpus;

        ~TPool() {
            while (Activations.Pop()) {}
        }

        void Stop() {
            AtomicStore(&StopFlag, true);
        }

        bool IsUnited() const {
            return WakeOrderCpus.size();
        }

        // Add activation of newly scheduled mailbox. Returns generated token (unless concurrency is exceeded)
        bool PushActivation(ui32 activation, ui64 /*revolvingCounter*/) {
            Activations.Push(activation);
            TAtomicBase active = AtomicIncrement(Active);
            if (active <= Concurrency) { // token generated
                AtomicIncrement(Tokens);
                return true;
            }
            return false;
        }

        template <bool Relaxed>
        static bool TryAcquireTokenImpl(TAtomic* tokens) {
            while (true) {
                TAtomicBase value;
                if constexpr (Relaxed) {
                    value = RelaxedLoad(tokens);
                } else {
                    value = AtomicLoad(tokens);
                }
                if (value > 0) {
                    if (AtomicCas(tokens, value - 1, value)) {
                        return true; // token acquired
                    }
                } else {
                    return false; // no more tokens
                }
            }
        }

        // Try acquire pending token. Must be done before execution
        bool TryAcquireToken() {
            return TryAcquireTokenImpl<false>(&Tokens);
        }

        // Try acquire pending token. Must be done before execution
        bool TryAcquireTokenRelaxed() {
            return TryAcquireTokenImpl<true>(&Tokens);
        }

        // Get activation. Requires acquired token.
        void BeginExecution(ui32& activation, ui64 /*revolvingCounter*/) {
            while (!RelaxedLoad(&StopFlag)) {
                if (activation = Activations.Pop()) {
                    return;
                }
                SpinLockPause();
            }
            activation = 0; // should stop
        }

        // End currently active execution and start new one if token is available.
        // Reuses token if it's not destroyed.
        // Returned `true` means successful switch, `activation` is filled.
        // Returned `false` means execution has ended, no need to call StopExecution()
        bool NextExecution(ui32& activation, ui64 revolvingCounter) {
            if (AtomicDecrement(Active) >= Concurrency) { // reuse just released token
                BeginExecution(activation, revolvingCounter);
                return true;
            } else if (TryAcquireToken()) { // another token acquired
                BeginExecution(activation, revolvingCounter);
                return true;
            }
            return false; // no more tokens available
        }

        // Stop active execution. Returns released token (unless it is destroyed)
        bool StopExecution() {
            TAtomicBase active = AtomicDecrement(Active);
            if (active >= Concurrency) { // token released
                AtomicIncrement(Tokens);
                return true;
            }
            return false; // token destroyed
        }

        // Switch worker context into this pool
        void Switch(TWorkerContext& wctx, ui64 softDeadlineTs, TExecutorThreadStats& stats) {
            wctx.Switch(ExecutorPool, MailboxTable, TimePerMailboxTs, EventsPerMailbox, softDeadlineTs, &stats);
        }
    };

    class TPoolScheduler {
        class TSchedulable {
            // Lower PoolBits store PoolId
            // All other higher bits store virtual runtime in cycles
            using TValue = ui64;
            TValue Value;

            static constexpr ui64 PoolIdMask = ui64((1ull << PoolBits) - 1);
            static constexpr ui64 VRunTsMask = ~PoolIdMask;

        public:
            explicit TSchedulable(TPoolId poolId = MaxPools, ui64 vrunts = 0)
                : Value((poolId & PoolIdMask) | (vrunts & VRunTsMask))
            {}

            TPoolId GetPoolId() const {
                return Value & PoolIdMask;
            }

            ui64 GetVRunTs() const {
                // Do not truncate pool id
                // NOTE: it decrease accuracy, but improves performance
                return Value;
            }

            ui64 GetPreciseVRunTs() const {
                return Value & VRunTsMask;
            }

            void SetVRunTs(ui64 vrunts) {
                Value = (Value & PoolIdMask) | (vrunts & VRunTsMask);
            }

            void Account(ui64 base, ui64 ts) {
                // Add at least minimum amount to change Value
                SetVRunTs(base + Max(ts, PoolIdMask + 1));
            }
        };

        // For min-heap of Items
        struct TCmp {
            bool operator()(TSchedulable lhs, TSchedulable rhs) const {
                return lhs.GetVRunTs() > rhs.GetVRunTs();
            }
        };

        TPoolId Size = 0; // total number of pools on this cpu
        TPoolId Current = 0; // index of current pool in `Items`

        // At the beginning `Current` items are orginized as binary min-heap -- ready to be scheduled
        // The rest `Size - Current` items are unordered (required to keep track of last vrunts)
        TSchedulable Items[MaxPools]; // virtual runtime in cycles for each pool
        ui64 MinVRunTs = 0; // virtual runtime used by waking pools (system's vrunts)
        ui64 Ts = 0; // real timestamp of current execution start (for accounting)

        // Maps PoolId into it's inverse weight
        ui64 InvWeights[MaxPools];
        static constexpr ui64 VRunTsOverflow = ui64(1ull << 62ull) / MaxPoolWeight;

    public:
        void AddPool(TPoolId pool, TPoolWeight weight) {
            Items[Size] = TSchedulable(pool, MinVRunTs);
            Size++;
            InvWeights[pool] = MaxPoolWeight / std::clamp(weight ? weight : DefPoolWeight, MinPoolWeight, MaxPoolWeight);
        }

        // Iterate over pools in scheduling order
        // should be used in construction:
        // for (TPoolId pool = Begin(); pool != End(); pool = Next())
        TPoolId Begin() {
            // Wrap vruntime around to avoid overflow, if required
            if (Y_UNLIKELY(MinVRunTs >= VRunTsOverflow)) {
                for (TPoolId i = 0; i < Size; i++) {
                    ui64 ts = Items[i].GetPreciseVRunTs();
                    Items[i].SetVRunTs(ts >= VRunTsOverflow ? ts - VRunTsOverflow : 0);
                }
                MinVRunTs -= VRunTsOverflow;
            }
            Current = Size;
            std::make_heap(Items, Items + Current, TCmp());
            return Next();
        }

        constexpr TPoolId End() const {
            return MaxPools;
        }

        TPoolId Next() {
            if (Current > 0) {
                std::pop_heap(Items, Items + Current, TCmp());
                Current--;
                return CurrentPool();
            } else {
                return End();
            }
        }

        // Scheduling was successful, we are going to run CurrentPool()
        void Scheduled() {
            MinVRunTs = Max(MinVRunTs, Items[Current].GetPreciseVRunTs());
            // NOTE: Ts is propagated on Account() to avoid gaps
        }

        // Schedule specific pool that woke up cpu after idle
        void ScheduledAfterIdle(TPoolId pool, ui64 ts) {
            if (Y_UNLIKELY(ts < Ts)) { // anomaly: time goes backwards (e.g. rdtsc is reset to zero on cpu reset)
                Ts = ts; // just skip anomalous time slice
                return;
            }
            MinVRunTs += (ts - Ts) * (MaxPoolWeight / DefPoolWeight); // propagate system's vrunts to blur difference between pools
            Ts = ts; // propagate time w/o accounting to any pool

            // Set specified pool as current, it requires scan
            for (Current = 0; Current < Size && pool != Items[Current].GetPoolId(); Current++) {}
            Y_VERIFY(Current < Size);
        }

        // Account currently running pool till now (ts)
        void Account(ui64 ts) {
            // Skip time slice for the first run and when time goes backwards (e.g. rdtsc is reset to zero on cpu reset)
            if (Y_LIKELY(Ts > 0 && Ts <= ts)) {
                TPoolId pool = CurrentPool();
                Y_VERIFY(pool < MaxPools);
                Items[Current].Account(MinVRunTs, (ts - Ts) * InvWeights[pool]);
            }
            Ts = ts; // propagate time
        }

        TPoolId CurrentPool() const {
            return Items[Current].GetPoolId();
        }
    };

    // Cyclic array of timers for idle workers to wait for hard preemption on
    struct TIdleQueue: public TNonCopyable {
        TArrayHolder<TTimerFd> Timers;
        size_t Size;
        TAtomic EnqueueCounter = 0;
        TAtomic DequeueCounter = 0;

        explicit TIdleQueue(size_t size)
            : Timers(new TTimerFd[size])
            , Size(size)
        {}

        void Stop() {
            for (size_t i = 0; i < Size; i++) {
                Timers[i].Wake();
            }
        }

        // Returns timer which new idle-worker should wait for
        TTimerFd* Enqueue() {
            return &Timers[AtomicGetAndIncrement(EnqueueCounter) % Size];
        }

        // Returns timer that hard preemption should trigger to wake idle-worker
        TTimerFd* Dequeue() {
            return &Timers[AtomicGetAndIncrement(DequeueCounter) % Size];
        }
    };

    // Base class for cpu-local managers that help workers on single cpu to cooperate
    struct TCpuLocalManager: public TThrRefBase {
        TUnitedWorkers* United;

        explicit TCpuLocalManager(TUnitedWorkers* united)
            : United(united)
        {}

        virtual TWorkerId WorkerCount() const = 0;
        virtual void AddWorker(TWorkerId workerId) = 0;
        virtual void Stop() = 0;
    };

    // Represents cpu with single associated worker that is able to execute any pool.
    // It always executes pool assigned by balancer and switch pool only if assigned pool has changed
    struct TAssignedCpu: public TCpuLocalManager {
        bool Started = false;

        TAssignedCpu(TUnitedWorkers* united)
            : TCpuLocalManager(united)
        {}

        TWorkerId WorkerCount() const override {
            return 1;
        }

        void AddWorker(TWorkerId workerId) override {
            Y_UNUSED(workerId);
        }

        ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) {
            ui32 activation;
            if (Y_UNLIKELY(!Started)) {
                Started = true;
            } else if (Y_UNLIKELY(United->IsPoolReassigned(wctx))) {
                United->StopExecution(wctx.PoolId); // stop current execution and switch pool if reassigned
            } else if (United->NextExecution(wctx.PoolId, activation, revolvingCounter)) {
                return activation; // another activation from currently executing pool (or 0 if stopped)
            }

            // Switch to another pool, it blocks until token is acquired
            if (Y_UNLIKELY(!SwitchPool(wctx))) {
                return 0; // stopped
            }
            United->SwitchPool(wctx, 0);
            United->BeginExecution(wctx.PoolId, activation, revolvingCounter);
            return activation;
        }

        void Stop() override {
        }

    private:
        // Sets next pool to run, and acquires token, blocks if there are no tokens
        bool SwitchPool(TWorkerContext& wctx) {
            if (Y_UNLIKELY(United->IsStopped())) {
                return false;
            }

            // Run balancer (if it's time to)
            United->Balance();

            // Select pool to execute
            wctx.PoolId = United->AssignedPool(wctx);
            Y_VERIFY(wctx.PoolId != CpuShared);
            if (United->TryAcquireToken(wctx.PoolId)) {
                return true;
            }

            // No more work -- wait for activations (spinning, then blocked)
            wctx.PoolId = United->Idle(wctx.PoolId, wctx);

            // Wakeup or stop occured
            if (Y_UNLIKELY(wctx.PoolId == CpuStopped)) {
                return false;
            }
            return true; // United->Idle() has already acquired token
        }
    };

    // Lock-free data structure that help workers on single cpu to discover their state and do hard preemptions
    struct TSharedCpu: public TCpuLocalManager {
        // Current lease
        volatile TLease::TValue CurrentLease;
        char Padding1[64 - sizeof(TLease)];

        // Slow pools
        // the highest bit: 1=wait-for-slow-workers mode 0=else
        // any lower bit (poolId is bit position): 1=pool-is-slow 0=pool-is-fast
        volatile TPoolsMask SlowPoolsMask = 0;
        char Padding2[64 - sizeof(TPoolsMask)];

        // Must be accessed under never expiring lease to avoid races
        TPoolScheduler PoolSched;
        TWorkerId FastWorker = MaxWorkers;
        TTimerFd* PreemptionTimer = nullptr;
        ui64 HardPreemptionTs = 0;
        bool Started = false;

        TIdleQueue IdleQueue;

        struct TConfig {
            const TCpuId CpuId;
            const TWorkerId Workers;
            ui64 SoftLimitTs;
            ui64 HardLimitTs;
            ui64 EventLimitTs;
            ui64 LimitPrecisionTs;
            const int IdleWorkerPriority;
            const int FastWorkerPriority;
            const bool NoRealtime;
            const bool NoAffinity;
            const TCpuAllocation CpuAlloc;

            TConfig(const TCpuAllocation& allocation, const TUnitedWorkersConfig& united)
                : CpuId(allocation.CpuId)
                , Workers(allocation.AllowedPools.size() + 1)
                , SoftLimitTs(Us2Ts(united.PoolLimitUs))
                , HardLimitTs(Us2Ts(united.PoolLimitUs + united.EventLimitUs))
                , EventLimitTs(Us2Ts(united.EventLimitUs))
                , LimitPrecisionTs(Us2Ts(united.LimitPrecisionUs))
                , IdleWorkerPriority(std::clamp<ui64>(united.IdleWorkerPriority ? united.IdleWorkerPriority : 20, 1, 99))
                , FastWorkerPriority(std::clamp<ui64>(united.FastWorkerPriority ? united.FastWorkerPriority : 10, 1, IdleWorkerPriority - 1))
                , NoRealtime(united.NoRealtime)
                , NoAffinity(united.NoAffinity)
                , CpuAlloc(allocation)
            {}
        };

        TConfig Config;
        TVector<TWorkerId> Workers;

        TSharedCpu(const TConfig& cfg, TUnitedWorkers* united)
            : TCpuLocalManager(united)
            , IdleQueue(cfg.Workers)
            , Config(cfg)
        {
            for (const auto& pa : Config.CpuAlloc.AllowedPools) {
                PoolSched.AddPool(pa.PoolId, pa.Weight);
            }
        }

        TWorkerId WorkerCount() const override {
            return Config.Workers;
        }

        void AddWorker(TWorkerId workerId) override {
            if (Workers.empty()) {
                // Grant lease to the first worker
                AtomicStore(&CurrentLease, TLease(workerId, NeverExpire).Value);
            }
            Workers.push_back(workerId);
        }

        ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) {
            ui32 activation;
            if (!wctx.Lease.IsNeverExpiring()) {
                if (wctx.SoftDeadlineTs < GetCycleCountFast()) { // stop if lease has expired or is near to be expired
                    United->StopExecution(wctx.PoolId);
                } else if (United->NextExecution(wctx.PoolId, activation, revolvingCounter)) {
                    return activation; // another activation from currently executing pool (or 0 if stopped)
                }
            }

            // Switch to another pool, it blocks until token is acquired
            if (Y_UNLIKELY(!SwitchPool(wctx))) {
                return 0; // stopped
            }
            United->BeginExecution(wctx.PoolId, activation, revolvingCounter);
            return activation;
        }

        void Stop() override {
            IdleQueue.Stop();
        }

    private:
        enum EPriority {
            IdlePriority, // highest (real-time, Config.IdleWorkerPriority)
            FastPriority, // normal (real-time, Config.FastWorkerPriority)
            SlowPriority, // lowest (not real-time)
        };

        enum EWorkerAction {
            // Fast-worker
            ExecuteFast,
            WaitForSlow,

            // Slow-worker
            BecameIdle,
            WakeFast,

            // Idle-worker
            BecameFast,
            Standby,

            // Common
            Stopped,
        };

        // Thread-safe; should be called from worker
        // Blocks for idle-workers; sets lease and next pool to run
        bool SwitchPool(TWorkerContext& wctx) {
            TTimerFd* idleTimer = nullptr;
            while (true) {
                if (DisablePreemptionAndTryExtend(wctx.Lease)) { // if fast-worker
                    if (Y_UNLIKELY(!Started)) {
                        SetPriority(0, FastPriority);
                        Started = true;
                    }
                    while (true) {
                        switch (FastWorkerAction(wctx)) {
                        case ExecuteFast:
                            United->SwitchPool(wctx, wctx.Lease.GetPreciseExpireTs() - Config.EventLimitTs);
                            EnablePreemptionAndGrant(wctx.Lease);
                            return true;
                        case WaitForSlow:
                            FastWorkerSleep(GetCycleCountFast() + Config.SoftLimitTs);
                            break;
                        case Stopped: return false;
                        default: Y_FAIL();
                        }
                    }
                } else if (wctx.Lease.IsNeverExpiring()) { // if idle-worker
                    switch (IdleWorkerAction(idleTimer, wctx.Lease.GetWorkerId())) {
                    case BecameFast:
                        SetPriority(0, FastPriority);
                        break; // try acquire new lease
                    case Standby:
                        if (!idleTimer) {
                            idleTimer = IdleQueue.Enqueue();
                        }
                        SetPriority(0, IdlePriority);
                        idleTimer->Wait();
                        break;
                    case Stopped: return false;
                    default: Y_FAIL();
                    }
                } else { // lease has expired and hard preemption occured, so we are executing in a slow-worker
                    wctx.IncrementPreemptedEvents();
                    switch (SlowWorkerAction(wctx.PoolId)) {
                    case WakeFast:
                        WakeFastWorker();
                        [[fallthrough]]; // no break; pass through
                    case BecameIdle:
                        wctx.Lease = wctx.Lease.NeverExpire();
                        wctx.PoolId = MaxPools;
                        idleTimer = nullptr;
                        break;
                    case Stopped: return false;
                    default: Y_FAIL();
                    }
                }
            }
        }

        enum ETryRunPool {
            RunFastPool,
            RunSlowPool,
            NoTokens,
        };

        ETryRunPool TryRun(TPoolId pool) {
            while (true) {
                // updates WaitPoolsFlag in SlowPoolsMask according to scheduled pool slowness
                TPoolsMask slow = AtomicLoad(&SlowPoolsMask);
                if ((1ull << pool) & slow) { // we are about to execute slow pool (fast-worker will just wait, token is NOT required)
                    if (slow & WaitPoolsFlag) {
                        return RunSlowPool; // wait flag is already set
                    } else {
                        if (AtomicCas(&SlowPoolsMask, slow | WaitPoolsFlag, slow)) { // try set wait flag
                            return RunSlowPool; // wait flag has been successfully set
                        }
                    }
                } else { // we are about to execute fast pool, token required
                    if (slow & WaitPoolsFlag) { // reset wait flag if required
                        if (AtomicCas(&SlowPoolsMask, slow & ~WaitPoolsFlag, slow)) { // try reset wait flag
                            return United->TryAcquireToken(pool) ? RunFastPool : NoTokens; // wait flag has been successfully reset
                        }
                    } else {
                        return United->TryAcquireToken(pool) ? RunFastPool : NoTokens; // wait flag is already reset
                    }
                }
            }
        }

        EWorkerAction FastWorkerAction(TWorkerContext& wctx) {
            if (Y_UNLIKELY(United->IsStopped())) {
                return Stopped;
            }

            // Account current pool
            ui64 ts = GetCycleCountFast();
            PoolSched.Account(ts);

            // Select next pool to execute
            for (wctx.PoolId = PoolSched.Begin(); wctx.PoolId != PoolSched.End(); wctx.PoolId = PoolSched.Next()) {
                switch (TryRun(wctx.PoolId)) {
                case RunFastPool:
                    PoolSched.Scheduled();
                    wctx.Lease = PostponePreemption(wctx.Lease.GetWorkerId(), ts);
                    return ExecuteFast;
                case RunSlowPool:
                    PoolSched.Scheduled();
                    ResetPreemption(wctx.Lease.GetWorkerId(), ts); // there is no point in preemption during wait
                    return WaitForSlow;
                case NoTokens: // concurrency limit reached, or no more work in pool
                    break; // just try next pool (if any)
                }
            }

            // No more work, no slow-workers -- wait for activations (active, then blocked)
            wctx.PoolId = United->Idle(CpuShared, wctx);

            // Wakeup or stop occured
            if (Y_UNLIKELY(wctx.PoolId == CpuStopped)) {
                return Stopped;
            }
            ts = GetCycleCountFast();
            PoolSched.ScheduledAfterIdle(wctx.PoolId, ts);
            wctx.Lease = PostponePreemption(wctx.Lease.GetWorkerId(), ts);
            return ExecuteFast; // United->Idle() has already acquired token
        }

        EWorkerAction IdleWorkerAction(TTimerFd* idleTimer, TWorkerId workerId) {
            if (Y_UNLIKELY(United->IsStopped())) {
                return Stopped;
            }
            if (!idleTimer) { // either worker start or became idle -- hard preemption is not required
                return Standby;
            }

            TLease lease = TLease(AtomicLoad(&CurrentLease));
            ui64 ts = GetCycleCountFast();
            if (lease.GetExpireTs() < ts) { // current lease has expired
                if (TryBeginHardPreemption(lease)) {
                    SetPoolIsSlowFlag(PoolSched.CurrentPool());
                    TWorkerId preempted = lease.GetWorkerId();
                    SetPriority(United->GetWorkerThreadId(preempted), SlowPriority);
                    LWPROBE(HardPreemption, Config.CpuId, PoolSched.CurrentPool(), preempted, workerId);
                    EndHardPreemption(workerId);
                    return BecameFast;
                } else {
                    // Lease has been changed just now, no way we need preemption right now, so no retry needed
                    return Standby;
                }
            } else {
                // Lease has not expired yet (maybe never expiring lease)
                return Standby;
            }
        }

        EWorkerAction SlowWorkerAction(TPoolId pool) {
            if (Y_UNLIKELY(United->IsStopped())) {
                return Stopped;
            }
            while (true) {
                TPoolsMask slow = AtomicLoad(&SlowPoolsMask);
                if (slow & (1ull << pool)) {
                    if (slow == (1ull << pool) & WaitPoolsFlag) { // the last slow pool is going to became fast
                        if (AtomicCas(&SlowPoolsMask, 0, slow)) { // reset both pool-is-slow flag and WaitPoolsFlag
                            return WakeFast;
                        }
                    } else { // there are (a) several slow-worker or (b) one slow-worker w/o waiting fast-worker
                        if (AtomicCas(&SlowPoolsMask, slow & ~(1ull << pool), slow)) { // reset pool-is-slow flag
                            return BecameIdle;
                        }
                    }
                } else {
                    // SlowWorkerAction has been called between TryBeginHardPreemption and SetPoolIsSlowFlag
                    // flag for this pool is not set yet, but we can be sure pool is slow:
                    //  - because SlowWorkerAction has been called;
                    //  - this mean lease has expired and hard preemption occured.
                    // So just wait other worker to call SetPoolIsSlowFlag
                    LWPROBE(SlowWorkerActionRace, Config.CpuId, pool, slow);
                }
            }
        }

        void SetPoolIsSlowFlag(TPoolId pool) {
            while (true) {
                TPoolsMask slow = AtomicLoad(&SlowPoolsMask);
                if ((slow & (1ull << pool)) == 0) { // if pool is fast
                    if (AtomicCas(&SlowPoolsMask, slow | (1ull << pool), slow)) { // set pool-is-slow flag
                        return;
                    }
                } else {
                    Y_FAIL("two slow-workers executing the same pool on the same core");
                    return; // pool is already slow
                }
            }
        }

        bool TryBeginHardPreemption(TLease lease) {
            return AtomicCas(&CurrentLease, HardPreemptionLease, lease);
        }

        void EndHardPreemption(TWorkerId to) {
            ATOMIC_COMPILER_BARRIER();
            if (!AtomicCas(&CurrentLease, TLease(to, NeverExpire), HardPreemptionLease)) {
                Y_FAIL("hard preemption failed");
            }
        }

        bool DisablePreemptionAndTryExtend(TLease lease) {
            return AtomicCas(&CurrentLease, lease.NeverExpire(), lease);
        }

        void EnablePreemptionAndGrant(TLease lease) {
            ATOMIC_COMPILER_BARRIER();
            if (!AtomicCas(&CurrentLease, lease, lease.NeverExpire())) {
                Y_FAIL("lease grant failed");
            }
        }

        void FastWorkerSleep(ui64 deadlineTs) {
            while (true) {
                TPoolsMask slow = AtomicLoad(&SlowPoolsMask);
                if ((slow & WaitPoolsFlag) == 0) {
                    return; // woken by WakeFast action
                }
                ui64 ts = GetCycleCountFast();
                if (deadlineTs <= ts) {
                    if (AtomicCas(&SlowPoolsMask, slow & ~WaitPoolsFlag, slow)) { // try reset wait flag
                        return; // wait flag has been successfully reset after timeout
                    }
                } else { // should wait
                    ui64 timeoutNs = Ts2Ns(deadlineTs - ts);
#ifdef _linux_
                    timespec timeout;
                    timeout.tv_sec = timeoutNs / 1'000'000'000;
                    timeout.tv_nsec = timeoutNs % 1'000'000'000;
                    SysFutex(FastWorkerFutex(), FUTEX_WAIT_PRIVATE, FastWorkerFutexValue(slow), &timeout, nullptr, 0);
#else
                    NanoSleep(timeoutNs); // non-linux wake is not supported, cpu will go idle on slow -> fast switch
#endif
                }
            }
        }

        void WakeFastWorker() {
#ifdef _linux_
            SysFutex(FastWorkerFutex(), FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
#endif
        }

#ifdef _linux_
        ui32* FastWorkerFutex() {
            // Actually we wait on one highest bit, but futex value size is 4 bytes on all platforms
            static_assert(sizeof(TPoolsMask) >= 4, "cannot be used as futex value on linux");
            return (ui32*)&SlowPoolsMask + 1; // higher 32 bits (little endian assumed)
        }

        ui32 FastWorkerFutexValue(TPoolsMask slow) {
            return ui32(slow >> 32); // higher 32 bits
        }
#endif

        void SetPriority(TThreadId tid, EPriority priority) {
            if (Config.NoRealtime) {
                return;
            }
#ifdef _linux_
            int policy;
            struct sched_param param;
            switch (priority) {
            case IdlePriority:
                policy = SCHED_FIFO;
                param.sched_priority = Config.IdleWorkerPriority;
                break;
            case FastPriority:
                policy = SCHED_FIFO;
                param.sched_priority = Config.FastWorkerPriority;
                break;
            case SlowPriority:
                policy = SCHED_OTHER;
                param.sched_priority = 0;
                break;
            }
            int ret = sched_setscheduler(tid, policy, &param);
            switch (ret) {
                case 0: return;
                case EINVAL:
                    Y_FAIL("sched_setscheduler(%" PRIu64 ", %d, %d) -> EINVAL", tid, policy, param.sched_priority);
                case EPERM:
                    // Requirements:
                    //  * CAP_SYS_NICE capability to run real-time processes and set cpu affinity.
                    //    Either run under root or set application capabilities:
                    //       sudo setcap cap_sys_nice=eip BINARY
                    //  * Non-zero rt-runtime (in case cgroups are used).
                    //    Either (a) disable global limit on RT processes bandwidth:
                    //       sudo sysctl -w kernel.sched_rt_runtime_us=-1
                    //    Or (b) set non-zero rt-runtime for your cgroup:
                    //       echo -1 > /sys/fs/cgroup/cpu/[cgroup]/cpu.rt_runtime_us
                    //       (also set the same value for every parent cgroup)
                    //    https://www.kernel.org/doc/Documentation/scheduler/sched-rt-group.txt
                    Y_FAIL("sched_setscheduler(%" PRIu64 ", %d, %d) -> EPERM", tid, policy, param.sched_priority);
                case ESRCH:
                    Y_FAIL("sched_setscheduler(%" PRIu64 ", %d, %d) -> ESRCH", tid, policy, param.sched_priority);
                default:
                    Y_FAIL("sched_setscheduler(%" PRIu64 ", %d, %d) -> %d", tid, policy, param.sched_priority, ret);
            }
#else
            Y_UNUSED(tid);
            Y_UNUSED(priority);
#endif
        }

        void ResetPreemption(TWorkerId fastWorkerId, ui64 ts) {
            if (Y_UNLIKELY(!PreemptionTimer)) {
                return;
            }
            if (FastWorker == fastWorkerId && HardPreemptionTs > 0) {
                PreemptionTimer->Reset();
                LWPROBE(ResetPreemptionTimer, Config.CpuId, FastWorker, PreemptionTimer->Fd, Ts2Ms(ts), Ts2Ms(HardPreemptionTs));
                HardPreemptionTs = 0;
            }
        }

        TLease PostponePreemption(TWorkerId fastWorkerId, ui64 ts) {
            // Select new timer after hard preemption
            if (FastWorker != fastWorkerId) {
                FastWorker = fastWorkerId;
                PreemptionTimer = IdleQueue.Dequeue();
                HardPreemptionTs = 0;
            }

            ui64 hardPreemptionTs = ts + Config.HardLimitTs;
            if (hardPreemptionTs > HardPreemptionTs) {
                // Reset timer (at most once in TickIntervalTs, sacrifice precision)
                HardPreemptionTs = hardPreemptionTs + Config.LimitPrecisionTs;
                PreemptionTimer->Set(HardPreemptionTs);
                LWPROBE(SetPreemptionTimer, Config.CpuId, FastWorker, PreemptionTimer->Fd, Ts2Ms(ts), Ts2Ms(HardPreemptionTs));
            }

            return TLease(fastWorkerId, hardPreemptionTs);
        }
    };

    // Proxy for start and switching TUnitedExecutorPool-s on single cpu via GetReadyActivation()
    // (does not implement any other method in IExecutorPool)
    class TCpuExecutorPool: public IExecutorPool {
        const TString Name;

    public:
        explicit TCpuExecutorPool(const TString& name)
            : IExecutorPool(MaxPools)
            , Name(name)
        {}

        TString GetName() const override {
            return Name;
        }

        void SetRealTimeMode() const override {
            // derived classes controls rt-priority - do nothing
        }

        // Should never be called
        void ReclaimMailbox(TMailboxType::EType, ui32, TWorkerId, ui64) override { Y_FAIL(); }
        TMailboxHeader *ResolveMailbox(ui32) override { Y_FAIL(); }
        void Schedule(TInstant, TAutoPtr<IEventHandle>, ISchedulerCookie*, TWorkerId) override { Y_FAIL(); }
        void Schedule(TMonotonic, TAutoPtr<IEventHandle>, ISchedulerCookie*, TWorkerId) override { Y_FAIL(); }
        void Schedule(TDuration, TAutoPtr<IEventHandle>, ISchedulerCookie*, TWorkerId) override { Y_FAIL(); }
        bool Send(TAutoPtr<IEventHandle>&) override { Y_FAIL(); }
        bool SpecificSend(TAutoPtr<IEventHandle>&) override { Y_FAIL(); }
        void ScheduleActivation(ui32) override { Y_FAIL(); }
        void SpecificScheduleActivation(ui32) override { Y_FAIL(); }
        void ScheduleActivationEx(ui32, ui64) override { Y_FAIL(); }
        TActorId Register(IActor*, TMailboxType::EType, ui64, const TActorId&) override { Y_FAIL(); }
        TActorId Register(IActor*, TMailboxHeader*, ui32, const TActorId&) override { Y_FAIL(); }
        void Prepare(TActorSystem*, NSchedulerQueue::TReader**, ui32*) override { Y_FAIL(); }
        void Start() override { Y_FAIL(); }
        void PrepareStop() override { Y_FAIL(); }
        void Shutdown() override { Y_FAIL(); }
        bool Cleanup() override { Y_FAIL(); }
    };

    // Proxy executor pool working with cpu-local scheduler (aka actorsystem 2.0)
    class TSharedCpuExecutorPool: public TCpuExecutorPool {
        TSharedCpu* Local;
        TIntrusivePtr<TAffinity> SingleCpuAffinity; // no migration support yet
    public:
        explicit TSharedCpuExecutorPool(TSharedCpu* local, const TUnitedWorkersConfig& config)
            : TCpuExecutorPool("u-" + ToString(local->Config.CpuId))
            , Local(local)
            , SingleCpuAffinity(config.NoAffinity ? nullptr : new TAffinity(TCpuMask(local->Config.CpuId)))
        {}

        TAffinity* Affinity() const override {
            return SingleCpuAffinity.Get();
        }

        ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) override {
            return Local->GetReadyActivation(wctx, revolvingCounter);
        }
    };

    // Proxy executor pool working with balancer and assigned pools (aka actorsystem 1.5)
    class TAssignedCpuExecutorPool: public TCpuExecutorPool {
        TAssignedCpu* Local;
        TIntrusivePtr<TAffinity> CpuAffinity;
    public:
        explicit TAssignedCpuExecutorPool(TAssignedCpu* local, const TUnitedWorkersConfig& config)
            : TCpuExecutorPool("United")
            , Local(local)
            , CpuAffinity(config.NoAffinity ? nullptr : new TAffinity(config.Allowed))
        {}

        TAffinity* Affinity() const override {
            return CpuAffinity.Get();
        }

        ui32 GetReadyActivation(TWorkerContext& wctx, ui64 revolvingCounter) override {
            return Local->GetReadyActivation(wctx, revolvingCounter);
        }
    };

    // Representation of a single cpu and it's state visible to other cpus and pools
    struct TUnitedWorkers::TCpu: public TNonCopyable {
        struct TScopedWaiters {
            TCpu& Cpu;
            TPool* AssignedPool; // nullptr if CpuShared

            // Subscribe on wakeups from allowed pools
            TScopedWaiters(TCpu& cpu, TPool* assignedPool) : Cpu(cpu), AssignedPool(assignedPool) {
                if (!AssignedPool) {
                    for (TPool* pool : Cpu.AllowedPools) {
                        AtomicIncrement(pool->Waiters);
                    }
                } else {
                    AtomicIncrement(AssignedPool->Waiters);
                }
            }

            // Unsubscribe from pools we've subscribed on
            ~TScopedWaiters() {
                if (!AssignedPool) {
                    for (TPool* pool : Cpu.AllowedPools) {
                        AtomicDecrement(pool->Waiters);
                    }
                } else {
                    AtomicDecrement(AssignedPool->Waiters);
                }
            }
        };

        // Current cpu state important for other cpus and balancer
        TCpuState State;

        // Thread-safe per pool stats
        // NOTE: It's guaranteed that cpu never executes two instance of the same pool
        TVector<TExecutorThreadStats> PoolStats;
        TCpuLoadLog<1024> LoadLog;


        // Configuration
        TCpuId CpuId;
        THolder<TCpuLocalManager> LocalManager;
        THolder<TCpuExecutorPool> ExecutorPool;

        // Pools allowed to run on this cpu
        TStackVec<TPool*, 15> AllowedPools;

        void Stop() {
            if (LocalManager) {
                State.Stop();
                LocalManager->Stop();
            }
        }

        bool StartSpinning(TUnitedWorkers* united, TPool* assignedPool, TPoolId& result) {
            // Mark cpu as idle
            if (Y_UNLIKELY(!State.StartSpinning())) {
                result = CpuStopped;
                return true;
            }

            // Avoid using multiple atomic seq_cst loads in cycle, use barrier once and relaxed ops
            AtomicBarrier();

            // Check there is no pending tokens (can be released before Waiters increment)
            if (!assignedPool) {
                for (TPool* pool : AllowedPools) {
                    if (pool->TryAcquireTokenRelaxed()) {
                        result = WakeWithTokenAcquired(united, pool->PoolId);
                        return true; // token acquired or stop
                    }
                }
            } else {
                if (assignedPool->TryAcquireTokenRelaxed()) {
                    result = WakeWithTokenAcquired(united, assignedPool->PoolId);
                    return true; // token acquired or stop
                }
            }

            // At this point we can be sure wakeup won't be lost
            // So we can actively spin or block w/o checking for pending tokens
            return false;
        }

        bool ActiveWait(ui64 spinThresholdTs, TPoolId& result) {
            ui64 ts = GetCycleCountFast();
            LoadLog.RegisterBusyPeriod(ts);
            ui64 deadline = ts + spinThresholdTs;
            while (GetCycleCountFast() < deadline) {
                for (ui32 i = 0; i < 12; ++i) {
                    TPoolId current = State.CurrentPool();
                    if (current == CpuSpinning) {
                        SpinLockPause();
                    } else {
                        result = current;
                        LoadLog.RegisterIdlePeriod(GetCycleCountFast());
                        return true; // wakeup
                    }
                }
            }
            return false; // spin threshold exceeded, no wakeups
        }

        bool StartBlocking(TPoolId& result) {
            // Switch into blocked state
            if (State.StartBlocking()) {
                result = State.CurrentPool();
                return true;
            } else {
                return false;
            }
        }

        bool BlockedWait(TPoolId& result, ui64 timeoutNs) {
            return State.Block(timeoutNs, result);
        }

        void SwitchPool(TPoolId pool) {
            return State.SwitchPool(pool);
        }

    private:
        TPoolId WakeWithTokenAcquired(TUnitedWorkers* united, TPoolId token) {
            switch (State.WakeWithTokenAcquired(token)) {
            case TCpuState::Woken: // we've got token and successfully woken up this cpu
                // NOTE: sending thread may also wakeup another worker, which wont be able to acquire token and will go idle (it's ok)
                return token;
            case TCpuState::NotIdle: { // wakeup event has also occured
                TPoolId wakeup = State.CurrentPool();
                if (wakeup != token) { // token and wakeup for different pools
                    united->TryWake(wakeup); // rewake another cpu to avoid losing wakeup
                }
                return token;
            }
            case TCpuState::Forbidden:
                Y_FAIL();
            case TCpuState::Stopped:
                return CpuStopped;
            }
        }
    };

    TUnitedWorkers::TUnitedWorkers(
            const TUnitedWorkersConfig& config,
            const TVector<TUnitedExecutorPoolConfig>& unitedPools,
            const TCpuAllocationConfig& allocation,
            IBalancer* balancer)
        : Balancer(balancer)
        , Config(config)
        , Allocation(allocation)
    {
        // Find max pool id and initialize pools
        PoolCount = 0;
        for (const TCpuAllocation& cpuAlloc : allocation.Items) {
            for (const auto& pa : cpuAlloc.AllowedPools) {
                PoolCount = Max<size_t>(PoolCount, pa.PoolId + 1);
            }
        }
        Pools.Reset(new TPool[PoolCount]);

        // Find max cpu id and initialize cpus
        CpuCount = 0;
        for (const TCpuAllocation& cpuAlloc : allocation.Items) {
            CpuCount = Max<size_t>(CpuCount, cpuAlloc.CpuId + 1);
        }
        Cpus.Reset(new TCpu[CpuCount]);

        // Setup allocated cpus
        // NOTE: leave gaps for not allocated cpus (default-initialized)
        WorkerCount = 0;
        for (const TCpuAllocation& cpuAlloc : allocation.Items) {
            TCpu& cpu = Cpus[cpuAlloc.CpuId];
            cpu.CpuId = cpuAlloc.CpuId;
            cpu.PoolStats.resize(PoolCount); // NOTE: also may have gaps
            for (const auto& pa : cpuAlloc.AllowedPools) {
                cpu.AllowedPools.emplace_back(&Pools[pa.PoolId]);
            }

            // Setup balancing and cpu-local manager
            if (!Balancer->AddCpu(cpuAlloc, &cpu.State)) {
                cpu.State.SwitchPool(0); // set initial state to non-idle to avoid losing wakeups on start
                cpu.State.AssignPool(CpuShared);
                TSharedCpu* local = new TSharedCpu(TSharedCpu::TConfig(cpuAlloc, Config), this);
                cpu.LocalManager.Reset(local);
                cpu.ExecutorPool.Reset(new TSharedCpuExecutorPool(local, Config));
            } else {
                TAssignedCpu* local = new TAssignedCpu(this);
                cpu.LocalManager.Reset(local);
                cpu.ExecutorPool.Reset(new TAssignedCpuExecutorPool(local, Config));
            }
            WorkerCount += cpu.LocalManager->WorkerCount();
        }

        // Initialize workers
        Workers.Reset(new TWorker[WorkerCount]);

        // Setup pools
        // NOTE: leave gaps for not united pools (default-initialized)
        for (const TUnitedExecutorPoolConfig& cfg : unitedPools) {
            TPool& pool = Pools[cfg.PoolId];
            Y_VERIFY(cfg.PoolId < MaxPools);
            pool.PoolId = cfg.PoolId;
            pool.Concurrency = cfg.Concurrency ? cfg.Concurrency : Config.CpuCount;
            pool.ExecutorPool = nullptr; // should be set later using SetupPool()
            pool.MailboxTable = nullptr; // should be set later using SetupPool()
            pool.TimePerMailboxTs = DurationToCycles(cfg.TimePerMailbox);
            pool.EventsPerMailbox = cfg.EventsPerMailbox;

            // Reinitialize per cpu pool stats with right MaxActivityType
            for (const TCpuAllocation& cpuAlloc : allocation.Items) {
                TCpu& cpu = Cpus[cpuAlloc.CpuId];
                cpu.PoolStats[cfg.PoolId] = TExecutorThreadStats();
            }

            // Setup WakeOrderCpus: left to right exclusive cpus, then left to right shared cpus.
            // Waking exclusive cpus first reduce load on shared cpu and improve latency isolation, which is
            // the point of using exclusive cpu. But note that number of actively spinning idle cpus may increase,
            // so cpu consumption on light load is higher.
            for (const TCpuAllocation& cpuAlloc : allocation.Items) {
                TCpu& cpu = Cpus[cpuAlloc.CpuId];
                if (cpu.AllowedPools.size() == 1 && cpu.AllowedPools[0] == &pool) {
                    pool.WakeOrderCpus.emplace_back(&cpu);
                }
            }
            for (const TCpuAllocation& cpuAlloc : allocation.Items) {
                TCpu& cpu = Cpus[cpuAlloc.CpuId];
                if (cpu.AllowedPools.size() > 1 && cpuAlloc.HasPool(pool.PoolId)) {
                    pool.WakeOrderCpus.emplace_back(&cpu);
                }
            }
        }
    }

    TUnitedWorkers::~TUnitedWorkers() {
    }

    void TUnitedWorkers::Prepare(TActorSystem* actorSystem, TVector<NSchedulerQueue::TReader*>& scheduleReaders) {
        // Setup allocated cpus
        // NOTE: leave gaps for not allocated cpus (default-initialized)
        TWorkerId workers = 0;
        for (TCpuId cpuId = 0; cpuId < CpuCount; cpuId++) {
            TCpu& cpu = Cpus[cpuId];

            // Setup cpu-local workers
            if (cpu.LocalManager) {
                for (i16 i = 0; i < cpu.LocalManager->WorkerCount(); i++) {
                    TWorkerId workerId = workers++;
                    cpu.LocalManager->AddWorker(workerId);

                    // Setup worker
                    Y_VERIFY(workerId < WorkerCount);
                    Workers[workerId].Thread.Reset(new TExecutorThread(
                        workerId,
                        cpu.CpuId,
                        actorSystem,
                        cpu.ExecutorPool.Get(), // use cpu-local manager as proxy executor for all workers on cpu
                        nullptr, // MailboxTable is pool-specific, will be set on pool switch
                        cpu.ExecutorPool->GetName()));
                    // NOTE: TWorker::ThreadId will be initialized after in Start()

                    scheduleReaders.push_back(&Workers[workerId].SchedulerQueue.Reader);
                }
            }
        }
    }

    void TUnitedWorkers::Start() {
        for (TWorkerId workerId = 0; workerId < WorkerCount; workerId++) {
            Workers[workerId].Thread->Start();
        }
        for (TWorkerId workerId = 0; workerId < WorkerCount; workerId++) {
            AtomicStore(&Workers[workerId].ThreadId, Workers[workerId].Thread->GetThreadId());
        }
    }

    inline TThreadId TUnitedWorkers::GetWorkerThreadId(TWorkerId workerId) const {
        volatile TThreadId* threadId = &Workers[workerId].ThreadId;
#ifdef _linux_
        while (AtomicLoad(threadId) == UnknownThreadId) {
            NanoSleep(1000);
        }
#endif
        return AtomicLoad(threadId);
    }

    inline NSchedulerQueue::TWriter* TUnitedWorkers::GetScheduleWriter(TWorkerId workerId) const {
        return &Workers[workerId].SchedulerQueue.Writer;
    }

    void TUnitedWorkers::SetupPool(TPoolId pool, IExecutorPool* executorPool, TMailboxTable* mailboxTable) {
        Pools[pool].ExecutorPool = executorPool;
        Pools[pool].MailboxTable = mailboxTable;
    }

    void TUnitedWorkers::PrepareStop() {
        AtomicStore(&StopFlag, true);
        for (TPoolId pool = 0; pool < PoolCount; pool++) {
            Pools[pool].Stop();
        }
        for (TCpuId cpuId = 0; cpuId < CpuCount; cpuId++) {
            Cpus[cpuId].Stop();
        }
    }

    void TUnitedWorkers::Shutdown() {
        for (TWorkerId workerId = 0; workerId < WorkerCount; workerId++) {
            Workers[workerId].Thread->Join();
        }
    }

    inline void TUnitedWorkers::PushActivation(TPoolId pool, ui32 activation, ui64 revolvingCounter) {
        if (Pools[pool].PushActivation(activation, revolvingCounter)) { // token generated
            TryWake(pool);
        }
    }

    inline bool TUnitedWorkers::TryAcquireToken(TPoolId pool) {
        return Pools[pool].TryAcquireToken();
    }

    inline void TUnitedWorkers::TryWake(TPoolId pool) {
        // Avoid using multiple atomic seq_cst loads in cycle, use barrier once
        AtomicBarrier();

        // Scan every allowed cpu in pool's wakeup order and try to wake the first idle cpu
        if (RelaxedLoad(&Pools[pool].Waiters) > 0) {
            for (TCpu* cpu : Pools[pool].WakeOrderCpus) {
                if (cpu->State.WakeWithoutToken(pool) == TCpuState::Woken) {
                    return; // successful wake up
                }
            }
        }

        // Cpu has not been woken up
    }

    inline void TUnitedWorkers::BeginExecution(TPoolId pool, ui32& activation, ui64 revolvingCounter) {
        Pools[pool].BeginExecution(activation, revolvingCounter);
    }

    inline bool TUnitedWorkers::NextExecution(TPoolId pool, ui32& activation, ui64 revolvingCounter) {
        return Pools[pool].NextExecution(activation, revolvingCounter);
    }

    inline void TUnitedWorkers::StopExecution(TPoolId pool) {
        if (Pools[pool].StopExecution()) { // pending token
            TryWake(pool);
        }
    }

    inline void TUnitedWorkers::Balance() {
        ui64 ts = GetCycleCountFast();
        if (Balancer->TryLock(ts)) {
            for (TPoolId pool = 0; pool < PoolCount; pool++) {
                if (Pools[pool].IsUnited()) {
                    ui64 ElapsedTs = 0;
                    ui64 ParkedTs = 0;
                    TStackVec<TCpuLoadLog<1024>*, 128> logs;
                    ui64 worstActivationTimeUs = 0;
                    for (TCpu* cpu : Pools[pool].WakeOrderCpus) {
                        TExecutorThreadStats& cpuStats = cpu->PoolStats[pool];
                        ElapsedTs += cpuStats.ElapsedTicks;
                        ParkedTs += cpuStats.ParkedTicks;
                        worstActivationTimeUs = Max(worstActivationTimeUs, cpuStats.WorstActivationTimeUs);
                        AtomicStore<decltype(cpuStats.WorstActivationTimeUs)>(&cpuStats.WorstActivationTimeUs, 0ul);
                        logs.push_back(&cpu->LoadLog);
                    }
                    ui64 minPeriodTs = Min(ui64(Us2Ts(Balancer->GetPeriodUs())), ui64((1024ull-2ull)*64ull*128ull*1024ull));
                    ui64 estimatedTs = MinusOneCpuEstimator.MaxLatencyIncreaseWithOneLessCpu(
                            &logs[0], logs.size(), ts, minPeriodTs);
                    TBalancerStats stats;
                    stats.Ts = ts;
                    stats.CpuUs = Ts2Us(ElapsedTs);
                    stats.IdleUs = Ts2Us(ParkedTs);
                    stats.ExpectedLatencyIncreaseUs = Ts2Us(estimatedTs);
                    stats.WorstActivationTimeUs = worstActivationTimeUs;
                    Balancer->SetPoolStats(pool, stats);
                }
            }
            Balancer->Balance();
            Balancer->Unlock();
        }
    }

    inline TPoolId TUnitedWorkers::AssignedPool(TWorkerContext& wctx) {
        return Cpus[wctx.CpuId].State.AssignedPool();
    }

    inline bool TUnitedWorkers::IsPoolReassigned(TWorkerContext& wctx) {
        return Cpus[wctx.CpuId].State.IsPoolReassigned(wctx.PoolId);
    }

    inline void TUnitedWorkers::SwitchPool(TWorkerContext& wctx, ui64 softDeadlineTs) {
        Pools[wctx.PoolId].Switch(wctx, softDeadlineTs, Cpus[wctx.CpuId].PoolStats[wctx.PoolId]);
        Cpus[wctx.CpuId].SwitchPool(wctx.PoolId);
    }

    TPoolId TUnitedWorkers::Idle(TPoolId assigned, TWorkerContext& wctx) {
        wctx.SwitchToIdle();

        TPoolId result;
        TTimeTracker timeTracker;
        TCpu& cpu = Cpus[wctx.CpuId];
        TPool* assignedPool = assigned == CpuShared ? nullptr : &Pools[assigned];
        TCpu::TScopedWaiters scopedWaiters(cpu, assignedPool);
        while (true) {
            if (cpu.StartSpinning(this, assignedPool, result)) {
                break; // token already acquired (or stop)
            }
            result = WaitSequence(cpu, wctx, timeTracker);
            if (Y_UNLIKELY(result == CpuStopped) || TryAcquireToken(result)) {
                break; // token acquired (or stop)
            }
        }

        wctx.AddElapsedCycles(ActorSystemIndex, timeTracker.Elapsed());
        return result;
    }

    TPoolId TUnitedWorkers::WaitSequence(TCpu& cpu, TWorkerContext& wctx, TTimeTracker& timeTracker) {
        TPoolId result;
        if (cpu.ActiveWait(Us2Ts(Config.SpinThresholdUs), result)) {
            wctx.AddElapsedCycles(ActorSystemIndex, timeTracker.Elapsed());
            return result;
        }
        if (cpu.StartBlocking(result)) {
            wctx.AddElapsedCycles(ActorSystemIndex, timeTracker.Elapsed());
            return result;
        }
        wctx.AddElapsedCycles(ActorSystemIndex, timeTracker.Elapsed());
        cpu.LoadLog.RegisterBusyPeriod(GetCycleCountFast());
        bool wakeup;
        do {
            wakeup = cpu.BlockedWait(result, Config.Balancer.PeriodUs * 1000);
            wctx.AddParkedCycles(timeTracker.Elapsed());
        } while (!wakeup);
        cpu.LoadLog.RegisterIdlePeriod(GetCycleCountFast());
        return result;
    }

    void TUnitedWorkers::GetCurrentStats(TPoolId pool, TVector<TExecutorThreadStats>& statsCopy) const {
        size_t idx = 1;
        statsCopy.resize(idx + Pools[pool].WakeOrderCpus.size());
        for (TCpu* cpu : Pools[pool].WakeOrderCpus) {
            TExecutorThreadStats& s = statsCopy[idx++];
            s = TExecutorThreadStats();
            s.Aggregate(cpu->PoolStats[pool]);
        }
    }

    TUnitedExecutorPool::TUnitedExecutorPool(const TUnitedExecutorPoolConfig& cfg, TUnitedWorkers* united)
        : TExecutorPoolBaseMailboxed(cfg.PoolId)
        , United(united)
        , PoolName(cfg.PoolName)
    {
        United->SetupPool(TPoolId(cfg.PoolId), this, MailboxTable.Get());
    }

    void TUnitedExecutorPool::Prepare(TActorSystem* actorSystem, NSchedulerQueue::TReader** scheduleReaders, ui32* scheduleSz) {
        ActorSystem = actorSystem;

        // Schedule readers are initialized through TUnitedWorkers::Prepare
        *scheduleReaders = nullptr;
        *scheduleSz = 0;
    }

    void TUnitedExecutorPool::Start() {
        // workers are actually started in TUnitedWorkers::Start()
    }

    void TUnitedExecutorPool::PrepareStop() {
    }

    void TUnitedExecutorPool::Shutdown() {
        // workers are actually joined in TUnitedWorkers::Shutdown()
    }

    TAffinity* TUnitedExecutorPool::Affinity() const {
        Y_FAIL(); // should never be called, TCpuExecutorPool is used instead
    }

    ui32 TUnitedExecutorPool::GetThreads() const {
        return 0;
    }

    ui32 TUnitedExecutorPool::GetReadyActivation(TWorkerContext&, ui64) {
        Y_FAIL(); // should never be called, TCpu*ExecutorPool is used instead
    }

    inline void TUnitedExecutorPool::ScheduleActivation(ui32 activation) {
        TUnitedExecutorPool::ScheduleActivationEx(activation, AtomicIncrement(ActivationsRevolvingCounter));
    }

    inline void TUnitedExecutorPool::SpecificScheduleActivation(ui32 activation) {
        TUnitedExecutorPool::ScheduleActivation(activation);
    }

    inline void TUnitedExecutorPool::ScheduleActivationEx(ui32 activation, ui64 revolvingCounter) {
        United->PushActivation(PoolId, activation, revolvingCounter);
    }

    void TUnitedExecutorPool::Schedule(TInstant deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        TUnitedExecutorPool::Schedule(deadline - ActorSystem->Timestamp(), ev, cookie, workerId);
    }

    void TUnitedExecutorPool::Schedule(TMonotonic deadline, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_VERIFY_DEBUG(workerId < United->GetWorkerCount());
        const auto current = ActorSystem->Monotonic();
        if (deadline < current) {
            deadline = current;
        }
        United->GetScheduleWriter(workerId)->Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    void TUnitedExecutorPool::Schedule(TDuration delta, TAutoPtr<IEventHandle> ev, ISchedulerCookie* cookie, TWorkerId workerId) {
        Y_VERIFY_DEBUG(workerId < United->GetWorkerCount());
        const auto deadline = ActorSystem->Monotonic() + delta;
        United->GetScheduleWriter(workerId)->Push(deadline.MicroSeconds(), ev.Release(), cookie);
    }

    void TUnitedExecutorPool::GetCurrentStats(TExecutorPoolStats& poolStats, TVector<TExecutorThreadStats>& statsCopy) const {
        Y_UNUSED(poolStats);
        if (statsCopy.empty()) {
            statsCopy.resize(1);
        }
        statsCopy[0] = TExecutorThreadStats();
        statsCopy[0].Aggregate(Stats);
        United->GetCurrentStats(PoolId, statsCopy);
    }
}
