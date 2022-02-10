#include "balancer.h" 
 
#include "probes.h" 
 
#include <library/cpp/actors/util/intrinsics.h> 
#include <library/cpp/actors/util/datetime.h> 
 
#include <util/system/spinlock.h> 
 
#include <algorithm> 
 
namespace NActors { 
    LWTRACE_USING(ACTORLIB_PROVIDER); 
 
    // Describes balancing-related state of pool, the most notable is `Importance` to add new cpu 
    struct TLevel { 
        // Balancer will try to give more cpu to overloaded pools 
        enum ELoadClass { 
            Underloaded = 0, 
            Moderate = 1, 
            Overloaded = 2, 
        }; 
 
        double ScaleFactor; 
        ELoadClass LoadClass; 
        ui64 Importance; // pool with lower importance is allowed to pass cpu to pool with higher, but the opposite is forbidden 
 
        TLevel() {} 
 
        TLevel(const TBalancingConfig& cfg, TPoolId poolId, ui64 currentCpus, double cpuIdle) { 
            ScaleFactor = double(currentCpus) / cfg.Cpus; 
            if (cpuIdle > 1.3) { // TODO: add a better underload criterion, based on estimated latency w/o 1 cpu 
                LoadClass = Underloaded; 
            } else if (cpuIdle < 0.2) { // TODO: add a better overload criterion, based on latency 
                LoadClass = Overloaded; 
            } else { 
                LoadClass = Moderate; 
            } 
            Importance = MakeImportance(LoadClass, cfg.Priority, ScaleFactor, cpuIdle, poolId); 
        } 
 
    private: 
        // Importance is simple ui64 value (from highest to lowest): 
        //   2 Bits: LoadClass 
        //   8 Bits: Priority 
        //  10 Bits: -ScaleFactor (for max-min fairness with weights equal to TBalancingConfig::Cpus) 
        //  10 Bits: -CpuIdle 
        //   6 Bits: PoolId 
        static ui64 MakeImportance(ELoadClass load, ui8 priority, double scaleFactor, double cpuIdle, TPoolId poolId) { 
            ui64 idle = std::clamp<i64>(1024 - cpuIdle * 512, 0, 1023); 
            ui64 scale = std::clamp<i64>(1024 - scaleFactor * 32, 0, 1023); 
 
            Y_VERIFY(ui64(load)     < (1ull << 2ull)); 
            Y_VERIFY(ui64(priority) < (1ull << 8ull)); 
            Y_VERIFY(ui64(scale)    < (1ull << 10ull)); 
            Y_VERIFY(ui64(idle)     < (1ull << 10ull)); 
            Y_VERIFY(ui64(poolId)   < (1ull << 6ull)); 
 
            static_assert(ui64(MaxPools) <= (1ull << 6ull)); 
 
            ui64 importance = 
                (ui64(load)     << ui64(6 + 10 + 10 + 8)) | 
                (ui64(priority) << ui64(6 + 10 + 10)) | 
                (ui64(scale)    << ui64(6 + 10)) | 
                (ui64(idle)     << ui64(6)) | 
                ui64(poolId); 
            return importance; 
        } 
    }; 
 
    // Main balancer implemenation 
    class TBalancer: public IBalancer { 
    private: 
        struct TCpu; 
        struct TPool; 
 
        bool Disabled = true; 
        TSpinLock Lock; 
        ui64 NextBalanceTs; 
        TVector<TCpu> Cpus; // Indexed by CpuId, can have gaps 
        TVector<TPool> Pools; // Indexed by PoolId, can have gaps 
        TBalancerConfig Config; 
 
    public: 
        // Setup 
        TBalancer(const TBalancerConfig& config, const TVector<TUnitedExecutorPoolConfig>& unitedPools, ui64 ts); 
        bool AddCpu(const TCpuAllocation& cpuAlloc, TCpuState* cpu) override; 
        ~TBalancer(); 
 
        // Balancing 
        bool TryLock(ui64 ts) override; 
        void SetPoolStats(TPoolId pool, const TBalancerStats& stats) override; 
        void Balance() override; 
        void Unlock() override; 
 
    private: 
        void MoveCpu(TPool& from, TPool& to); 
    }; 
 
    struct TBalancer::TPool { 
        TBalancingConfig Config; 
        TPoolId PoolId; 
        TString PoolName; 
 
        // Input data for balancing 
        TBalancerStats Prev; 
        TBalancerStats Next; 
 
        // Derived stats 
        double CpuLoad; 
        double CpuIdle; 
 
        // Classification 
        // NOTE: We want to avoid passing cpu back and forth, so we must consider not only current level, 
        // NOTE: but expected levels after movements also 
        TLevel CurLevel; // Level with current amount of cpu 
        TLevel AddLevel; // Level after one cpu acception 
        TLevel SubLevel; // Level after one cpu donation 
 
        // Balancing state 
        ui64 CurrentCpus = 0; // Total number of cpus assigned for this pool (zero means pools is not balanced) 
        ui64 PrevCpus = 0; // Cpus in last period 
 
        explicit TPool(const TBalancingConfig& cfg = {}) 
            : Config(cfg) 
        {} 
 
        void Configure(const TBalancingConfig& cfg, const TString& poolName) { 
            Config = cfg; 
            // Enforce constraints 
            Config.MinCpus = std::clamp<ui32>(Config.MinCpus, 1, Config.Cpus); 
            Config.MaxCpus = Max<ui32>(Config.MaxCpus, Config.Cpus); 
            PoolName = poolName; 
        } 
    }; 
 
    struct TBalancer::TCpu { 
        TCpuState* State = nullptr; // Cpu state, nullptr means cpu is not used (gap) 
        TCpuAllocation Alloc; 
        TPoolId Current; 
        TPoolId Assigned; 
    }; 
 
    TBalancer::TBalancer(const TBalancerConfig& config, const TVector<TUnitedExecutorPoolConfig>& unitedPools, ui64 ts) 
        : NextBalanceTs(ts) 
        , Config(config) 
    { 
        for (TPoolId pool = 0; pool < MaxPools; pool++) { 
            Pools.emplace_back(); 
            Pools.back().PoolId = pool; 
        } 
        for (const TUnitedExecutorPoolConfig& united : unitedPools) { 
            Pools[united.PoolId].Configure(united.Balancing, united.PoolName); 
        } 
    } 
 
    TBalancer::~TBalancer() { 
    } 
 
    bool TBalancer::AddCpu(const TCpuAllocation& cpuAlloc, TCpuState* state) { 
        // Setup 
        TCpuId cpuId = cpuAlloc.CpuId; 
        if (Cpus.size() <= cpuId) { 
            Cpus.resize(cpuId + 1); 
        } 
        TCpu& cpu = Cpus[cpuId]; 
        cpu.State = state; 
        cpu.Alloc = cpuAlloc; 
 
        // Fill every pool with cpus up to TBalancingConfig::Cpus 
        TPoolId pool = 0; 
        for (TPool& p : Pools) { 
            if (p.CurrentCpus < p.Config.Cpus) { 
                p.CurrentCpus++; 
                break; 
            } 
            pool++; 
        } 
        if (pool != MaxPools) { // cpu under balancer control 
            state->SwitchPool(pool); 
            state->AssignPool(pool); 
            Disabled = false; 
            return true; 
        } 
        return false; // non-balanced cpu 
    } 
 
    bool TBalancer::TryLock(ui64 ts) { 
        if (!Disabled && NextBalanceTs < ts && Lock.TryAcquire()) { 
            NextBalanceTs = ts + Us2Ts(Config.PeriodUs); 
            return true; 
        } 
        return false; 
    } 
 
    void TBalancer::SetPoolStats(TPoolId pool, const TBalancerStats& stats) { 
        Y_VERIFY(pool < MaxPools); 
        TPool& p = Pools[pool]; 
        p.Prev = p.Next; 
        p.Next = stats; 
    } 
 
    void TBalancer::Balance() { 
        // Update every cpu state 
        for (TCpu& cpu : Cpus) { 
            if (cpu.State) { 
                cpu.State->Load(cpu.Assigned, cpu.Current); 
                if (cpu.Current < MaxPools && cpu.Current != cpu.Assigned) { 
                    return; // previous movement has not been applied yet, wait 
                } 
            } 
        } 
 
        // Process stats, classify and compute pool importance 
        TStackVec<TPool*, MaxPools> order; 
        for (TPool& pool : Pools) { 
            if (pool.Config.Cpus == 0) { 
                continue; // skip gaps (non-existent or non-united pools) 
            } 
            if (pool.Prev.Ts == 0 || pool.Prev.Ts >= pool.Next.Ts) { 
                return; // invalid stats 
            } 
 
            // Compute derived stats 
            pool.CpuLoad = (pool.Next.CpuUs - pool.Prev.CpuUs) / Ts2Us(pool.Next.Ts - pool.Prev.Ts); 
            if (pool.Prev.IdleUs == ui64(-1) || pool.Next.IdleUs == ui64(-1)) { 
                pool.CpuIdle = pool.CurrentCpus - pool.CpuLoad; // for tests 
            } else { 
                pool.CpuIdle = (pool.Next.IdleUs - pool.Prev.IdleUs) / Ts2Us(pool.Next.Ts - pool.Prev.Ts); 
            } 
 
            // Compute levels 
            pool.CurLevel = TLevel(pool.Config, pool.PoolId, pool.CurrentCpus, pool.CpuIdle); 
            pool.AddLevel = TLevel(pool.Config, pool.PoolId, pool.CurrentCpus + 1, pool.CpuIdle); // we expect taken cpu to became utilized 
            pool.SubLevel = TLevel(pool.Config, pool.PoolId, pool.CurrentCpus - 1, pool.CpuIdle - 1); 
 
            // Prepare for balancing 
            pool.PrevCpus = pool.CurrentCpus; 
            order.push_back(&pool); 
        } 
 
        // Sort pools by importance 
        std::sort(order.begin(), order.end(), [] (TPool* l, TPool* r) {return l->CurLevel.Importance < r->CurLevel.Importance; }); 
        for (TPool* pool : order) { 
            LWPROBE(PoolStats, pool->PoolId, pool->PoolName, pool->CurrentCpus, pool->CurLevel.LoadClass, pool->Config.Priority, pool->CurLevel.ScaleFactor, pool->CpuIdle, pool->CpuLoad, pool->CurLevel.Importance, pool->AddLevel.Importance, pool->SubLevel.Importance); 
        } 
 
        // Move cpus from lower importance to higher importance pools 
        for (auto toIter = order.rbegin(); toIter != order.rend(); ++toIter) { 
            TPool& to = **toIter; 
            if (to.CurLevel.LoadClass == TLevel::Overloaded && // if pool is overloaded 
                to.CurrentCpus < to.Config.MaxCpus) // and constraints would not be violated 
            { 
                for (auto fromIter = order.begin(); (*fromIter)->CurLevel.Importance < to.CurLevel.Importance; ++fromIter) { 
                    TPool& from = **fromIter; 
                    if (from.CurrentCpus == from.PrevCpus && // if not balanced yet 
                        from.CurrentCpus > from.Config.MinCpus && // and constraints would not be violated 
                        from.SubLevel.Importance < to.AddLevel.Importance) // and which of two pools is more important would not change after cpu movement 
                    { 
                        MoveCpu(from, to); 
                        from.CurrentCpus--; 
                        to.CurrentCpus++; 
                        break; 
                    } 
                } 
            } 
        } 
    } 
 
    void TBalancer::MoveCpu(TBalancer::TPool& from, TBalancer::TPool& to) { 
        for (auto ci = Cpus.rbegin(), ce = Cpus.rend(); ci != ce; ci++) { 
            TCpu& cpu = *ci; 
            if (!cpu.State) { 
                continue; 
            } 
            if (cpu.Assigned == from.PoolId) { 
                cpu.State->AssignPool(to.PoolId); 
                cpu.Assigned = to.PoolId; 
                LWPROBE(MoveCpu, from.PoolId, to.PoolId, from.PoolName, to.PoolName, cpu.Alloc.CpuId); 
                return; 
            } 
        } 
        Y_FAIL(); 
    } 
 
    void TBalancer::Unlock() { 
        Lock.Release(); 
    } 
 
    IBalancer* MakeBalancer(const TBalancerConfig& config, const TVector<TUnitedExecutorPoolConfig>& unitedPools, ui64 ts) { 
        return new TBalancer(config, unitedPools, ts); 
    } 
} 
