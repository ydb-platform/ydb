#include "balancer.h"

#include <library/cpp/actors/util/datetime.h>
#include <library/cpp/lwtrace/all.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(PoolCpuBalancer) {
    struct TTest {
        TCpuManagerConfig Config;
        TCpuMask Available;
        THolder<IBalancer> Balancer;
        TVector<TCpuState> CpuStates;
        TVector<ui64> CpuUs;
        ui64 Now = 0;

        void SetCpuCount(size_t count) {
            Config.UnitedWorkers.CpuCount = count;
            for (TCpuId cpuId = 0; cpuId < count; cpuId++) {
                Available.Set(cpuId);
            }
        }

        void AddPool(ui32 minCpus, ui32 cpus, ui32 maxCpus, ui8 priority = 0) {
            TUnitedExecutorPoolConfig u;
            u.PoolId = TPoolId(Config.United.size());
            u.Balancing.Cpus = cpus;
            u.Balancing.MinCpus = minCpus;
            u.Balancing.MaxCpus = maxCpus;
            u.Balancing.Priority = priority;
            Config.United.push_back(u);
        }

        void Start() {
            TCpuAllocationConfig allocation(Available, Config);
            Balancer.Reset(MakeBalancer(Config.UnitedWorkers.Balancer, Config.United, 0));
            CpuStates.resize(allocation.Items.size()); // do not resize it later to avoid dangling pointers
            CpuUs.resize(CpuStates.size());
            for (const TCpuAllocation& cpuAlloc : allocation.Items) {
                bool added = Balancer->AddCpu(cpuAlloc, &CpuStates[cpuAlloc.CpuId]);
                UNIT_ASSERT(added);
            }
        }

        void Balance(ui64 deltaTs, const TVector<ui64>& cpuUs) {
            Now += deltaTs;
            ui64 ts = Now;
            if (Balancer->TryLock(ts)) {
                for (TPoolId pool = 0; pool < cpuUs.size(); pool++) {
                    CpuUs[pool] += cpuUs[pool];
                    TBalancerStats stats;
                    stats.Ts = ts;
                    stats.CpuUs = CpuUs[pool];
                    Balancer->SetPoolStats(pool, stats);
                }
                Balancer->Balance();
                Balancer->Unlock();
            }
        }

        void ApplyMovements() {
            for (TCpuState& state : CpuStates) {
                TPoolId current;
                TPoolId assigned;
                state.Load(assigned, current);
                state.SwitchPool(assigned);
            }
        }

        static TString ToStr(const TVector<ui64>& values) {
            TStringStream ss;
            ss << "{";
            for (auto v : values) {
                ss << " " << v;
            }
            ss << " }";
            return ss.Str();
        }

        void AssertPoolsCurrentCpus(const TVector<ui64>& cpuRequired) {
            TVector<ui64> cpuCurrent;
            cpuCurrent.resize(cpuRequired.size());
            for (TCpuState& state : CpuStates) {
                TPoolId current;
                TPoolId assigned;
                state.Load(assigned, current);
                cpuCurrent[current]++;
            }
            for (TPoolId pool = 0; pool < cpuRequired.size(); pool++) {
                UNIT_ASSERT_C(cpuCurrent[pool] == cpuRequired[pool],
                    "cpu distribution mismatch, required " << ToStr(cpuRequired) << " but got " << ToStr(cpuCurrent));
            }
        }
    };

    Y_UNIT_TEST(StartLwtrace) {
        NLWTrace::StartLwtraceFromEnv();
    }

    Y_UNIT_TEST(AllOverloaded) {
        TTest t;
        int cpus = 10;
        t.SetCpuCount(cpus);
        t.AddPool(1, 1, 10); // pool=0
        t.AddPool(1, 2, 10); // pool=1
        t.AddPool(1, 3, 10); // pool=2
        t.AddPool(1, 4, 10); // pool=2
        t.Start();
        ui64 dts = 1.01 * Us2Ts(t.Config.UnitedWorkers.Balancer.PeriodUs);
        ui64 totalCpuUs = cpus * Ts2Us(dts); // pretend every pool has consumed as whole actorsystem, overload
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {totalCpuUs, totalCpuUs, totalCpuUs, totalCpuUs});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({1, 2, 3, 4});
    }

    Y_UNIT_TEST(OneOverloaded) {
        TTest t;
        int cpus = 10;
        t.SetCpuCount(cpus);
        t.AddPool(1, 1, 10); // pool=0
        t.AddPool(1, 2, 10); // pool=1
        t.AddPool(1, 3, 10); // pool=2
        t.AddPool(1, 4, 10); // pool=2
        t.Start();
        ui64 dts = 1.01 * Us2Ts(t.Config.UnitedWorkers.Balancer.PeriodUs);
        ui64 totalCpuUs = cpus * Ts2Us(dts);
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {totalCpuUs, 0, 0, 0});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({7, 1, 1, 1});
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {0, totalCpuUs, 0, 0});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({1, 7, 1, 1});
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {0, 0, totalCpuUs, 0});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({1, 1, 7, 1});
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {0, 0, 0, totalCpuUs});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({1, 1, 1, 7});
    }

    Y_UNIT_TEST(TwoOverloadedFairness) {
        TTest t;
        int cpus = 10;
        t.SetCpuCount(cpus);
        t.AddPool(1, 1, 10); // pool=0
        t.AddPool(1, 2, 10); // pool=1
        t.AddPool(1, 3, 10); // pool=2
        t.AddPool(1, 4, 10); // pool=2
        t.Start();
        ui64 dts = 1.01 * Us2Ts(t.Config.UnitedWorkers.Balancer.PeriodUs);
        ui64 totalCpuUs = cpus * Ts2Us(dts);
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {totalCpuUs, totalCpuUs, 0, 0});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({3, 5, 1, 1});
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {totalCpuUs, 0, totalCpuUs, 0});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({2, 1, 6, 1});
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {totalCpuUs, 0, 0, totalCpuUs});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({2, 1, 1, 6});
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {0, totalCpuUs, totalCpuUs, 0});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({1, 3, 5, 1});
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {0, totalCpuUs, 0, totalCpuUs});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({1, 3, 1, 5});
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {0, 0, totalCpuUs, totalCpuUs});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({1, 1, 3, 5});
    }

    Y_UNIT_TEST(TwoOverloadedPriority) {
        TTest t;
        int cpus = 20;
        t.SetCpuCount(cpus);
        t.AddPool(1, 5, 20, 0); // pool=0
        t.AddPool(1, 5, 20, 1); // pool=1
        t.AddPool(1, 5, 20, 2); // pool=2
        t.AddPool(1, 5, 20, 3); // pool=3
        t.Start();
        ui64 dts = 1.01 * Us2Ts(t.Config.UnitedWorkers.Balancer.PeriodUs);
        ui64 mErlang = Ts2Us(dts) / 1000;
        for (int i = 0; i < cpus; i++) {
            t.Balance(dts, {20000 * mErlang, 2500 * mErlang, 4500 * mErlang, 9500 * mErlang});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({2, 3, 5, 10});
        t.Balance(dts, {20000 * mErlang, 2500 * mErlang, 4500 * mErlang, 8500 * mErlang});
        t.ApplyMovements();
        t.AssertPoolsCurrentCpus({3, 3, 5, 9});
        // NOTE: this operation require one move, but we do not make global analysis, so multiple steps (1->2 & 0->1) are required (can be optimized later)
        for (int i = 0; i < 3; i++) {
            t.Balance(dts, {20000 * mErlang, 2500 * mErlang, 5500 * mErlang, 8500 * mErlang});
            t.ApplyMovements();
        }
        t.AssertPoolsCurrentCpus({2, 3, 6, 9});
    }
}
