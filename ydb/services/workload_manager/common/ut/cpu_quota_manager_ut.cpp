#include <ydb/services/workload_manager/common/cpu_quota_manager.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager {

namespace {

constexpr double MAX_CLUSTER_LOAD = 0.5;
constexpr double DEFAULT_QUERY_LOAD = 0.1;
constexpr ui64 CPU_NUMBER = 8;
constexpr TDuration LONG_QUERY = TDuration::Seconds(30);

TCpuQuotaManager MakeManager() {
    return TCpuQuotaManager(TDuration::Seconds(1), TDuration::Seconds(10), TDuration::Seconds(60),
        DEFAULT_QUERY_LOAD, true, CPU_NUMBER, MakeIntrusive<NMonitoring::TDynamicCounters>());
}

bool StartQuery(TCpuQuotaManager& manager) {
    return manager.RequestCpuQuota(0.0, MAX_CLUSTER_LOAD).Status == NYdb::EStatus::SUCCESS;
}

// A query that ran for a long time and burned no cpu at all, so the refund is the whole
// DefaultQueryLoad guess, halved
void FinishZeroCpuQuery(TCpuQuotaManager& manager) {
    manager.AdjustCpuQuota(0.0, LONG_QUERY, 0.0);
}

}  // anonymous namespace

Y_UNIT_TEST_SUITE(CpuQuotaManager) {
    // A query longer than AverageLoadInterval / 2 used to be refused the refund entirely
    Y_UNIT_TEST(LongQueryReleasesItsQuota) {
        auto manager = MakeManager();

        manager.UpdateCpuLoad(0.1, CPU_NUMBER, true);
        UNIT_ASSERT(StartQuery(manager));
        const double admitted = manager.GetQuotedLoad();

        FinishZeroCpuQuery(manager);
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), admitted - DEFAULT_QUERY_LOAD / 2, 1e-9);
    }

    // The floor is the instant load and not the average: a wave of queries finishing together may
    // not push the quoted load below what the cluster was last measured at
    Y_UNIT_TEST(RefundIsFlooredAtInstantLoad) {
        auto manager = MakeManager();

        // Two measurements far apart in value, so the average still carries the older one however
        // much wall time passes between these two calls
        manager.UpdateCpuLoad(0.9, CPU_NUMBER, true);
        manager.UpdateCpuLoad(0.1, CPU_NUMBER, true);
        UNIT_ASSERT_GT(manager.GetAverageLoad(), manager.GetInstantLoad() + DEFAULT_QUERY_LOAD);

        UNIT_ASSERT(manager.RequestCpuQuota(0.0, 1.0).Status == NYdb::EStatus::SUCCESS);
        for (int i = 0; i < 20; ++i) {
            FinishZeroCpuQuery(manager);
        }

        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), manager.GetInstantLoad(), 1e-9);
    }
}

}  // namespace NKikimr::NWorkloadManager
