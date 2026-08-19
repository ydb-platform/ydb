#include <ydb/services/workload_manager/common/cpu_quota_manager.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager {

namespace {

constexpr double MAX_CLUSTER_LOAD = 0.5;
constexpr double BACKGROUND_LOAD = 0.1;
constexpr ui64 CPU_NUMBER = 8;

// Drives TCpuQuotaManager on a virtual clock, so the time based accounting can be
// checked without sleeping.
class TTestCpuQuotaManager : public TCpuQuotaManager {
public:
    explicit TTestCpuQuotaManager(const TSettings& settings)
        : TCpuQuotaManager(settings, MakeIntrusive<NMonitoring::TDynamicCounters>())
    {}

    void Advance(TDuration delta) {
        CurrentTime += delta;
    }

    // Reports the load measurement the cluster would produce at the current moment
    void MeasureLoad(double instantLoad) {
        UpdateCpuLoad(instantLoad, CPU_NUMBER, true);
    }

    bool TryStartQuery(double maxClusterLoad = MAX_CLUSTER_LOAD) {
        return RequestCpuQuota(0.0, maxClusterLoad).Status == NYdb::EStatus::SUCCESS;
    }

    void FinishQuery(TDuration duration, double cpuSecondsConsumed = 0.0) {
        AdjustCpuQuota(0.0, duration, cpuSecondsConsumed);
    }

protected:
    TInstant GetNow() const override {
        return CurrentTime;
    }

private:
    TInstant CurrentTime = TInstant::Seconds(1000);
};

}  // anonymous namespace

Y_UNIT_TEST_SUITE(CpuQuotaManager) {
    // Reproduces YQ-5528: with the legacy accounting the quota reserved for a query is never
    // returned when the query finishes, it only decays with the AverageLoadInterval time
    // constant. A steady stream of long queries makes QuotedLoad drift far above the real
    // load until admission stops completely.
    Y_UNIT_TEST(LegacyQuotaDriftsAboveRealLoad) {
        TTestCpuQuotaManager manager({});

        bool admissionStopped = false;
        for (int i = 0; i < 30; ++i) {
            manager.Advance(TDuration::Seconds(1));
            manager.MeasureLoad(BACKGROUND_LOAD);
            if (!manager.TryStartQuery()) {
                admissionStopped = true;
                break;
            }
            manager.FinishQuery(TDuration::Seconds(30));
        }

        UNIT_ASSERT_C(admissionStopped, "expected the legacy accounting to block admission");
        UNIT_ASSERT_GT(manager.GetQuotedLoad(), MAX_CLUSTER_LOAD);
        UNIT_ASSERT_VALUES_EQUAL(manager.GetInstantLoad(), BACKGROUND_LOAD);
    }

    Y_UNIT_TEST(BlocksWhenMeasuredLoadIsAboveThreshold) {
        TTestCpuQuotaManager manager({});

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(0.6);
        UNIT_ASSERT(!manager.TryStartQuery());
    }

    Y_UNIT_TEST(BlocksWhenLoadIsOutdated) {
        TCpuQuotaManager::TSettings settings;
        TTestCpuQuotaManager manager(settings);

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());

        manager.Advance(settings.AverageLoadInterval + TDuration::Seconds(1));
        UNIT_ASSERT(!manager.TryStartQuery());
    }

    Y_UNIT_TEST(IgnoresInvalidSettings) {
        TCpuQuotaManager::TSettings settings;
        TTestCpuQuotaManager manager(settings);

        auto invalid = settings;
        invalid.AverageLoadInterval = TDuration::Zero();
        invalid.DefaultQueryLoad = 0.0;
        manager.UpdateSettings(invalid);

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD + settings.DefaultQueryLoad, 1e-9);
    }

    Y_UNIT_TEST(ReservationReleasedOnFinish) {
        TCpuQuotaManager::TSettings settings;
        settings.EnableLoadReservations = true;
        TTestCpuQuotaManager manager(settings);

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD + settings.DefaultQueryLoad, 1e-9);

        manager.Advance(TDuration::Seconds(1));
        manager.FinishQuery(TDuration::Seconds(1));
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD, 1e-9);
    }

    Y_UNIT_TEST(ReservationExpiresForLongQuery) {
        TCpuQuotaManager::TSettings settings;
        settings.EnableLoadReservations = true;
        TTestCpuQuotaManager manager(settings);

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());

        // A query longer than the visibility delay is already accounted for by the measurement,
        // its reservation is dropped by expiration rather than by the finish event
        manager.FinishQuery(TDuration::Seconds(30));
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD + settings.DefaultQueryLoad, 1e-9);

        manager.Advance(settings.LoadVisibilityDelay);
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD, 1e-9);
    }

    // YQ-5528: with reservations the quoted load stays tied to the measured one instead of
    // drifting away, so a steady stream of long queries does not strangle admission
    Y_UNIT_TEST(QuotedLoadFollowsMeasuredLoad) {
        TCpuQuotaManager::TSettings settings;
        settings.EnableLoadReservations = true;
        TTestCpuQuotaManager manager(settings);

        for (int i = 0; i < 60; ++i) {
            manager.Advance(TDuration::Seconds(1));
            manager.MeasureLoad(BACKGROUND_LOAD);
            if (manager.TryStartQuery()) {
                manager.FinishQuery(TDuration::Seconds(30));
            }
            UNIT_ASSERT_LE(manager.GetQuotedLoad(), MAX_CLUSTER_LOAD + settings.DefaultQueryLoad);
        }

        manager.Advance(settings.LoadVisibilityDelay);
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD, 1e-9);
    }

    Y_UNIT_TEST(SwitchingOffReservationsDoesNotStallAdmission) {
        TCpuQuotaManager::TSettings settings;
        settings.EnableLoadReservations = true;
        TTestCpuQuotaManager manager(settings);

        for (int i = 0; i < 60; ++i) {
            manager.Advance(TDuration::Seconds(1));
            manager.MeasureLoad(BACKGROUND_LOAD);
            if (manager.TryStartQuery()) {
                manager.FinishQuery(TDuration::Seconds(30));
            }
        }

        settings.EnableLoadReservations = false;
        manager.UpdateSettings(settings);
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD, 1e-9);
        UNIT_ASSERT(manager.TryStartQuery());
    }

    // Two queries in flight, the younger finishes first: its own reservation must be the one
    // released, otherwise the survivor carries the wrong expiry deadline
    Y_UNIT_TEST(ConcurrentQueriesReleaseOwnReservation) {
        TCpuQuotaManager::TSettings settings;
        settings.EnableLoadReservations = true;
        TTestCpuQuotaManager manager(settings);

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());                       // admitted at t0

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());                       // admitted at t0 + 1s

        manager.Advance(TDuration::Seconds(1));
        manager.FinishQuery(TDuration::Seconds(1));                 // the younger one finishes
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD + settings.DefaultQueryLoad, 1e-9);

        // the reservation left behind belongs to the older query, so it expires first
        manager.Advance(TDuration::MilliSeconds(3500));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD, 1e-9);
    }

    // A query reports the duration of its execution, which starts after admission. When that gap
    // pushes it past LoadVisibilityDelay its own reservation is already gone, and finishing must
    // not consume a reservation belonging to a query that is still running.
    Y_UNIT_TEST(NoReleaseWhenOwnReservationExpired) {
        TCpuQuotaManager::TSettings settings;
        settings.EnableLoadReservations = true;
        TTestCpuQuotaManager manager(settings);

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());                       // admitted at t0

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());                       // admitted at t0 + 1s

        // 5.2s after its admission, reporting 4.9s of execution
        manager.Advance(TDuration::MilliSeconds(4200));
        manager.FinishQuery(TDuration::MilliSeconds(4900));

        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD + settings.DefaultQueryLoad, 1e-9);
    }

    // FQ reserves a per request amount rather than the default, so a release must match the
    // amount that was reserved
    Y_UNIT_TEST(ReleaseMatchesReservedAmount) {
        TCpuQuotaManager::TSettings settings;
        settings.EnableLoadReservations = true;
        TTestCpuQuotaManager manager(settings);

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.RequestCpuQuota(0.3, 0.9).Status == NYdb::EStatus::SUCCESS);
        UNIT_ASSERT(manager.RequestCpuQuota(0.1, 0.9).Status == NYdb::EStatus::SUCCESS);

        manager.Advance(TDuration::Seconds(1));
        manager.AdjustCpuQuota(0.1, TDuration::Seconds(1), 0.0);    // the small one finishes

        // the 0.3 reservation must survive untouched
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD + 0.3, 1e-9);
    }

    // Enabling reservations on a live node must carry over the admissions made just before the
    // flip: they are running and still unmeasured, so discarding them would over admit. The
    // resulting conservatism is bounded by LoadVisibilityDelay and must clear on its own.
    Y_UNIT_TEST(SwitchingOnReservationsKeepsRecentAdmissions) {
        TCpuQuotaManager::TSettings settings;
        TTestCpuQuotaManager manager(settings);

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());

        manager.Advance(TDuration::Seconds(1));
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT(manager.TryStartQuery());

        manager.Advance(TDuration::Seconds(1));
        settings.EnableLoadReservations = true;
        manager.UpdateSettings(settings);
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD + 2 * settings.DefaultQueryLoad, 1e-9);

        manager.Advance(settings.LoadVisibilityDelay);
        manager.MeasureLoad(BACKGROUND_LOAD);
        UNIT_ASSERT_DOUBLES_EQUAL(manager.GetQuotedLoad(), BACKGROUND_LOAD, 1e-9);
        UNIT_ASSERT(manager.TryStartQuery());
    }
}

}  // namespace NKikimr::NWorkloadManager
