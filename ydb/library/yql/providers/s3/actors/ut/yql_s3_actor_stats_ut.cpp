#include <ydb/library/yql/providers/s3/actors/yql_s3_actor_stats.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <thread>

namespace NYql::NDq {
namespace {

class TWork: public IDqSchedulableWork {
public:
    std::optional<TDuration> TryStartExecution(TMonotonic) override {
        UNIT_FAIL("Metrics must not acquire quota");
        return {};
    }

    void StopExecution() override {
        UNIT_FAIL("Metrics must not release quota");
    }

    void NotifyResumed(bool) override {
    }

    void RegisterForResume(const NActors::TActorId &) override {
    }

    TWorkScope GetWorkScope() const override {
        return { "/Root", "test" };
    }
};

struct TFixture {
    TWork Work;
    ::NMonitoring::TDynamicCounterPtr Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
    const TMonotonic Start = TMonotonic::Now();

    ui64 Value(const TString &name, const TString &kind = "reader") const {
        const bool derivative = name == "S3CpuAdmissionWaitUs" || name == "S3CpuUnfinishedOnExit";
        return Counters->GetSubgroup("db", "/Root")->GetSubgroup("pool", "test")->GetSubgroup("kind", kind)->GetCounter(name, derivative)->Val();
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TS3ActorStatsTest) {
    Y_UNIT_TEST(RetriesCountOneWaitAndItsFullDuration) {
        TFixture f;
        TS3ActorStats metrics(f.Counters, &f.Work, "reader");
        metrics.OnAdmission(false, f.Start);
        metrics.OnAdmission(false, f.Start + TDuration::MicroSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuWaiting"), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 0);
        metrics.OnAdmission(true, f.Start + TDuration::MicroSeconds(30));
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuWaiting"), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuAdmissionWaitUs"), 30);
        metrics.OnStop(f.Start + TDuration::MicroSeconds(40));
        metrics.OnAdmission(false, f.Start + TDuration::MicroSeconds(50));
        metrics.OnAdmission(true, f.Start + TDuration::MicroSeconds(55));
        metrics.OnStop(f.Start + TDuration::MicroSeconds(60));
        metrics.OnExit(f.Start + TDuration::MicroSeconds(70));
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuAdmissionWaitUs"), 35);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuUnfinishedOnExit"), 0);
    }

    Y_UNIT_TEST(ExitWhileWaitingRecordsOnceAndBalancesGauge) {
        TFixture f;
        {
            TS3ActorStats metrics(f.Counters, &f.Work, "reader");
            metrics.OnAdmission(false, f.Start);
            metrics.OnExit(f.Start + TDuration::MicroSeconds(25));
            metrics.OnExit(f.Start + TDuration::MicroSeconds(30));
            UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuWaiting"), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuAdmissionWaitUs"), 25);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuUnfinishedOnExit"), 1);
    }

    Y_UNIT_TEST(DestructionWhileActivePreservesOtherLiveWork) {
        TFixture f;
        TS3ActorStats live(f.Counters, &f.Work, "reader");
        live.OnAdmission(true, f.Start);
        {
            TS3ActorStats exiting(f.Counters, &f.Work, "reader");
            exiting.OnAdmission(true, f.Start);
            UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuUnfinishedOnExit"), 1);
        live.OnStop(f.Start);
    }

    Y_UNIT_TEST(ReadersHaveIndependentSnapshotsAndSharedCounters) {
        TFixture f;
        TS3ActorStats reader(f.Counters, &f.Work);
        TS3ActorStats waiting(f.Counters, &f.Work);
        reader.OnAdmission(true, f.Start);
        waiting.OnAdmission(false, f.Start);
        waiting.SetState(IS3ActorStats::EState::WaitingCpu, f.Start);
        UNIT_ASSERT(reader.GetSnapshot().CpuHeld);
        UNIT_ASSERT(!waiting.GetSnapshot().CpuHeld);
        UNIT_ASSERT(waiting.GetSnapshot().State == IS3ActorStats::EState::WaitingCpu);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuWaiting"), 1);
        reader.OnStop(f.Start);
        waiting.OnStop(f.Start);
    }

    Y_UNIT_TEST(ExplicitStatesWithoutCounters) {
        TFixture f;
        TS3ActorStats stats(nullptr, &f.Work);
        using EState = IS3ActorStats::EState;
        UNIT_ASSERT(stats.GetSnapshot().State == EState::Initializing);
        stats.SetState(EState::Processing, f.Start);
        stats.OnAdmission(true, f.Start);
        // A phase must not conceal an erroneously held slot.
        stats.SetState(EState::WaitingHttp, f.Start);
        UNIT_ASSERT(stats.GetSnapshot().CpuHeld);
        stats.OnStop(f.Start);
        stats.SetState(EState::WaitingCpu, f.Start + TDuration::MicroSeconds(10));
        stats.OnAdmission(false, f.Start + TDuration::MicroSeconds(10));
        stats.OnAdmission(false, f.Start + TDuration::MicroSeconds(20));
        auto snapshot = stats.GetSnapshot();
        UNIT_ASSERT(snapshot.State == EState::WaitingCpu);
        UNIT_ASSERT(!snapshot.CpuHeld);
        UNIT_ASSERT(snapshot.StateSince == f.Start + TDuration::MicroSeconds(10));
        stats.OnAdmission(true, f.Start + TDuration::MicroSeconds(30));
        // Admission updates CPU accounting only; the caller resets the wait.
        UNIT_ASSERT(stats.GetSnapshot().State == EState::WaitingCpu);
        stats.SetState(EState::Processing, f.Start + TDuration::MicroSeconds(30));
        snapshot = stats.GetSnapshot();
        UNIT_ASSERT(snapshot.State == EState::Processing);
        UNIT_ASSERT(snapshot.CpuHeld);
        UNIT_ASSERT(snapshot.StateSince == f.Start + TDuration::MicroSeconds(30));
        stats.SetState(EState::Processing, f.Start + TDuration::MicroSeconds(40));
        UNIT_ASSERT(stats.GetSnapshot().StateSince == snapshot.StateSince);
        stats.OnExit(f.Start + TDuration::MicroSeconds(50));
        stats.OnExit(f.Start + TDuration::MicroSeconds(60));
        snapshot = stats.GetSnapshot();
        UNIT_ASSERT(snapshot.State == EState::Finished);
        UNIT_ASSERT(!snapshot.CpuHeld);
        UNIT_ASSERT(snapshot.StateSince == f.Start + TDuration::MicroSeconds(50));
    }

    Y_UNIT_TEST(SnapshotSurvivesReaderExit) {
        TFixture f;
        auto stats = std::make_shared<TS3ActorStats>(f.Counters, &f.Work);
        std::shared_ptr<const IS3ActorStats> source = stats;
        stats->OnAdmission(false, f.Start);
        stats->OnExit(f.Start + TDuration::MicroSeconds(25));
        stats.reset();
        UNIT_ASSERT(source->GetSnapshot().State == IS3ActorStats::EState::Finished);
        UNIT_ASSERT(!source->GetSnapshot().CpuHeld);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuWaiting"), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuAdmissionWaitUs"), 25);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuUnfinishedOnExit"), 1);
    }

    Y_UNIT_TEST(ConcurrentSnapshots) {
        TFixture f;
        TS3ActorStats stats(nullptr, &f.Work);
        using EState = IS3ActorStats::EState;
        stats.SetState(EState::Processing, f.Start);
        std::atomic<bool> done = false;
        std::atomic<bool> invalid = false;
        std::thread monitor([&] {
            do {
                const auto snapshot = stats.GetSnapshot();
                if ((snapshot.State == EState::Finished) && snapshot.CpuHeld) {
                    invalid = true;
                }
            } while (!done.load());
        });
        for (ui32 i = 0; i < 10000; ++i) {
            stats.OnAdmission(false, f.Start);
            stats.SetState(EState::WaitingCpu, f.Start);
            stats.OnAdmission(true, f.Start);
            stats.SetState(EState::Processing, f.Start);
            stats.OnStop(f.Start);
        }
        stats.OnExit(f.Start);
        done = true;
        monitor.join();
        UNIT_ASSERT(!invalid.load());
    }

    Y_UNIT_TEST(DisabledWithoutCountersOrSchedulerWork) {
        TFixture f;
        TS3ActorStats noCounters(nullptr, &f.Work, "reader");
        TS3ActorStats noWork(f.Counters, nullptr, "reader");
        for (auto *metrics : { &noCounters, &noWork }) {
            metrics->OnAdmission(false, f.Start);
            metrics->OnAdmission(true, f.Start);
            metrics->OnExit(f.Start);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuWaiting"), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuUnfinishedOnExit"), 0);
    }
}

}   // namespace NYql::NDq
