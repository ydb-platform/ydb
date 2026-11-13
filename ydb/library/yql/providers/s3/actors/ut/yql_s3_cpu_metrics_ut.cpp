#include <ydb/library/yql/providers/s3/actors/yql_s3_cpu_metrics.h>

#include <library/cpp/testing/unittest/registar.h>

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

Y_UNIT_TEST_SUITE(TS3CpuMetricsTest) {
    Y_UNIT_TEST(RetriesCountOneWaitAndItsFullDuration) {
        TFixture f;
        TS3CpuMetrics metrics(f.Counters, &f.Work, "reader");
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
            TS3CpuMetrics metrics(f.Counters, &f.Work, "reader");
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
        TS3CpuMetrics live(f.Counters, &f.Work, "reader");
        live.OnAdmission(true, f.Start);
        {
            TS3CpuMetrics exiting(f.Counters, &f.Work, "reader");
            exiting.OnAdmission(true, f.Start);
            UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuUnfinishedOnExit"), 1);
        live.OnStop(f.Start);
    }

    Y_UNIT_TEST(ReaderAndDecompressorAreSeparate) {
        TFixture f;
        TS3CpuMetrics reader(f.Counters, &f.Work, "reader");
        TS3CpuMetrics decompressor(f.Counters, &f.Work, "decompressor");
        reader.OnAdmission(true, f.Start);
        decompressor.OnAdmission(false, f.Start);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive"), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuWaiting"), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuActive", "decompressor"), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Value("S3CpuWaiting", "decompressor"), 1);
        reader.OnStop(f.Start);
        decompressor.OnStop(f.Start);
    }

    Y_UNIT_TEST(DisabledWithoutCountersOrSchedulerWork) {
        TFixture f;
        TS3CpuMetrics noCounters(nullptr, &f.Work, "reader");
        TS3CpuMetrics noWork(f.Counters, nullptr, "reader");
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
