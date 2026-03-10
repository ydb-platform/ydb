#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/library/profiling/solomon/registry.h>
#include <yt/yt/library/profiling/solomon/remote.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/string/join.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace std::string_literals;

////////////////////////////////////////////////////////////////////////////////

struct TTestMetricConsumer
    : public NMonitoring::IMetricConsumer
{
    void OnStreamBegin() override
    { }

    void OnStreamEnd() override
    { }

    void OnCommonTime(TInstant) override
    { }

    void OnMetricBegin(NMonitoring::EMetricType) override
    { }

    void OnMetricEnd() override
    { }

    void OnLabelsBegin() override
    {
        Labels.clear();
    }

    void OnLabelsEnd() override
    { }

    void OnLabel(TStringBuf name, TStringBuf value) override
    {
        if (name == "sensor") {
            Name = value;
        } else {
            Labels.push_back(std::string(name) + "=" + value.data());
        }
    }

    void OnLabel(ui32 name, ui32 value) override
    {
        OnLabel(LabelsCache[name], LabelsCache[value]);
    }

    std::pair<ui32, ui32> PrepareLabel(const TStringBuf name, const TStringBuf value) override
    {
        LabelsCache.emplace_back(name);
        LabelsCache.emplace_back(value);
        return {LabelsCache.size() - 2, LabelsCache.size() - 1};
    }

    void OnDouble(TInstant, double value) override
    {
        Cerr << FormatName() << " " << value << Endl;
        Gauges[FormatName()] = value;
    }

    void OnUint64(TInstant, ui64) override
    { }

    void OnInt64(TInstant, i64 value) override
    {
        Cerr << FormatName() << " " << value << Endl;
        Counters[FormatName()] = value;
    }

    void OnHistogram(TInstant, NMonitoring::IHistogramSnapshotPtr value) override
    {
        Cerr << FormatName() << " histogram{";
        for (size_t i = 0; i < value->Count(); ++i) {
            Cerr << value->UpperBound(i) << ":" << value->Value(i);
            if (i + 1 != value->Count()) {
                Cerr << ", ";
            }
        }
        Cerr << "}" << Endl;
        Histograms[FormatName()] = value;
    }

    void OnLogHistogram(TInstant, NMonitoring::TLogHistogramSnapshotPtr) override
    { }

    void OnSummaryDouble(TInstant, NMonitoring::ISummaryDoubleSnapshotPtr snapshot) override
    {
        Cerr << FormatName() << " summary{"
            << "min: " << snapshot->GetMin()
            << ", max: " << snapshot->GetMax()
            << ", sum: " << snapshot->GetSum()
            << ", count: " << snapshot->GetCount()
            << ", last: " << snapshot->GetLast()
            << "}" << Endl;
        Summaries[FormatName()] = snapshot;
    }

    std::string Name;
    std::vector<std::string> Labels;

    THashMap<std::string, i64> Counters;
    THashMap<std::string, double> Gauges;
    THashMap<std::string, NMonitoring::ISummaryDoubleSnapshotPtr> Summaries;
    THashMap<std::string, NMonitoring::IHistogramSnapshotPtr> Histograms;

    std::vector<std::string> LabelsCache;

    std::string FormatName() const
    {
        return Name + "{" + JoinSeq(";", Labels) + "}";
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRenameTest
    : public ::testing::TestWithParam<bool>
{ };

////////////////////////////////////////////////////////////////////////////////

TEST(TSolomonRegistryTest, Registration)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/debug");

    auto counter = profiler.Counter("/c0");
    auto gauge = profiler.Gauge("/g0");

    impl->ProcessRegistrations();

    counter.Increment(1);
    gauge.Update(42);
}

TTestMetricConsumer CollectSensors(TSolomonRegistryPtr impl, int subsample = 1, bool enableHack = false)
{
    impl->ProcessRegistrations();

    auto i = impl->GetNextIteration();
    impl->Collect();

    TTestMetricConsumer testConsumer;

    TReadOptions options;
    options.EnableSolomonAggregationWorkaround = enableHack;
    options.Times = {{{}, TInstant::Now()}};
    for (int j = subsample - 1; j >= 0; --j) {
        options.Times[0].first.push_back(impl->IndexOf(i - j));
    }

    impl->ReadSensors(options, &testConsumer);
    Cerr << "-------------------------------------" << Endl;

    return testConsumer;
}

TTestMetricConsumer ReadSensors(TSolomonRegistryPtr impl)
{
    auto i = impl->GetNextIteration();

    TTestMetricConsumer testConsumer;

    TReadOptions options;
    options.Times = {{{impl->IndexOf(i - 1)}, TInstant::Now()}};

    impl->ReadSensors(options, &testConsumer);
    Cerr << "-------------------------------------" << Endl;

    return testConsumer;
}

TEST(TSolomonRegistryTest, CounterProjections)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    auto c0 = profiler.WithTag("user", "u0").Counter("/count");
    auto c1 = profiler.WithTag("user", "u1").Counter("/count");

    auto result = CollectSensors(impl).Counters;

    ASSERT_EQ(result["yt.d.count{}"], 0u);
    ASSERT_EQ(result["yt.d.count{user=u0}"], 0u);

    c0.Increment();
    c1.Increment();

    result = CollectSensors(impl).Counters;

    ASSERT_EQ(result["yt.d.count{}"], 2u);
    ASSERT_EQ(result["yt.d.count{user=u0}"], 1u);

    c0.Increment();
    c1 = {};

    result = CollectSensors(impl).Counters;
    ASSERT_EQ(result["yt.d.count{}"], 3u);
    ASSERT_EQ(result["yt.d.count{user=u0}"], 2u);
    ASSERT_EQ(result.find("yt.d.count{user=u1}"), result.end());

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);
}

TEST(TSolomonRegistryTest, GaugeProjections)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    auto g0 = profiler.WithTag("user", "u0").Gauge("/memory");
    auto g1 = profiler.WithTag("user", "u1").Gauge("/memory");

    auto result = CollectSensors(impl).Gauges;

    ASSERT_EQ(result["yt.d.memory{}"], 0.0);
    ASSERT_EQ(result["yt.d.memory{user=u0}"], 0.0);

    g0.Update(1.0);
    g1.Update(2.0);

    result = CollectSensors(impl).Gauges;
    ASSERT_EQ(result["yt.d.memory{}"], 3.0);
    ASSERT_EQ(result["yt.d.memory{user=u0}"], 1.0);

    g0.Update(10.0);
    g1 = {};

    result = CollectSensors(impl).Gauges;
    ASSERT_EQ(result["yt.d.memory{}"], 10.0);
    ASSERT_EQ(result["yt.d.memory{user=u0}"], 10.0);
    ASSERT_EQ(result.find("yt.d.memory{user=u1}"), result.end());

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);
}

TEST(TSolomonRegistryTest, ExponentialHistogramProjections)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    auto c0 = profiler.WithTag("user", "u0").TimeHistogram("/histogram", TDuration::Zero(), TDuration::MilliSeconds(20));
    auto c1 = profiler.WithTag("user", "u1").TimeHistogram("/histogram", TDuration::Zero(), TDuration::MilliSeconds(20));

    auto result = CollectSensors(impl).Histograms;

    ASSERT_EQ(result["yt.d.histogram{}"]->Count(), 16u);
    ASSERT_EQ(result["yt.d.histogram{user=u0}"]->Count(), 16u);

    c0.Record(TDuration::MilliSeconds(5));
    c1.Record(TDuration::MilliSeconds(5));
    c0.Record(TDuration::MilliSeconds(30));

    result = CollectSensors(impl).Histograms;

    ASSERT_EQ(result["yt.d.histogram{}"]->Count(), 16u);
    ASSERT_EQ(result["yt.d.histogram{}"]->Value(13), 2u);
    ASSERT_EQ(result["yt.d.histogram{user=u0}"]->Value(13), 1u);

    ASSERT_EQ(result["yt.d.histogram{}"]->Value(15), 1u);
    ASSERT_EQ(Max<double>(), result["yt.d.histogram{}"]->UpperBound(15));

    c0.Record(TDuration::MilliSeconds(10));
    c1 = {};

    result = CollectSensors(impl).Histograms;
    ASSERT_EQ(result["yt.d.histogram{}"]->Value(14), 1u);
    ASSERT_EQ(result["yt.d.histogram{user=u0}"]->Value(14), 1u);
    ASSERT_EQ(result.find("yt.d.histogram{user=u1}"), result.end());

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);
}

TEST(TSolomonRegistryTest, DifferentBuckets)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    std::vector<TDuration> firstBounds{
        TDuration::Zero(), TDuration::MilliSeconds(5), TDuration::MilliSeconds(10)
    };

    std::vector<TDuration> secondBounds{
        TDuration::Zero(), TDuration::MilliSeconds(500), TDuration::MilliSeconds(1000)
    };

    auto c0 = profiler.WithTag("user", "u0").TimeHistogram("/histogram", firstBounds);
    auto c1 = profiler.WithTag("user", "u1").TimeHistogram("/histogram", secondBounds);

    auto result = CollectSensors(impl).Histograms;

    ASSERT_EQ(result.size(), 3u);
    ASSERT_EQ(result["yt.d.histogram{}"]->Count(), 6u);
    ASSERT_EQ(result["yt.d.histogram{user=u0}"]->Count(), 4u);
}

TEST(TSolomonRegistryTest, CustomHistogramProjections)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    std::vector<TDuration> bounds{
        TDuration::Zero(), TDuration::MilliSeconds(5), TDuration::MilliSeconds(10), TDuration::MilliSeconds(15)
    };
    auto c0 = profiler.WithTag("user", "u0").TimeHistogram("/histogram", bounds);
    auto c1 = profiler.WithTag("user", "u1").TimeHistogram("/histogram", bounds);

    auto result = CollectSensors(impl).Histograms;

    ASSERT_EQ(result["yt.d.histogram{}"]->Count(), 5u);
    ASSERT_EQ(result["yt.d.histogram{user=u0}"]->Count(), 5u);

    c0.Record(TDuration::MilliSeconds(5));
    c1.Record(TDuration::MilliSeconds(5));
    c0.Record(TDuration::MilliSeconds(16));

    result = CollectSensors(impl).Histograms;

    ASSERT_EQ(result["yt.d.histogram{}"]->Count(), 5u);
    ASSERT_EQ(result["yt.d.histogram{}"]->Value(1), 2u);
    ASSERT_EQ(result["yt.d.histogram{user=u0}"]->Value(1), 1u);

    ASSERT_EQ(result["yt.d.histogram{}"]->Value(4), 1u);
    ASSERT_EQ(Max<double>(), result["yt.d.histogram{}"]->UpperBound(4));

    c0.Record(TDuration::MilliSeconds(10));
    c1 = {};

    result = CollectSensors(impl).Histograms;
    ASSERT_EQ(result["yt.d.histogram{}"]->Value(2), 1u);
    ASSERT_EQ(result["yt.d.histogram{user=u0}"]->Value(2), 1u);
    ASSERT_EQ(result.find("yt.d.histogram{user=u1}"), result.end());

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);
}

TEST(TSolomonRegistryTest, SparseHistogram)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    auto h0 = profiler.WithSparse().TimeHistogram("/histogram", TDuration::Zero(), TDuration::MilliSeconds(20));

    auto result = CollectSensors(impl).Histograms;
    ASSERT_TRUE(result.empty());

    h0.Record(TDuration::MilliSeconds(5));
    result = CollectSensors(impl).Histograms;

    ASSERT_FALSE(result.empty());
    ASSERT_EQ(result["yt.d.histogram{}"]->Count(), 16u);
    ASSERT_EQ(result["yt.d.histogram{}"]->Value(13), 1u);

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);
}

TEST(TSolomonRegistryTest, HistogramWithBigCounterValues)
{
    auto impl = New<TSolomonRegistry>();
    TProfiler profiler(impl, "/d");

    auto h0 = profiler.GaugeHistogram("/histogram", {1.0});

    h0.Add(0, 2e9);
    h0.Add(0, 2e9);

    auto result = h0.GetSnapshot().Values;

    ASSERT_FALSE(result.empty());
    ASSERT_EQ(result.front(), 4e9);
}

TEST(TSolomonRegistryTest, SparseCounters)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    auto c = profiler.WithSparse().Counter("/sparse_counter");

    auto result = CollectSensors(impl).Counters;
    ASSERT_TRUE(result.empty());

    c.Increment();
    result = CollectSensors(impl).Counters;
    ASSERT_EQ(result["yt.d.sparse_counter{}"], 1u);

    result = CollectSensors(impl).Counters;
    ASSERT_TRUE(result.empty());

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);

    c.Increment();
    result = CollectSensors(impl).Counters;
    ASSERT_EQ(result["yt.d.sparse_counter{}"], 2u);
}

TEST(TSolomonRegistryTest, GaugesNoDefault)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    auto g = profiler.WithDefaultDisabled().Gauge("/gauge");

    auto result = CollectSensors(impl).Gauges;
    ASSERT_TRUE(result.empty());

    g.Update(1);
    result = CollectSensors(impl).Gauges;
    ASSERT_EQ(result["yt.d.gauge{}"], 1.0);
}

TEST(TSolomonRegistryTest, SparseCountersWithHack)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    auto c = profiler.WithSparse().Counter("/sparse_counter_with_hack");

    auto result = CollectSensors(impl, 1, true).Counters;
    ASSERT_TRUE(result.empty());

    c.Increment();
    result = CollectSensors(impl, 1, true).Counters;
    ASSERT_EQ(result["yt.d.sparse_counter_with_hack{}"], 1u);

    result = CollectSensors(impl, 2, true).Counters;
    ASSERT_EQ(result["yt.d.sparse_counter_with_hack{}"], 1u);

    result = CollectSensors(impl, 3, true).Counters;
    ASSERT_EQ(result["yt.d.sparse_counter_with_hack{}"], 1u);

    result = CollectSensors(impl, 3, true).Counters;
    ASSERT_TRUE(result.empty());
}

TEST(TSolomonRegistryTest, SparseGauge)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    auto c = profiler.WithSparse().Gauge("/sparse_gauge");

    auto result = CollectSensors(impl).Gauges;
    ASSERT_TRUE(result.empty());

    c.Update(1.0);
    result = CollectSensors(impl).Gauges;
    ASSERT_EQ(result["yt.d.sparse_gauge{}"], 1.0);

    c.Update(0.0);
    result = CollectSensors(impl).Gauges;
    ASSERT_TRUE(result.empty());

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);
}

TEST(TSolomonRegistryTest, SparseGaugeSummary)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    auto c = profiler.WithSparse().GaugeSummary("/sparse_gauge_summary", ESummaryPolicy::Max);

    auto result = CollectSensors(impl).Gauges;
    ASSERT_TRUE(result.empty());

    c.Update(1.0);
    result = CollectSensors(impl).Gauges;
    ASSERT_EQ(result["yt.d.sparse_gauge_summary.max{}"], 1.0);

    c.Update(0.0);
    result = CollectSensors(impl).Gauges;
    ASSERT_TRUE(result.empty());
}

TEST(TSolomonRegistryTest, InvalidSensors)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d");

    auto invalidTypeCounter = r.Counter("/invalid_type");
    auto invalidTypeGauge = r.Gauge("/invalid_type");

    auto invalidSettingsCounter0 = r.Counter("/invalid_settings");
    auto invalidSettingsCounter1 = r.WithGlobal().Counter("/invalid_settings");

    auto result = CollectSensors(impl);
    ASSERT_TRUE(result.Counters.empty());
    ASSERT_TRUE(result.Gauges.empty());

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);
}

struct TDebugProducer
    : public ISensorProducer
{
    TSensorBuffer Buffer;

    virtual ~TDebugProducer()
    { }

    void CollectSensors(ISensorWriter* writer) override
    {
        Buffer.WriteTo(writer);
    }
};

TEST(TSolomonRegistryTest, GaugeProducer)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d");

    auto p0 = New<TDebugProducer>();
    r.AddProducer("/cpu", p0);

    auto p1 = New<TDebugProducer>();
    r.AddProducer("/cpu", p1);

    auto result = CollectSensors(impl).Gauges;
    ASSERT_TRUE(result.empty());

    {
        TWithTagGuard tagGuard(&p0->Buffer, "thread", "Control");
        p0->Buffer.AddGauge("/user_time", 98);
        p0->Buffer.AddGauge("/system_time", 15);
    }

    {
        TWithTagGuard tagGuard(&p1->Buffer, "thread", "Profiler");
        p1->Buffer.AddGauge("/user_time", 2);
        p1->Buffer.AddGauge("/system_time", 25);
    }

    result = CollectSensors(impl).Gauges;
    ASSERT_EQ(result["yt.d.cpu.user_time{thread=Control}"], 98.0);
    ASSERT_EQ(result["yt.d.cpu.user_time{thread=Profiler}"], 2.0);
    ASSERT_EQ(result["yt.d.cpu.user_time{}"], 100.0);
    ASSERT_EQ(result["yt.d.cpu.system_time{thread=Control}"], 15.0);
    ASSERT_EQ(result["yt.d.cpu.system_time{thread=Profiler}"], 25.0);
    ASSERT_EQ(result["yt.d.cpu.system_time{}"], 40.0);

    p0 = {};
    result = CollectSensors(impl).Gauges;
    ASSERT_EQ(result.size(), static_cast<size_t>(4));
    ASSERT_EQ(result["yt.d.cpu.user_time{thread=Profiler}"], 2.0);
    ASSERT_EQ(result["yt.d.cpu.user_time{}"], 2.0);
    ASSERT_EQ(result["yt.d.cpu.system_time{thread=Profiler}"], 25.0);
    ASSERT_EQ(result["yt.d.cpu.system_time{}"], 25.0);

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);
}

TEST(TSolomonRegistryTest, CustomProjections)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d");

    auto c0 = r.Counter("/simple_sharded");
    c0.Increment();

    auto c1 = r.Counter("/simple_sharded");
    c1.Increment();

    auto g0 = r.WithExcludedTag("node_shard", "0").Gauge("/excluded_tag");
    g0.Update(10);

    auto g1 = r.WithExcludedTag("node_shard", "1").Gauge("/excluded_tag");
    g1.Update(20);

    auto c2 = r
        .WithRequiredTag("bundle", "sys")
        .WithTag("table_path", "//sys/operations")
        .Counter("/request_count");
    c2.Increment();

    auto c3 = r
        .WithTag("medium", "ssd")
        .WithTag("disk", "ssd0", -1)
        .Counter("/iops");
    c3.Increment();

    auto result = CollectSensors(impl);
    ASSERT_EQ(result.Counters["yt.d.simple_sharded{}"], 2u);

    ASSERT_EQ(result.Gauges["yt.d.excluded_tag{}"], 30.0);
    ASSERT_EQ(result.Gauges.size(), static_cast<size_t>(1));

    ASSERT_EQ(result.Counters["yt.d.request_count{bundle=sys}"], 1u);
    ASSERT_EQ(result.Counters["yt.d.request_count{bundle=sys;table_path=//sys/operations}"], 1u);
    ASSERT_TRUE(result.Counters.find("yt.d.request_count{}") == result.Counters.end());
    ASSERT_TRUE(result.Counters.find("yt.d.request_count{table_path=//sys/operations}") == result.Counters.end());

    CollectSensors(impl, 2);
    CollectSensors(impl, 3);
}

TEST(TSolomonRegistryTest, DisableProjections)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d");

    auto p0 = New<TDebugProducer>();
    r.WithProjectionsDisabled().AddProducer("/bigb", p0);

    {
        TWithTagGuard guard(&p0->Buffer, "mode", "sum");
        p0->Buffer.AddGauge("", 10);
    }

    {
        TWithTagGuard guard(&p0->Buffer, "mode", "percentile");
        {
            TWithTagGuard guard(&p0->Buffer, "p", "50");
            p0->Buffer.AddCounter("", 20);
        }
        {
            TWithTagGuard guard(&p0->Buffer, "p", "99");
            p0->Buffer.AddCounter("", 1);
        }
    }

    auto result = CollectSensors(impl);
    ASSERT_EQ(1u, result.Gauges.size());
    ASSERT_EQ(10.0, result.Gauges["yt.d.bigb{mode=sum}"]);

    ASSERT_EQ(2u, result.Counters.size());
    ASSERT_EQ(20, result.Counters["yt.d.bigb{mode=percentile;p=50}"]);
    ASSERT_EQ(1, result.Counters["yt.d.bigb{mode=percentile;p=99}"]);
}

TEST(TSolomonRegistryTest, DisableRenaming)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d", "");

    auto p0 = New<TDebugProducer>();
    r.WithRenameDisabled().AddProducer("/bigb", p0);
    p0->Buffer.AddGauge("/gauge", 10);
    p0->Buffer.AddCounter("/counter", 5);


    auto result = CollectSensors(impl);
    ASSERT_EQ(1u, result.Gauges.size());
    EXPECT_EQ(10.0, result.Gauges["/d/bigb/gauge{}"]);

    ASSERT_EQ(1u, result.Counters.size());
    EXPECT_EQ(5, result.Counters["/d/bigb/counter{}"]);
}

DECLARE_REFCOUNTED_STRUCT(TCounterProducer)

struct TCounterProducer
    : public ISensorProducer
{
    int i = 0;

    void CollectSensors(ISensorWriter* writer) override
    {
        writer->AddCounter("/counter", ++i);
    }
};

DEFINE_REFCOUNTED_TYPE(TCounterProducer)

TEST(TSolomonRegistryTest, CounterProducer)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d");

    auto p0 = New<TCounterProducer>();
    r.WithProjectionsDisabled().AddProducer("", p0);

    auto result = CollectSensors(impl).Counters;
    ASSERT_EQ(1, result["yt.d.counter{}"]);

    result = CollectSensors(impl).Counters;
    ASSERT_EQ(2, result["yt.d.counter{}"]);

    result = CollectSensors(impl).Counters;
    ASSERT_EQ(3, result["yt.d.counter{}"]);
}

DECLARE_REFCOUNTED_STRUCT(TBadProducer)

struct TBadProducer
    : public ISensorProducer
{
    void CollectSensors(ISensorWriter*) override
    {
        THROW_ERROR_EXCEPTION("Unavailable");
    }
};

DEFINE_REFCOUNTED_TYPE(TBadProducer)

TEST(TSolomonRegistryTest, Exceptions)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d");

    auto producer = New<TBadProducer>();
    r.AddProducer("/p", producer);
    r.AddFuncCounter("/c", producer, [] () -> i64 {
        THROW_ERROR_EXCEPTION("Unavailable");
    });
    r.AddFuncGauge("/g", producer, [] () -> double {
        THROW_ERROR_EXCEPTION("Unavailable");
    });

    impl->ProcessRegistrations();
    impl->Collect();
}

TEST(TSolomonRegistryTest, CounterTagsBug)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d");

    auto r1 = r.WithTag("client", "1");

    TTagList tags;
    tags.emplace_back("cluster", "hahn");

    auto c = r1.WithTags(TTagSet{tags}).Counter("/foo");
    c.Increment();

    impl->ProcessRegistrations();
}

TEST(TSolomonRegistryTest, TestRemoteTransfer)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);

    auto remote = New<TSolomonRegistry>();
    remote->SetWindowSize(12);
    TProfiler r(remote, "/r");

    auto c0 = r.Counter("/c");
    c0.Increment(1);

    auto d0 = r.Gauge("/d");
    d0.Update(1.0);

    auto c1 = r.TimeCounter("/t");
    c1.Add(TDuration::Seconds(1));

    auto s0 = r.Summary("/s");
    s0.Record(1.0);

    auto t0 = r.Timer("/dt");
    t0.Record(TDuration::Seconds(1));

    auto h0 = r.TimeHistogram("/h", TDuration::Zero(), TDuration::MilliSeconds(20));
    h0.Record(TDuration::MilliSeconds(1));

    remote->ProcessRegistrations();
    remote->Collect();

    auto dump = remote->DumpSensors();

    TRemoteRegistry remoteRegistry(impl.Get());

    impl->Collect();
    remoteRegistry.Transfer(dump);

    auto sensors = ReadSensors(impl);
    ASSERT_EQ(1, sensors.Counters["yt.r.c{}"]);

    impl->Collect();
    remoteRegistry.Detach();

    sensors = ReadSensors(impl);
    ASSERT_TRUE(sensors.Counters.empty());
}

TEST(TSolomonRegistryTest, TestRemoteTransferWithDistinctTags)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);

    TRemoteRegistry remoteRegistry(impl.Get());
    const auto& tagRegistry = impl->GetTagRegistry();

    auto buildSensorDump = [] (const std::string& tagsPrefix, int tagsCount, const std::string& sensor, i64 value) {
        NProto::TSensorDump dump;

        dump.add_tags();
        for (TTagId tagId = 1; tagId <= tagsCount; ++tagId) {
            auto value = std::to_string(tagId);

            auto* tag = dump.add_tags();
            tag->set_key(tagsPrefix + value);
            tag->set_value(value);
        }

        auto* cube = dump.add_cubes();
        cube->set_name(sensor);

        auto* projection = cube->add_projections();
        for (TTagId tagId = 1; tagId <= tagsCount; ++tagId) {
            projection->add_tag_ids(tagId);
        }
        projection->set_has_value(true);
        projection->set_counter(value);

        return dump;
    };

    auto transferAndCollect = [&] (const std::string& tagsPrefix, int tagsCount, const std::string& sensor, i64 value) {
        auto dump = buildSensorDump(tagsPrefix, tagsCount, sensor, value);
        remoteRegistry.Transfer(dump);

        impl->Collect();
        return ReadSensors(impl);
    };

    // First collect to prepare registry for subsequent transfers.
    impl->Collect();

    auto sensors = transferAndCollect("a", 3, "s1", 1);
    ASSERT_EQ(1, std::ssize(sensors.Counters));
    ASSERT_EQ(1, sensors.Counters["s1{a1=1;a2=2;a3=3}"]);
    ASSERT_EQ(3, tagRegistry.GetSize());

    sensors = transferAndCollect("b", 2, "s2", 2);
    ASSERT_EQ(1, std::ssize(sensors.Counters));
    ASSERT_EQ(2, sensors.Counters["s2{b1=1;b2=2}"]);
    ASSERT_EQ(5, tagRegistry.GetSize());

    sensors = transferAndCollect("b", 4, "s2", 3);
    ASSERT_EQ(1, std::ssize(sensors.Counters));
    ASSERT_EQ(3, sensors.Counters["s2{b1=1;b2=2;b3=3;b4=4}"]);
    ASSERT_EQ(7, tagRegistry.GetSize());
}

TEST(TSolomonRegistryTest, ExtensionTag)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d");

    auto c0 = r.WithTag("location_type", "store")
        .WithTag("medium", "ssd_blobs", -1)
        .WithTag("location_id", "store0", -1)
        .WithExtensionTag("device", "sdb", -1)
        .WithExtensionTag("model", "M5100", -1)
        .Counter("/bytes_read");
    c0.Increment();

    auto result = CollectSensors(impl);
    ASSERT_EQ(result.Counters.size(), 4u);

    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{}"));
    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{location_type=store}"));
    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{location_type=store;medium=ssd_blobs}"));
    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{location_type=store;medium=ssd_blobs;location_id=store0;device=sdb;model=M5100}"));
}

TEST_P(TRenameTest, RenameTag)
{
    bool recreate_sensor = GetParam();

    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler r(impl, "/d");

    auto tagSet = TTagSet{}
        .WithTag({"location_type", "store"})
        .WithTag({"medium", "ssd_blobs"}, -1)
        .WithTag({"location_id", "store0"}, -1);

    tagSet.AddExtensionTag({"device", "sdb"}, -1);
    tagSet.AddExtensionTag({"model", "M5100"}, -1);

    auto mediumTag = tagSet.AddDynamicTag(1);
    r = r.WithTags(tagSet);

    auto c0 = r.Counter("/bytes_read");
    c0.Increment();

    auto result = CollectSensors(impl);
    ASSERT_EQ(result.Counters.size(), 4u);

    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{}"));
    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{location_type=store}"));
    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{location_type=store;medium=ssd_blobs}"));
    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{location_type=store;medium=ssd_blobs;location_id=store0;device=sdb;model=M5100}"));

    r.RenameDynamicTag(mediumTag, "medium", "default");

    if (recreate_sensor) {
        c0 = r.Counter("/bytes_read");
    }

    c0.Increment();

    result = CollectSensors(impl);
    ASSERT_EQ(result.Counters.size(), 4u);

    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{}"));
    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{location_type=store}"));
    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{location_type=store;medium=default}"));
    ASSERT_TRUE(result.Counters.contains("yt.d.bytes_read{location_type=store;location_id=store0;device=sdb;model=M5100;medium=default}"));
}

INSTANTIATE_TEST_SUITE_P(
    TRenameTest,
    TRenameTest,
    ::testing::Values(false, true)
);

struct TBlinkingProducer
    : ISensorProducer
{
    bool Report = true;

    void CollectSensors(ISensorWriter* writer)
    {
        if (Report) {
            writer->AddCounter("/c", 1);
            writer->AddGauge("/g", 1);
        }

        Report = !Report;
    }
};

DEFINE_REFCOUNTED_TYPE(TBlinkingProducer)

TEST(TSolomonRegistryTest, ProducerRemoveSupport)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);

    TProfiler r(impl, "/d");

    auto p0 = New<TBlinkingProducer>();
    r.AddProducer("/no_remove", p0);

    auto p1 = New<TBlinkingProducer>();
    r.WithProducerRemoveSupport().AddProducer("/remove", p1);

    auto result = CollectSensors(impl);
    ASSERT_EQ(result.Counters.size(), 2u);
    ASSERT_EQ(result.Gauges.size(), 2u);

    result = CollectSensors(impl);
    ASSERT_EQ(result.Counters.size(), 1u);
    ASSERT_EQ(result.Gauges.size(), 1u);
}

////////////////////////////////////////////////////////////////////////////////

class TGaugeSummaryTriple
{
public:
    TGaugeSummaryTriple(TProfiler* profiler, const std::string& name, ESummaryPolicy policy)
        : First_(profiler->GaugeSummary(name, policy))
        , Second_(profiler->GaugeSummary(name, policy))
        , Third_(profiler->GaugeSummary(name, policy))
    { }

    void Update(double a, double b, double c)
    {
        First_.Update(a);
        Second_.Update(b);
        Third_.Update(c);
    }

private:
    TGauge First_;
    TGauge Second_;
    TGauge Third_;
};

class TOmitNameLabelSuffixTest
    : public ::testing::TestWithParam<bool>
{ };

INSTANTIATE_TEST_SUITE_P(
    TSolomonRegistry,
    TOmitNameLabelSuffixTest,
    testing::Values(false, true));

TEST_P(TOmitNameLabelSuffixTest, GaugeSummary)
{
    bool omitNameLabelSuffix = GetParam();

    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    TProfiler profiler(impl, "/d");

    ESummaryPolicy additionalPolicy = omitNameLabelSuffix ? ESummaryPolicy::OmitNameLabelSuffix : ESummaryPolicy::Default;

    TGaugeSummaryTriple all(&profiler, "all", ESummaryPolicy::All);
    TGaugeSummaryTriple sum(&profiler, "sum", ESummaryPolicy::Sum | additionalPolicy);
    TGaugeSummaryTriple min(&profiler, "min", ESummaryPolicy::Min | additionalPolicy);
    TGaugeSummaryTriple max(&profiler, "max", ESummaryPolicy::Max | additionalPolicy);
    TGaugeSummaryTriple avg(&profiler, "avg", ESummaryPolicy::Avg | additionalPolicy);

    all.Update(40, 20, 50);

    sum.Update(21, 31, 41);
    min.Update(1337, 32, 322);
    max.Update(22, 44, 11);
    avg.Update(55, 44, 22);

    auto result = CollectSensors(impl);
    auto& gauges = result.Gauges;
    auto& summaries = result.Summaries;

    ASSERT_NEAR(summaries["yt.dall{}"]->GetSum(), 110, 1e-6);
    ASSERT_NEAR(summaries["yt.dall{}"]->GetMin(), 20, 1e-6);
    ASSERT_NEAR(summaries["yt.dall{}"]->GetMax(), 50, 1e-6);
    ASSERT_NEAR(summaries["yt.dall{}"]->GetCount(), 3, 1e-6);

    ASSERT_NEAR(gauges[Format("yt.dsum%v{}", omitNameLabelSuffix ? "" : ".sum")], 93, 1e-6);
    ASSERT_NEAR(gauges[Format("yt.dmin%v{}", omitNameLabelSuffix ? "" : ".min")], 32, 1e-6);
    ASSERT_NEAR(gauges[Format("yt.dmax%v{}", omitNameLabelSuffix ? "" : ".max")], 44, 1e-6);
    ASSERT_NEAR(gauges[Format("yt.davg%v{}", omitNameLabelSuffix ? "" : ".avg")], 40 + 1 / 3.0, 1e-6);
}

struct TTagInfo
{
    std::string TagValue;
    std::string EncodedTagValue;
};

using TTagInfoMapping = THashMap<std::string, TTagInfo>;

TTagInfo GetLongTag()
{
    std::string longTag;
    longTag.reserve(210);
    for (int index = 0; index < 210; ++index) {
        longTag.append(1, 'a' + index % 26);
    }

    std::string longTagEncoded;
    longTagEncoded.reserve(200);
    longTagEncoded.append(longTag.begin(), 100).append("...");
    for (int index = 103; index < 200; ++index) {
        longTagEncoded.append(1, 'a' + (index - 103 + 9) % 26);
    }

    return {
        .TagValue = std::move(longTag),
        .EncodedTagValue = std::move(longTagEncoded),
    };
}

void CheckTags(const TSolomonRegistryPtr& registry, const TTagInfoMapping& tagInfoMapping)
{
    int tagNameLength = tagInfoMapping.begin()->first.size();

    TTagSet tagSet;
    for (const auto& [tagName, tagInfo] : tagInfoMapping) {
        ASSERT_EQ(ssize(tagName), tagNameLength);
        tagSet.AddTag({tagName, tagInfo.TagValue});
    }

    auto profiler = TProfiler(registry, "/debug")
        .WithTags(tagSet);
    auto c0 = profiler.Counter("/c");
    c0.Increment(1);

    auto result = CollectSensors(registry);

    for (const auto& label : result.Labels) {
        // Label has structure - "labelName=labelValue"
        auto tagName = label.substr(0, tagNameLength);
        auto tagValue = label.substr(tagNameLength + 1, label.size() - tagNameLength - 1);

        ASSERT_EQ(tagValue, tagInfoMapping.at(tagName).EncodedTagValue);
    }
}

TEST(TSolomonRegistryTest, IncorrectSolomonLabelsWeakPolicy)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    impl->SetLabelSanitizationPolicy(ELabelSanitizationPolicy::Weak);
std::string a(98, 'a');
    CheckTags(
        impl,
        {
            {
                "tag0",
                GetLongTag(),
            },
            {
                "tag1",
                {
                    .TagValue = "a\0aa|*?\"'\\`b\0b\xff"s,
                    .EncodedTagValue = "a%00aa|*?\"'\\`b%00b\xff",
                },
            },
            {
                "tag2",
                {
                    .TagValue = std::string(98, 'a')
                        .append(1, '\0')
                        .append(100, 'a')
                        .append(1, '\0')
                        .append(1, '\xff'),
                    .EncodedTagValue = std::string(98, 'a')
                        .append("%0...")
                        .append(93, 'a')
                        .append("%00")
                        .append(1, '\xff')
                },
            },
        });
}

TEST(TSolomonRegistryTest, IncorrectSolomonLabelsStrongPolicy)
{
    auto impl = New<TSolomonRegistry>();
    impl->SetWindowSize(12);
    impl->SetLabelSanitizationPolicy(ELabelSanitizationPolicy::Strong);

    CheckTags(
        impl,
        {
            {
                "tag0",
                GetLongTag(),
            },
            {
                "tag1",
                {
                    .TagValue = "a\0aa|*?\"'\\`bb\xff\0"s,
                    .EncodedTagValue = "a%00aa%7c%2a%3f%22%27%5c%60bb%ff%00",
                },
            },
            {
                "tag2",
                {
                    .TagValue = std::string(98, 'a')
                        .append(1, '\xff')
                        .append(100, 'a')
                        .append(1, '\0'),
                    .EncodedTagValue = std::string(98, 'a')
                        .append("%f...")
                        .append(94, 'a')
                        .append("%00"),
                },
            },
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
