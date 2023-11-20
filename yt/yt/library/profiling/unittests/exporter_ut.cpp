#include "yt/yt/library/profiling/solomon/registry.h"
#include <gtest/gtest.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

namespace NYT::NProfiling {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TSolomonExporter, MemoryLeak)
{
    auto registry = New<TSolomonRegistry>();
    auto counter = TProfiler{registry, "yt"}.Counter("/foo");

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;

    auto exporter = New<TSolomonExporter>(config, registry);
    auto json = exporter->ReadJson();
    EXPECT_FALSE(json);
    auto spack = exporter->ReadSpack();
    EXPECT_FALSE(spack);

    exporter->Start();

    Sleep(TDuration::Seconds(5));

    json = exporter->ReadJson();
    EXPECT_TRUE(json);
    EXPECT_FALSE(json->empty());
    spack = exporter->ReadSpack();
    EXPECT_TRUE(spack);
    EXPECT_FALSE(spack->empty());

    exporter->Stop();
}

TEST(TSolomonExporter, ReadJsonHistogram)
{
    auto registry = New<TSolomonRegistry>();
    auto hist = TProfiler{registry, "yt"}.TimeHistogram("/foo", TDuration::MilliSeconds(1), TDuration::Seconds(1));

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;

    auto exporter = NYT::New<TSolomonExporter>(config, registry);
    auto json = exporter->ReadJson();
    EXPECT_FALSE(json);

    exporter->Start();

    hist.Record(TDuration::MilliSeconds(500));
    hist.Record(TDuration::MilliSeconds(500));
    hist.Record(TDuration::MilliSeconds(500));
    Sleep(TDuration::Seconds(5));

    json = exporter->ReadJson();
    ASSERT_TRUE(json);
    Cerr << *json;

    exporter->Stop();
}

TEST(TSolomonExporter, ReadSpackHistogram)
{
    auto registry = New<TSolomonRegistry>();
    auto hist = TProfiler{registry, "yt"}.TimeHistogram("/foo", TDuration::MilliSeconds(1), TDuration::Seconds(1));

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;

    auto exporter = NYT::New<TSolomonExporter>(config, registry);
    auto spack = exporter->ReadSpack();
    EXPECT_FALSE(spack);

    exporter->Start();

    hist.Record(TDuration::MilliSeconds(500));
    hist.Record(TDuration::MilliSeconds(500));
    hist.Record(TDuration::MilliSeconds(500));
    Sleep(TDuration::Seconds(5));

    spack = exporter->ReadSpack();
    ASSERT_TRUE(spack);
    Cerr << *spack;

    exporter->Stop();
}

TEST(TSolomonExporter, ReadSensorsFilter)
{
    auto registry = New<TSolomonRegistry>();

    THashMap<TString, NYT::NProfiling::TShardConfigPtr> shards;
    auto AddShardConfig = [&shards] (TString shardName) -> void {
        auto shardConfig = New<TShardConfig>();
        shardConfig->GridStep = TDuration::Seconds(1);
        shardConfig->Filter = {shardName};

        shards.try_emplace(shardName, shardConfig);
    };
    AddShardConfig("/uptime/");
    AddShardConfig("/cache/");
    AddShardConfig("/requests/");

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;
    config->Shards = std::move(shards);

    auto exporter = NYT::New<TSolomonExporter>(config, registry);

    TGauge uptime = TProfiler("/uptime/", "", {}, registry).Gauge("uptime");
    TGauge cache_size = TProfiler("/cache/", "", {}, registry).Gauge("size");
    TGauge responses = TProfiler("/requests/", "", {}, registry).Gauge("responses");

    auto isSensorInShard = [&exporter] (const TString& shardName, const TString& sensor) -> bool {
        std::optional<TString> out = exporter->ReadJson({}, shardName);
        if (!out) {
            return false;
        }

        const TString& sensors = out.value();
        return sensors.Contains(sensor);
    };

    ASSERT_FALSE(isSensorInShard("/uptime/", "uptime"));
    ASSERT_FALSE(isSensorInShard("/cache/", "size"));
    ASSERT_FALSE(isSensorInShard("/requests/", "responses"));

    exporter->Start();

    uptime.Update(42);
    cache_size.Update(69);

    Sleep(TDuration::Seconds(5));

    // uptime
    ASSERT_TRUE(isSensorInShard("/uptime/", "uptime"));
    ASSERT_FALSE(isSensorInShard("/uptime/", "size"));
    ASSERT_FALSE(isSensorInShard("/uptime/", "responses"));

    // cache
    ASSERT_FALSE(isSensorInShard("/cache/", "uptime"));
    ASSERT_TRUE(isSensorInShard("/cache/", "size"));
    ASSERT_FALSE(isSensorInShard("/cache/", "responses"));

    // requests
    ASSERT_FALSE(isSensorInShard("/requests/", "uptime"));
    ASSERT_FALSE(isSensorInShard("/requests/", "size"));
    ASSERT_TRUE(isSensorInShard("/requests/", "responses"));

    exporter->Stop();
}

TEST(TSolomonExporter, ReadSensorsStripSensorsOption)
{
    auto registry = New<TSolomonRegistry>();

    THashMap<TString, NYT::NProfiling::TShardConfigPtr> shards;
    auto AddShardConfig = [&shards] (TString shardName) -> void {
        auto shardConfig = New<TShardConfig>();
        shardConfig->GridStep = TDuration::Seconds(1);
        shardConfig->Filter = {shardName};

        shards.try_emplace(shardName, shardConfig);
    };
    AddShardConfig("/uptime/");

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;
    config->Shards = std::move(shards);

    auto exporter = NYT::New<TSolomonExporter>(config, registry);

    TGauge uptime = TProfiler("/uptime/", "", {}, registry).Gauge("uptime");

    exporter->Start();

    uptime.Update(42);

    Sleep(TDuration::Seconds(5));

    // WO Strip option
    std::optional<TString> out = exporter->ReadJson({}, "/uptime/");
    ASSERT_TRUE(out);

    TString& sensors = out.value();
    ASSERT_TRUE(sensors.Contains("uptime"));
    ASSERT_TRUE(sensors.Contains("uptime.")); // not "/uptime/" Reason: sensor rename

    // With Strip option
    TReadOptions options;
    options.StripSensorsNamePrefix = true;
    out = exporter->ReadJson(options, "/uptime/");
    ASSERT_TRUE(out);

    sensors = out.value();
    ASSERT_TRUE(sensors.Contains("uptime"));
    ASSERT_FALSE(sensors.Contains("uptime."));

    exporter->Stop();
}

////////////////////////////////////////////////////////////////////////////////

class TSummaryPolicyTest
    : public ::testing::TestWithParam<ESummaryPolicy>
{ };

INSTANTIATE_TEST_SUITE_P(
    TSolomonExporter,
    TSummaryPolicyTest,
    testing::Values(
        ESummaryPolicy::All | ESummaryPolicy::Sum,
        ESummaryPolicy::All | ESummaryPolicy::Max,
        ESummaryPolicy::All | ESummaryPolicy::Min,
        ESummaryPolicy::All | ESummaryPolicy::Avg,
        ESummaryPolicy::All | ESummaryPolicy::OmitNameLabelSuffix,
        ESummaryPolicy::Max | ESummaryPolicy::Avg | ESummaryPolicy::OmitNameLabelSuffix));

TEST_P(TSummaryPolicyTest, InvalidReadOptions)
{
    auto registry = New<TSolomonRegistry>();
    TProfiler profiler(registry, "yt");

    auto config = New<TSolomonExporterConfig>();
    config->ExportSummaryAsMax = false;
    config->EnableSelfProfiling = false;
    config->Shards.emplace("yt", New<TShardConfig>());

    auto exporter = New<TSolomonExporter>(config, registry);
    exporter->Start();

    Sleep(TDuration::Seconds(5));

    ASSERT_THROW(exporter->ReadJson({.SummaryPolicy = GetParam()}, "yt"), TErrorException);
    exporter->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
