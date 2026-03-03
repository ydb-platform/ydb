#include <gtest/gtest.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/library/profiling/solomon/config.h>
#include <yt/yt/library/profiling/solomon/exporter.h>
#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/json/yson/json2yson.h>

namespace NYT::NProfiling {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

auto GetSensors(const std::string& json)
{
    auto yson = NYson::TYsonString(NJson2Yson::SerializeJsonValueAsYson(NJson::ReadJsonFastTree(json)));
    auto sensors = NYTree::ConvertToNode(yson)->AsMap()->GetChildOrThrow("sensors");
    return sensors->AsList()->GetChildren();
}

NYTree::INodePtr GetSensorByName(const std::vector<NYTree::INodePtr>& sensors, std::string_view sensorName)
{
    for (const auto& sensor : sensors) {
        auto labels = sensor->AsMap()->GetChildOrThrow("labels")->AsMap();
        if (labels->GetChildValueOrThrow<std::string>("sensor") == sensorName) {
            return sensor;
        }
    }
    THROW_ERROR_EXCEPTION("Sensor %Qv not found", sensorName);
}

std::optional<std::string> GetLabel(const NYTree::INodePtr& sensor, std::string_view labelName)
{
    auto labels = sensor->AsMap()->GetChildOrThrow("labels")->AsMap();
    return labels->FindChildValue<std::string>(std::string(labelName));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSolomonExporterTest, MemoryLeak)
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

TEST(TSolomonExporterTest, MemoryLeakWithSelfProfiling)
{
    auto registry = New<TSolomonRegistry>();
    auto counter = TProfiler{registry, "yt"}.Counter("/foo");

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = true;

    auto exporter = New<TSolomonExporter>(config, registry);
}

TEST(TSolomonExporterTest, ReadJsonHistogram)
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

TEST(TSolomonExporterTest, SplitRateHistogramIntoGauges)
{
    auto registry = New<TSolomonRegistry>();
    auto hist = TProfiler{registry, "yt"}.TimeHistogram("/foo", TDuration::MilliSeconds(1), TDuration::Seconds(1));

    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(5);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;

    auto exporter = NYT::New<TSolomonExporter>(config, registry);
    auto json = exporter->ReadJson();
    EXPECT_FALSE(json);

    exporter->Start();

    hist.Record(TDuration::MilliSeconds(500));
    hist.Record(TDuration::MilliSeconds(500));
    hist.Record(TDuration::MilliSeconds(500));
    auto alignedNow = TInstant::Seconds((TInstant::Now().Seconds() / 5) * 5);
    // Wait one second after the next tick to ensure the value is ready.
    SleepUntil(alignedNow + TDuration::Seconds(6));

    auto options = TReadOptions{
        .ConvertCountersToRateGauge = true,
        .RateDenominator = 5.0,
        .SplitRateHistogramIntoGauges = true,
    };
    json = exporter->ReadJson(options);
    ASSERT_TRUE(json);

    NJson::TJsonValue jsonValue;
    NJson::ReadJsonTree(*json, &jsonValue, true);
    auto sensors = jsonValue["sensors"].GetArraySafe();
    EraseIf(sensors, [] (const auto& sensor) {
        return sensor["labels"]["sensor"].GetStringSafe() != "ytyt.foo";
    });
    THashSet<TString> buckets = {"0.001", "0.002", "0.004", "0.008", "0.016", "0.032", "0.064", "0.125", "0.25", "0.5", "1", "inf"};
    ASSERT_EQ(buckets.size(), sensors.size());
    for (const auto& sensor : sensors) {
        auto bin = sensor["labels"]["bin"].GetStringSafe();
        ASSERT_TRUE(buckets.contains(bin));
        if (sensor["labels"]["bin"].GetString() == "0.5") {
            ASSERT_DOUBLE_EQ(sensor["value"].GetDoubleSafe(), 3.0 / 5.0);
        }
    }
    exporter->Stop();
}

TEST(TSolomonExporterTest, ReadSpackHistogram)
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

TEST(TSolomonExporterTest, ReadSensorsFilter)
{
    auto registry = New<TSolomonRegistry>();

    THashMap<std::string, NYT::NProfiling::TShardConfigPtr> shards;
    auto AddShardConfig = [&shards] (const std::string& shardName) {
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

    auto isSensorInShard = [&exporter] (const std::string& shardName, const std::string& sensor) -> bool {
        std::optional<std::string> out = exporter->ReadJson({}, shardName);
        if (!out) {
            return false;
        }

        const std::string& sensors = out.value();
        return sensors.contains(sensor);
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

TEST(TSolomonExporterTest, ReadSensorsStripSensorsOption)
{
    auto registry = New<TSolomonRegistry>();

    THashMap<std::string, NYT::NProfiling::TShardConfigPtr> shards;
    auto AddShardConfig = [&shards] (const std::string& shardName) {
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
    std::optional<std::string> out = exporter->ReadJson({}, "/uptime/");
    ASSERT_TRUE(out);

    std::string& sensors = out.value();
    ASSERT_TRUE(sensors.contains("uptime"));
    ASSERT_TRUE(sensors.contains("uptime.")); // not "/uptime/" Reason: sensor rename

    // With Strip option
    TReadOptions options;
    options.StripSensorsNamePrefix = true;
    out = exporter->ReadJson(options, "/uptime/");
    ASSERT_TRUE(out);

    sensors = out.value();
    ASSERT_TRUE(sensors.contains("uptime"));
    ASSERT_FALSE(sensors.contains("uptime."));

    exporter->Stop();
}

TEST(TSolomonExporterTest, ReadSensorsSolomonAggregates)
{
    auto registry = New<TSolomonRegistry>();
    auto config = New<TSolomonExporterConfig>();
    config->GridStep = TDuration::Seconds(1);
    config->EnableCoreProfilingCompatibility = true;
    config->EnableSelfProfiling = false;
    config->ReportBuildInfo = false;
    config->ReportKernelVersion = false;
    config->ReportRestart = false;

    config->MarkAggregates = true;
    config->ExportSummaryAsSum = true;
    config->ExportSummaryAsMax = true;
    config->ExportSummaryAsMin = true;
    config->ExportSummaryAsAvg = true;
    config->Shards.try_emplace("default", New<TShardConfig>());

    auto exporter = NYT::New<TSolomonExporter>(config, registry);

    auto profiler = TProfiler("", "", {}, registry);
    auto summaryDefault = profiler.Summary("summary");
    auto summaryMax = profiler.Summary("max_only", ESummaryPolicy::Max | ESummaryPolicy::OmitNameLabelSuffix);

    auto globalProfiler = profiler.WithGlobal();
    auto sumGlobal = globalProfiler.Summary("sum_global");
    auto maxGlobal = globalProfiler
        .WithMemOnly()
        .Summary("max_global", ESummaryPolicy::Max | ESummaryPolicy::OmitNameLabelSuffix);

    exporter->Start();

    summaryDefault.Record(42);
    summaryDefault.Record(21);

    summaryMax.Record(42);
    summaryMax.Record(21);

    sumGlobal.Record(31);
    sumGlobal.Record(4);

    maxGlobal.Record(16);
    maxGlobal.Record(6);

    Sleep(TDuration::Seconds(1));

    TReadOptions readOptions;
    readOptions.MarkAggregates = true;
    readOptions.SummaryPolicy = config->GetSummaryPolicy();

    {
        readOptions.EnableSolomonAggregates = true;
        readOptions.ExportGlobalsAsMemOnly = true;

        std::optional<std::string> out = exporter->ReadJson(readOptions);
        ASSERT_TRUE(out);
        auto sensors = GetSensors(*out);

        auto checkSensor = [&](std::string_view name, bool global, std::string_view aggr) {
            auto sensor = GetSensorByName(sensors, name);
            EXPECT_EQ(aggr, GetLabel(sensor, "yt_aggr"));
            EXPECT_NE("", GetLabel(sensor, "host"));
            if (global) {
                EXPECT_TRUE(sensor->AsMap()->GetChildValueOrDefault<bool>("memOnly", false));
            } else {
                EXPECT_FALSE(sensor->AsMap()->GetChildValueOrDefault<bool>("memOnly", false));
            }
        };

        checkSensor("summary.sum"sv, false, "sum");
        checkSensor("summary.max"sv, false, "max");
        checkSensor("summary.min"sv, false, "min");
        checkSensor("summary.avg"sv, false, "avg");
        checkSensor("max_only"sv, false, "max");

        checkSensor("max_global"sv, true, "max");
        checkSensor("sum_global.sum"sv, true, "sum");
        checkSensor("sum_global.max"sv, true, "max");
        checkSensor("sum_global.min"sv, true, "min");
        checkSensor("sum_global.avg"sv, true, "avg");
    }

    // Disable solomon aggregates.
    {
        readOptions.EnableSolomonAggregates = false;
        readOptions.ExportGlobalsAsMemOnly = false;

        std::optional<std::string> out = exporter->ReadJson(readOptions);
        ASSERT_TRUE(out);
        auto sensors = GetSensors(*out);

        EXPECT_EQ("1", GetLabel(GetSensorByName(sensors, "summary.sum"sv), "yt_aggr"));
        EXPECT_EQ(std::nullopt, GetLabel(GetSensorByName(sensors, "summary.max"sv), "yt_aggr"));
        EXPECT_EQ(std::nullopt, GetLabel(GetSensorByName(sensors, "summary.min"sv), "yt_aggr"));
        EXPECT_EQ(std::nullopt, GetLabel(GetSensorByName(sensors, "summary.avg"sv), "yt_aggr"));
        EXPECT_EQ(std::nullopt, GetLabel(GetSensorByName(sensors, "sum_global.sum"sv), "yt_aggr"));

        EXPECT_TRUE(GetSensorByName(sensors, "max_global"sv)->AsMap()->GetChildValueOrDefault<bool>("memOnly", false));
        EXPECT_FALSE(GetSensorByName(sensors, "summary.sum"sv)->AsMap()->GetChildValueOrDefault<bool>("memOnly", false));

        EXPECT_NE("", GetLabel(GetSensorByName(sensors, "summary.sum"sv), "host"));
        EXPECT_EQ("", GetLabel(GetSensorByName(sensors, "sum_global.sum"sv), "host"));
    }

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
