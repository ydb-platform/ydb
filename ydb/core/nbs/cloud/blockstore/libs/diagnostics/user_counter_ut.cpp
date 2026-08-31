#include "user_counter.h"

#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/histogram_types.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/text/text.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/gtest/gtest.h>

namespace NYdb::NBS::NBlockStore::NUserCounter {

using namespace NYdb::NBS::NStorage::NUserStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

NJson::TJsonValue GetValue(const auto& object, const auto& name)
{
    for (const auto& data: object["sensors"].GetArray()) {
        if (data["labels"]["name"] == name) {
            if (!data.Has("hist")) {
                return data["value"];
            }
        }
    }
    EXPECT_TRUE(false) << "Value not found " << name;
    return NJson::TJsonValue{};
}

NJson::TJsonValue
GetHist(const auto& object, const auto& name, const auto& valueName)
{
    for (const auto& data: object["sensors"].GetArray()) {
        if (data["labels"]["name"] == name) {
            if (data.Has("hist")) {
                return data["hist"][valueName];
            }
        }
    }

    EXPECT_TRUE(false) << "Value not found " << name << "/" << valueName;
    return NJson::TJsonValue{};
}

void ValidateJsons(
    const NJson::TJsonValue& expectedJson,
    const NJson::TJsonValue& actualJson)
{
    for (const auto& jsonValue: expectedJson["sensors"].GetArray()) {
        const TString name = jsonValue["labels"]["name"].GetString();

        if (jsonValue.Has("hist")) {
            for (const auto* valueName: {"bounds", "buckets", "inf"}) {
                EXPECT_EQ(
                    NJson::WriteJson(GetHist(expectedJson, name, valueName)),
                    NJson::WriteJson(GetHist(actualJson, name, valueName)))
                    << " for " << name << "/" << valueName;
            }
        } else {
            EXPECT_EQ(
                NJson::WriteJson(GetValue(expectedJson, name)),
                NJson::WriteJson(GetValue(actualJson, name)))
                << " for " << name;
        }
    }
}

void ValidateTestResult(IUserCounterSupplier* supplier, const TString& resource)
{
    const TString testResult = NResource::Find(resource);
    NJson::TJsonValue testJson = NJson::ReadJsonFastTree(testResult, true);

    TStringStream jsonOut;
    auto encoder = NMonitoring::EncoderJson(&jsonOut);
    supplier->Accept(TInstant::Seconds(12), encoder.Get());

    NJson::TJsonValue resultJson = NJson::ReadJsonFastTree(jsonOut.Str(), true);

    ValidateJsons(testJson, resultJson);
}

////////////////////////////////////////////////////////////////////////////////

struct THistogramTestConfiguration
{
    bool SetUnits;
    bool SetHistogramSingleCounter;
    bool SetHistogramMultipleCounter;
    bool UseMsUnitsForTimeHistogram;
};

void SetTimeHistogramCountersUs(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
    const TString& histName,
    const THistogramTestConfiguration& config)
{
    auto subgroup = counters->GetSubgroup("histogram", histName);
    if (config.SetUnits) {
        subgroup = subgroup->GetSubgroup("units", "usec");
    }

    if (config.SetHistogramSingleCounter) {
        const auto& buckets = TRequestUsTimeBuckets::Buckets;
        const auto& bounds = ConvertToHistBounds(buckets);
        auto histogram = subgroup->GetHistogram(
            histName,
            NMonitoring::ExplicitHistogram(bounds));
        histogram->Collect(1., 1);
        histogram->Collect(100., 2);
        histogram->Collect(200., 3);
        histogram->Collect(300., 4);
        histogram->Collect(400., 5);
        histogram->Collect(500., 6);
        histogram->Collect(600., 7);
        histogram->Collect(700., 8);
        histogram->Collect(800., 9);
        histogram->Collect(900., 0);
        histogram->Collect(1000., 1);
        histogram->Collect(2000., 2);
        histogram->Collect(5000., 3);
        histogram->Collect(10000., 4);
        histogram->Collect(20000., 5);
        histogram->Collect(50000., 6);
        histogram->Collect(100000., 7);
        histogram->Collect(200000., 8);
        histogram->Collect(500000., 9);
        histogram->Collect(1000000., 10);
        histogram->Collect(2000000., 11);
        histogram->Collect(5000000., 12);
        histogram->Collect(10000000., 13);
        histogram->Collect(35000000., 14);
        histogram->Collect(100000000., 15);   // Inf
    }

    if (config.SetHistogramMultipleCounter) {
        subgroup->GetCounter("1")->Set(1);
        subgroup->GetCounter("100")->Set(2);
        subgroup->GetCounter("200")->Set(3);
        subgroup->GetCounter("300")->Set(4);
        subgroup->GetCounter("400")->Set(5);
        subgroup->GetCounter("500")->Set(6);
        subgroup->GetCounter("600")->Set(7);
        subgroup->GetCounter("700")->Set(8);
        subgroup->GetCounter("800")->Set(9);
        subgroup->GetCounter("900")->Set(0);
        subgroup->GetCounter("1000")->Set(1);
        subgroup->GetCounter("2000")->Set(2);
        subgroup->GetCounter("5000")->Set(3);
        subgroup->GetCounter("10000")->Set(4);
        subgroup->GetCounter("20000")->Set(5);
        subgroup->GetCounter("50000")->Set(6);
        subgroup->GetCounter("100000")->Set(7);
        subgroup->GetCounter("200000")->Set(8);
        subgroup->GetCounter("500000")->Set(9);
        subgroup->GetCounter("1000000")->Set(10);
        subgroup->GetCounter("2000000")->Set(11);
        subgroup->GetCounter("5000000")->Set(12);
        subgroup->GetCounter("10000000")->Set(13);
        subgroup->GetCounter("35000000")->Set(14);
        subgroup->GetCounter("Inf")->Set(15);
    }
}

void SetTimeHistogramCountersMs(
    const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
    const TString& histName,
    const THistogramTestConfiguration& config)
{
    auto subgroup = counters->GetSubgroup("histogram", histName);
    if (config.SetUnits) {
        subgroup = subgroup->GetSubgroup("units", "msec");
    }

    if (config.SetHistogramSingleCounter) {
        const auto& buckets = TRequestMsTimeBuckets::Buckets;
        const auto& bounds = ConvertToHistBounds(buckets);
        auto histogram = subgroup->GetHistogram(
            histName,
            NMonitoring::ExplicitHistogram(bounds));
        histogram->Collect(0.001, 1);
        histogram->Collect(0.1, 2);
        histogram->Collect(0.2, 3);
        histogram->Collect(0.3, 4);
        histogram->Collect(0.4, 5);
        histogram->Collect(0.5, 6);
        histogram->Collect(0.6, 7);
        histogram->Collect(0.7, 8);
        histogram->Collect(0.8, 9);
        histogram->Collect(0.9, 0);
        histogram->Collect(1., 1);
        histogram->Collect(2., 2);
        histogram->Collect(5., 3);
        histogram->Collect(10., 4);
        histogram->Collect(20., 5);
        histogram->Collect(50., 6);
        histogram->Collect(100., 7);
        histogram->Collect(200., 8);
        histogram->Collect(500., 9);
        histogram->Collect(1000., 10);
        histogram->Collect(2000., 11);
        histogram->Collect(5000., 12);
        histogram->Collect(10000., 13);
        histogram->Collect(35000., 14);
        histogram->Collect(100000., 15);   // Inf
    }

    if (config.SetHistogramMultipleCounter) {
        subgroup->GetCounter("0.001ms")->Set(1);
        subgroup->GetCounter("0.1ms")->Set(2);
        subgroup->GetCounter("0.2ms")->Set(3);
        subgroup->GetCounter("0.3ms")->Set(4);
        subgroup->GetCounter("0.4ms")->Set(5);
        subgroup->GetCounter("0.5ms")->Set(6);
        subgroup->GetCounter("0.6ms")->Set(7);
        subgroup->GetCounter("0.7ms")->Set(8);
        subgroup->GetCounter("0.8ms")->Set(9);
        subgroup->GetCounter("0.9ms")->Set(0);
        subgroup->GetCounter("1ms")->Set(1);
        subgroup->GetCounter("2ms")->Set(2);
        subgroup->GetCounter("5ms")->Set(3);
        subgroup->GetCounter("10ms")->Set(4);
        subgroup->GetCounter("20ms")->Set(5);
        subgroup->GetCounter("50ms")->Set(6);
        subgroup->GetCounter("100ms")->Set(7);
        subgroup->GetCounter("200ms")->Set(8);
        subgroup->GetCounter("500ms")->Set(9);
        subgroup->GetCounter("1000ms")->Set(10);
        subgroup->GetCounter("2000ms")->Set(11);
        subgroup->GetCounter("5000ms")->Set(12);
        subgroup->GetCounter("10000ms")->Set(13);
        subgroup->GetCounter("35000ms")->Set(14);
        subgroup->GetCounter("Inf")->Set(15);
    }
}

class TUserWrapperTest
    : public testing::TestWithParam<std::tuple<bool, bool, bool, bool>>
{
public:
    TUserWrapperTest() = default;
    ~TUserWrapperTest() override = default;

    static THistogramTestConfiguration GetHistogramTestConfiguration()
    {
        return THistogramTestConfiguration{
            .SetUnits = std::get<0>(GetParam()),
            .SetHistogramSingleCounter = std::get<1>(GetParam()),
            .SetHistogramMultipleCounter = std::get<2>(GetParam()),
            .UseMsUnitsForTimeHistogram = std::get<3>(GetParam())};
    }

    static EHistogramCounterOptions GetHistogramCounterOptions()
    {
        const auto histConfig = GetHistogramTestConfiguration();
        EHistogramCounterOptions histogramCounterOptions;
        if (histConfig.UseMsUnitsForTimeHistogram) {
            histogramCounterOptions |=
                EHistogramCounterOption::UseMsUnitsForTimeHistogram;
        }
        if (histConfig.SetHistogramSingleCounter) {
            histogramCounterOptions |=
                EHistogramCounterOption::ReportSingleCounter;
        }
        if (histConfig.SetHistogramMultipleCounter) {
            histogramCounterOptions |=
                EHistogramCounterOption::ReportMultipleCounters;
        }
        return histogramCounterOptions;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using namespace NMonitoring;

TEST_P(TUserWrapperTest, UserServerVolumeInstanceTests)
{
    const auto histConfig = GetHistogramTestConfiguration();
    if (histConfig.SetHistogramSingleCounter == false &&
        histConfig.SetHistogramMultipleCounter == false)
    {
        return;
    }

    auto setCounters = [](const NMonitoring::TDynamicCounterPtr& stats,
                          const TString& name,
                          const THistogramTestConfiguration& histConfig)
    {
        auto request = stats->GetSubgroup("request", name);
        request->GetCounter("Count")->Set(1);
        request->GetCounter("MaxCount")->Set(10);
        request->GetCounter("Errors/Fatal")->Set(2);
        request->GetCounter("RequestBytes")->Set(3);
        request->GetCounter("MaxRequestBytes")->Set(30);
        request->GetCounter("InProgress")->Set(4);
        request->GetCounter("MaxInProgress")->Set(40);
        request->GetCounter("InProgressBytes")->Set(5);
        request->GetCounter("MaxInProgressBytes")->Set(50);

        if (histConfig.UseMsUnitsForTimeHistogram) {
            SetTimeHistogramCountersMs(request, "Time", histConfig);
        } else {
            SetTimeHistogramCountersUs(request, "Time", histConfig);
        }
    };

    struct TTestConfiguration
    {
        bool ReportZeroBlocksMetrics;
        TString Resource;
    };

    std::vector<TTestConfiguration> testConfigurations = {
        {true, "user_server_volume_instance_test"},
        {false, "user_server_volume_instance_skip_zero_blocks_test"}};

    for (const auto& config: testConfigurations) {
        auto stats = MakeIntrusive<TDynamicCounters>();
        setCounters(stats, "ReadBlocks", histConfig);
        setCounters(stats, "WriteBlocks", histConfig);
        setCounters(stats, "ZeroBlocks", histConfig);

        auto supplier = CreateUserCounterSupplier();
        RegisterServerVolumeInstance(
            *supplier,
            "cloudId",
            "folderId",
            "diskId",
            "instanceId",
            config.ReportZeroBlocksMetrics,
            GetHistogramCounterOptions(),
            stats);

        ValidateTestResult(supplier.get(), config.Resource);
    }
}

TEST_P(TUserWrapperTest, UserServiceVolumeInstanceTests)
{
    const auto histConfig = GetHistogramTestConfiguration();
    if (histConfig.SetHistogramSingleCounter == false &&
        histConfig.SetHistogramMultipleCounter == false)
    {
        return;
    }

    auto makeCounters = [](const NMonitoring::TDynamicCounterPtr& stats,
                           const TString& name,
                           const THistogramTestConfiguration& histConfig)
    {
        stats->GetCounter("UsedQuota")->Set(1);
        stats->GetCounter("MaxUsedQuota")->Set(10);

        auto request = stats->GetSubgroup("request", name);
        SetTimeHistogramCountersUs(request, "ThrottlerDelay", histConfig);
    };

    auto stats = MakeIntrusive<TDynamicCounters>();

    makeCounters(stats, "ReadBlocks", histConfig);
    makeCounters(stats, "WriteBlocks", histConfig);
    makeCounters(stats, "ZeroBlocks", histConfig);

    auto supplier = CreateUserCounterSupplier();
    RegisterServiceVolume(
        *supplier,
        "cloudId",
        "folderId",
        "diskId",
        GetHistogramCounterOptions(),
        stats);
    ValidateTestResult(supplier.get(), "user_service_volume_instance_test");
}

INSTANTIATE_TEST_SUITE_P(
    ,
    TUserWrapperTest,
    testing::Combine(
        testing::Bool(),
        testing::Bool(),
        testing::Bool(),
        testing::Bool()),
    [](const testing::TestParamInfo<TUserWrapperTest::ParamType>& info)
        -> std::string
    {
        const auto name = TStringBuilder() << std::get<0>(info.param) << "_"
                                           << std::get<1>(info.param) << "_"
                                           << std::get<2>(info.param) << "_"
                                           << std::get<3>(info.param);
        return name;
    });

}   // namespace NYdb::NBS::NBlockStore::NUserCounter
