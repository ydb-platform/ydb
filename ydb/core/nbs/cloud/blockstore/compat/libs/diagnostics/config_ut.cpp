#include "config.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

using NYdb::NBS::HasField;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TDiagnosticsConfig CreateConfig(const TString& protoText)
{
    NProto::TDiagnosticsConfig config;
    NYdb::NBS::ParseProtoTextFromStringRobust(protoText, config);
    return config;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiagnosticsConfigTest)
{
    Y_UNIT_TEST(TestBooleanFieldExplicitTrue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
ReportHistogramAsSingleCounter: true
ReportHistogramAsMultipleCounters: true
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "ReportHistogramAsSingleCounter"));
        UNIT_ASSERT(HasField(protoConfig, "ReportHistogramAsMultipleCounters"));

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsSingleCounter(),
            true);
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsMultipleCounters(),
            true);
    }

    Y_UNIT_TEST(TestBooleanFieldExplicitFalse)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
ReportHistogramAsSingleCounter: false
ReportHistogramAsMultipleCounters: false
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "ReportHistogramAsSingleCounter"));
        UNIT_ASSERT(HasField(protoConfig, "ReportHistogramAsMultipleCounters"));

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsSingleCounter(),
            false);
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsMultipleCounters(),
            false);
    }

    Y_UNIT_TEST(TestBooleanFieldDelaultValues)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig("");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(!HasField(protoConfig, "ReportHistogramAsSingleCounter"));
        UNIT_ASSERT(
            !HasField(protoConfig, "ReportHistogramAsMultipleCounters"));

        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsSingleCounter(),
            false);
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetReportHistogramAsMultipleCounters(),
            true);
    }

    Y_UNIT_TEST(TestStringFieldExplicitValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
CpuWaitFilename: "override.wait"
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "CpuWaitFilename"));
        UNIT_ASSERT_VALUES_EQUAL(config.GetCpuWaitFilename(), "override.wait");
    }

    Y_UNIT_TEST(TestStringFieldEmptyValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
CpuWaitFilename: ""
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "CpuWaitFilename"));
        UNIT_ASSERT_VALUES_EQUAL(config.GetCpuWaitFilename(), "");
    }

    Y_UNIT_TEST(TestStringFieldDefaultValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig("");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(!HasField(protoConfig, "CpuWaitFilename"));
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetCpuWaitFilename(),
            "/sys/fs/cgroup/cpu/system.slice/nbs.service/cpuacct.wait");
    }

    Y_UNIT_TEST(TestRepeatedFieldExplicitValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig(R"(
CloudIdsWithStrictSLA: "cloud1"
CloudIdsWithStrictSLA: "cloud2"
CloudIdsWithStrictSLA: "cloud3"
)");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(HasField(protoConfig, "CloudIdsWithStrictSLA"));
        UNIT_ASSERT_VALUES_EQUAL(config.GetCloudIdsWithStrictSLA().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetCloudIdsWithStrictSLA()[0],
            "cloud1");
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetCloudIdsWithStrictSLA()[1],
            "cloud2");
        UNIT_ASSERT_VALUES_EQUAL(
            config.GetCloudIdsWithStrictSLA()[2],
            "cloud3");
    }

    Y_UNIT_TEST(TestRepeatedFieldDefaultValue)
    {
        NProto::TDiagnosticsConfig protoConfig = CreateConfig("");
        TDiagnosticsConfig config(protoConfig);

        UNIT_ASSERT(!HasField(protoConfig, "CloudIdsWithStrictSLA"));
        UNIT_ASSERT_VALUES_EQUAL(config.GetCloudIdsWithStrictSLA().size(), 0);
    }
}

}   // namespace NCloud::NBlockStore
