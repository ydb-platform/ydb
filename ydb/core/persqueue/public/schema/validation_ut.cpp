#include "common.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ::NSchema {

Y_UNIT_TEST_SUITE(SchemaValidation) {

Y_UNIT_TEST(GetWorkingDirAndName) {
    {
        auto [dir, name] = GetWorkingDirAndName("/Root/db/topic");
        UNIT_ASSERT_VALUES_EQUAL(dir, "/Root/db");
        UNIT_ASSERT_VALUES_EQUAL(name, "topic");
    }
    {
        auto [dir, name] = GetWorkingDirAndName("/Root/topic");
        UNIT_ASSERT_VALUES_EQUAL(dir, "/Root");
        UNIT_ASSERT_VALUES_EQUAL(name, "topic");
    }
}

Y_UNIT_TEST(CheckRetentionPeriod) {
    {
        auto r = CheckRetentionPeriod(3600);
        UNIT_ASSERT(r.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*r, 3600);
    }
    {
        auto r = CheckRetentionPeriod(0);
        UNIT_ASSERT(!r.has_value());
        UNIT_ASSERT_STRING_CONTAINS(r.error(), "positive");
    }
    {
        auto r = CheckRetentionPeriod(-10);
        UNIT_ASSERT(!r.has_value());
        UNIT_ASSERT_STRING_CONTAINS(r.error(), "positive");
    }
}

Y_UNIT_TEST(ConvertPositiveDuration) {
    {
        google::protobuf::Duration d;
        d.set_seconds(5);
        auto r = ConvertPositiveDuration(d);
        UNIT_ASSERT(r.has_value());
        UNIT_ASSERT_VALUES_EQUAL(r->Seconds(), 5u);
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(-1);
        auto r = ConvertPositiveDuration(d);
        UNIT_ASSERT(!r.has_value());
        UNIT_ASSERT_STRING_CONTAINS(r.error(), "negative");
    }
}

Y_UNIT_TEST(ValidateDuration) {
    {
        google::protobuf::Duration d;
        d.set_seconds(1);
        d.set_nanos(0);
        UNIT_ASSERT(ValidateDuration(d, "timeout"));
    }
    {
        google::protobuf::Duration d;
        d.set_seconds(-1);
        auto r = ValidateDuration(d, "timeout");
        UNIT_ASSERT(!r);
        UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "timeout");
    }
}

Y_UNIT_TEST(ValidatePartitionStrategyBounds) {
    NKikimrPQ::TPQTabletConfig config;
    auto* strategy = config.MutablePartitionStrategy();
    strategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
    strategy->SetMinPartitionCount(10);
    strategy->SetMaxPartitionCount(5);
    strategy->SetScaleThresholdSeconds(30);
    strategy->SetScaleUpPartitionWriteSpeedThresholdPercent(80);
    strategy->SetScaleDownPartitionWriteSpeedThresholdPercent(20);

    auto r = ValidatePartitionStrategy(config);
    UNIT_ASSERT(!r);
    UNIT_ASSERT_VALUES_EQUAL(r.GetStatus(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_STRING_CONTAINS(r.GetErrorMessage(), "Max active partitions");
}

} // Y_UNIT_TEST_SUITE(SchemaValidation)

} // namespace NKikimr::NPQ::NSchema
