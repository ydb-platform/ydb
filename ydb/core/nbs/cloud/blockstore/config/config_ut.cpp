#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageConfigTest)
{
    Y_UNIT_TEST(ShouldApplyDefaultsForEmptyProto)
    {
        TStorageConfig config{NProto::TStorageServiceConfig{}};

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(1000),
            config.GetTraceSamplePeriod());
        UNIT_ASSERT_VALUES_EQUAL(3u, config.GetSyncRequestsBatchSize());
        UNIT_ASSERT_VALUES_EQUAL(524288u, config.GetStripeSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(700),
            config.GetWriteHedgingDelay());
        UNIT_ASSERT_VALUES_EQUAL("ddp1", config.GetDDiskPoolName());
        UNIT_ASSERT_VALUES_EQUAL(
            "ddp1",
            config.GetPersistentBufferDDiskPoolName());
        UNIT_ASSERT_VALUES_EQUAL(134217728, config.GetVChunkSize());
    }

    Y_UNIT_TEST(ShouldUseExplicitProtoValuesWhenSet)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetTraceSamplePeriod(42);
        proto.SetSyncRequestsBatchSize(7);
        proto.SetStripeSize(8192);
        proto.SetWriteHedgingDelay(99);
        proto.SetVChunkSize(33554432);

        TStorageConfig config{std::move(proto)};

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(42),
            config.GetTraceSamplePeriod());
        UNIT_ASSERT_VALUES_EQUAL(7u, config.GetSyncRequestsBatchSize());
        UNIT_ASSERT_VALUES_EQUAL(8192u, config.GetStripeSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(99),
            config.GetWriteHedgingDelay());
        UNIT_ASSERT_VALUES_EQUAL(33554432, config.GetVChunkSize());
    }

    Y_UNIT_TEST(ShouldApplyDefaultsForPartialProto)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetStripeSize(2048);

        TStorageConfig config{std::move(proto)};

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(1000),
            config.GetTraceSamplePeriod());
        UNIT_ASSERT_VALUES_EQUAL(3u, config.GetSyncRequestsBatchSize());
        UNIT_ASSERT_VALUES_EQUAL(2048u, config.GetStripeSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(700),
            config.GetWriteHedgingDelay());
        UNIT_ASSERT_VALUES_EQUAL(134217728, config.GetVChunkSize());
    }
}

}   // namespace NYdb::NBS::NBlockStore
