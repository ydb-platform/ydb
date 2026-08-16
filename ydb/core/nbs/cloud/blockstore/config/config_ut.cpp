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
        UNIT_ASSERT_VALUES_EQUAL(10u, config.GetSyncRequestsBatchSize());
        UNIT_ASSERT_VALUES_EQUAL(524288u, config.GetStripeSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(1000),
            config.GetWriteHedgingDelay());
        UNIT_ASSERT_VALUES_EQUAL("ddp1", config.GetDDiskPoolName());
        UNIT_ASSERT_VALUES_EQUAL(
            "ddp1",
            config.GetPersistentBufferDDiskPoolName());
        UNIT_ASSERT_VALUES_EQUAL(134217728, config.GetVChunkSize());
        UNIT_ASSERT_VALUES_EQUAL(4u, config.GetVhostThreadsCount());
        UNIT_ASSERT_VALUES_EQUAL(4u, config.GetVhostQueuesCount());
    }

    Y_UNIT_TEST(ShouldUseExplicitProtoValuesWhenSet)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetTraceSamplePeriod(42);
        proto.SetSyncRequestsBatchSize(7);
        proto.SetStripeSize(8192);
        proto.SetWriteHedgingDelay(99);
        proto.SetVChunkSize(33554432);
        proto.SetVhostThreadsCount(12);
        proto.SetVhostQueuesCount(16);

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
        UNIT_ASSERT_VALUES_EQUAL(12u, config.GetVhostThreadsCount());
        UNIT_ASSERT_VALUES_EQUAL(16u, config.GetVhostQueuesCount());
    }

    Y_UNIT_TEST(ShouldApplyDefaultsForPartialProto)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetStripeSize(2048);

        TStorageConfig config{std::move(proto)};

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(1000),
            config.GetTraceSamplePeriod());
        UNIT_ASSERT_VALUES_EQUAL(10u, config.GetSyncRequestsBatchSize());
        UNIT_ASSERT_VALUES_EQUAL(2048u, config.GetStripeSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(1000),
            config.GetWriteHedgingDelay());
        UNIT_ASSERT_VALUES_EQUAL(134217728, config.GetVChunkSize());
        UNIT_ASSERT_VALUES_EQUAL(4u, config.GetVhostThreadsCount());
        UNIT_ASSERT_VALUES_EQUAL(4u, config.GetVhostQueuesCount());
    }

    Y_UNIT_TEST(ShouldAcceptOnlyVhostThreadsCountAndKeepOtherDefaults)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetVhostThreadsCount(8);

        TStorageConfig config{std::move(proto)};

        UNIT_ASSERT_VALUES_EQUAL(8u, config.GetVhostThreadsCount());
        // VhostQueuesCount must fall back to its default when not set.
        UNIT_ASSERT_VALUES_EQUAL(4u, config.GetVhostQueuesCount());
    }

    Y_UNIT_TEST(ShouldAcceptOnlyVhostQueuesCountAndKeepOtherDefaults)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetVhostQueuesCount(2);

        TStorageConfig config{std::move(proto)};

        UNIT_ASSERT_VALUES_EQUAL(2u, config.GetVhostQueuesCount());
        // VhostThreadsCount must fall back to its default when not set.
        UNIT_ASSERT_VALUES_EQUAL(4u, config.GetVhostThreadsCount());
    }
}

}   // namespace NYdb::NBS::NBlockStore
