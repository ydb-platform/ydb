#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/config.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NStorage {

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
        UNIT_ASSERT_VALUES_EQUAL(4096u, config.GetStripeSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(700),
            config.GetWriteHandoffDelay());
        UNIT_ASSERT_VALUES_EQUAL(TString{}, config.GetDDiskPoolName());
        UNIT_ASSERT_VALUES_EQUAL(
            TString{},
            config.GetPersistentBufferDDiskPoolName());
    }

    Y_UNIT_TEST(ShouldUseExplicitProtoValuesWhenSet)
    {
        NProto::TStorageServiceConfig proto;
        proto.SetTraceSamplePeriod(42);
        proto.SetSyncRequestsBatchSize(7);
        proto.SetStripeSize(8192);
        proto.SetWriteHandoffDelay(99);

        TStorageConfig config{std::move(proto)};

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(42),
            config.GetTraceSamplePeriod());
        UNIT_ASSERT_VALUES_EQUAL(7u, config.GetSyncRequestsBatchSize());
        UNIT_ASSERT_VALUES_EQUAL(8192u, config.GetStripeSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(99),
            config.GetWriteHandoffDelay());
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
            config.GetWriteHandoffDelay());
    }
}

}   // namespace NYdb::NBS::NStorage
