#include <ydb/core/base/generated/runtime_feature_flags.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

#define CHECK_FLAG_MATCHES(a, b, name) do {\
    UNIT_ASSERT_VALUES_EQUAL(a.Has##name(), b.Has##name()); \
    UNIT_ASSERT_VALUES_EQUAL(a.Get##name(), b.Get##name()); \
} while (0)

Y_UNIT_TEST_SUITE(RuntimeFeatureFlags) {

    Y_UNIT_TEST(DefaultValues) {
        TRuntimeFeatureFlags flags;
        NKikimrConfig::TFeatureFlags proto;

        // Check some known defaults (both true and false)
        CHECK_FLAG_MATCHES(flags, proto, EnableSystemViews);
        CHECK_FLAG_MATCHES(flags, proto, TrimEntireDeviceOnStartup);
        CHECK_FLAG_MATCHES(flags, proto, EnableFailureInjectionTermination);
    }

    Y_UNIT_TEST(ConversionToProto) {
        TRuntimeFeatureFlags flags;

        NKikimrConfig::TFeatureFlags proto = flags;
        UNIT_ASSERT_VALUES_EQUAL(proto.DebugString(), "");

        UNIT_ASSERT_VALUES_EQUAL(flags.HasEnableDataShardVolatileTransactions(), false);
        UNIT_ASSERT_VALUES_EQUAL(flags.GetEnableDataShardVolatileTransactions(), true);
        flags.SetEnableDataShardVolatileTransactions(false);
        UNIT_ASSERT_VALUES_EQUAL(flags.HasEnableDataShardVolatileTransactions(), true);
        UNIT_ASSERT_VALUES_EQUAL(flags.GetEnableDataShardVolatileTransactions(), false);
        proto = flags;
        UNIT_ASSERT_VALUES_EQUAL(proto.DebugString(),
            "EnableDataShardVolatileTransactions: false\n");

        flags.SetEnableVolatileTransactionArbiters(true);
        proto = flags;
        UNIT_ASSERT_VALUES_EQUAL(proto.DebugString(),
            "EnableDataShardVolatileTransactions: false\n"
            "EnableVolatileTransactionArbiters: true\n");

        flags.ClearEnableDataShardVolatileTransactions();
        UNIT_ASSERT_VALUES_EQUAL(flags.HasEnableDataShardVolatileTransactions(), false);
        UNIT_ASSERT_VALUES_EQUAL(flags.GetEnableDataShardVolatileTransactions(), true);
        proto = flags;
        UNIT_ASSERT_VALUES_EQUAL(proto.DebugString(),
            "EnableVolatileTransactionArbiters: true\n");
    }

    Y_UNIT_TEST(ConversionFromProto) {
        TRuntimeFeatureFlags flags;

        {
            NKikimrConfig::TFeatureFlags proto;
            proto.SetEnableDataShardVolatileTransactions(false);
            flags.MergeFrom(proto);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrConfig::TFeatureFlags(flags).DebugString(),
            "EnableDataShardVolatileTransactions: false\n");

        {
            NKikimrConfig::TFeatureFlags proto;
            proto.SetEnableVolatileTransactionArbiters(false);
            flags.MergeFrom(proto);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrConfig::TFeatureFlags(flags).DebugString(),
            "EnableDataShardVolatileTransactions: false\n"
            "EnableVolatileTransactionArbiters: false\n");

        {
            NKikimrConfig::TFeatureFlags proto;
            proto.SetEnableGranularTimecast(false);
            flags.CopyFrom(proto);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrConfig::TFeatureFlags(flags).DebugString(),
            "EnableGranularTimecast: false\n");
        UNIT_ASSERT_VALUES_EQUAL(flags.GetEnableDataShardVolatileTransactions(), true);
        UNIT_ASSERT_VALUES_EQUAL(flags.GetEnableVolatileTransactionArbiters(), true);

        {
            NKikimrConfig::TFeatureFlags proto;
            proto.SetEnableBackupService(true);
            flags = proto;
        }

        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrConfig::TFeatureFlags(flags).DebugString(),
            "EnableBackupService: true\n");
    }

    Y_UNIT_TEST(UpdatingRuntimeFlags) {
        TRuntimeFeatureFlags flags;

        NKikimrConfig::TFeatureFlags proto;
        proto.SetEnableDbCounters(false);
        proto.SetEnableDataShardVolatileTransactions(false);

        // EnableDbCounters flag is not changed
        flags.CopyRuntimeFrom(proto);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrConfig::TFeatureFlags(flags).DebugString(),
            "EnableDataShardVolatileTransactions: false\n");

        flags.SetEnableDbCounters(true);
        flags.SetEnableDataShardVolatileTransactions(true);
        flags.SetEnableVolatileTransactionArbiters(true);

        // EnableDbCounters flag is not changed
        // EnableVolatileTransactionArbiters is cleared
        flags.CopyRuntimeFrom(proto);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrConfig::TFeatureFlags(flags).DebugString(),
            "EnableDbCounters: true\n"
            "EnableDataShardVolatileTransactions: false\n");
    }

} // Y_UNIT_TEST_SUITE(RuntimeFeatureFlags)

} // namespace NKikimr
