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
        CHECK_FLAG_MATCHES(flags, proto, TrimEntireDeviceOnStartup);
        CHECK_FLAG_MATCHES(flags, proto, EnableFailureInjectionTermination);
        CHECK_FLAG_MATCHES(flags, proto, EnableTopicDeferredPublish);
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

    // EnableNbsDisksSsdIoV2 is read at process start. A runtime config update
    // must not turn it on and must not clear it.
    Y_UNIT_TEST(EnableNbsDisksSsdIoV2RequiresRestart) {
        TRuntimeFeatureFlags flags;

        NKikimrConfig::TFeatureFlags proto;
        proto.SetEnableNbsDisksSsdIoV2(true);
        proto.SetEnableDataShardVolatileTransactions(false);

        // Runtime update does not turn the restart-only flag on.
        flags.CopyRuntimeFrom(proto);
        UNIT_ASSERT_VALUES_EQUAL(flags.HasEnableNbsDisksSsdIoV2(), false);
        UNIT_ASSERT_VALUES_EQUAL(flags.GetEnableNbsDisksSsdIoV2(), false);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrConfig::TFeatureFlags(flags).DebugString(),
            "EnableDataShardVolatileTransactions: false\n");

        flags.SetEnableNbsDisksSsdIoV2(true);
        flags.SetEnableDataShardVolatileTransactions(true);
        proto.SetEnableNbsDisksSsdIoV2(false);

        // Runtime update does not clear the restart-only flag.
        // EnableDataShardVolatileTransactions is a runtime flag and is updated.
        flags.CopyRuntimeFrom(proto);
        UNIT_ASSERT_VALUES_EQUAL(flags.HasEnableNbsDisksSsdIoV2(), true);
        UNIT_ASSERT_VALUES_EQUAL(flags.GetEnableNbsDisksSsdIoV2(), true);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimrConfig::TFeatureFlags(flags).DebugString(),
            "EnableDataShardVolatileTransactions: false\n"
            "EnableNbsDisksSsdIoV2: true\n");
    }

} // Y_UNIT_TEST_SUITE(RuntimeFeatureFlags)

} // namespace NKikimr
