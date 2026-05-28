#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry_settings.h>
#undef INCLUDE_YDB_INTERNAL_H

using namespace NYdb;
using namespace NYdb::NRetry;

Y_UNIT_TEST_SUITE(YdbUnaryRetrySettings) {
    Y_UNIT_TEST(IsRetryEnabledRespectsMaxRetries) {
        UNIT_ASSERT(NRetry::IsRetryEnabled(TRetryOperationSettings()));
        UNIT_ASSERT(!NRetry::IsRetryEnabled(TRetryOperationSettings().MaxRetries(0)));
    }

    Y_UNIT_TEST(ResolveRetrySettingsUsesClientDefault) {
        const auto clientDefault = TRetryOperationSettings().MaxRetries(5);
        const auto resolved = ResolveRetrySettings(
            clientDefault,
            std::nullopt,
            TDuration::Seconds(10),
            ERetryIdempotentDefault::True);

        UNIT_ASSERT_VALUES_EQUAL(resolved.MaxRetries_, 5u);
        UNIT_ASSERT_VALUES_EQUAL(resolved.MaxTimeout_, TDuration::Seconds(10));
        UNIT_ASSERT(resolved.Idempotent_);
    }

    Y_UNIT_TEST(ResolveRetrySettingsUsesOperationOverride) {
        const auto clientDefault = TRetryOperationSettings().MaxRetries(10);
        const auto operationOverride = TRetryOperationSettings().MaxRetries(2);
        const auto resolved = ResolveRetrySettings(
            clientDefault,
            operationOverride,
            TDuration::Max(),
            ERetryIdempotentDefault::False);

        UNIT_ASSERT_VALUES_EQUAL(resolved.MaxRetries_, 2u);
        UNIT_ASSERT(!resolved.Idempotent_);
    }

    Y_UNIT_TEST(ResolveRetrySettingsExplicitOverrideHasHighestPriority) {
        const auto clientDefault = TRetryOperationSettings().MaxRetries(10);
        const auto operationOverride = TRetryOperationSettings().MaxRetries(2);
        const auto explicitOverride = TRetryOperationSettings().MaxRetries(7);
        const auto resolved = ResolveRetrySettings(
            clientDefault,
            operationOverride,
            explicitOverride,
            TDuration::Max(),
            ERetryIdempotentDefault::True);

        UNIT_ASSERT_VALUES_EQUAL(resolved.MaxRetries_, 7u);
    }

    Y_UNIT_TEST(ResolveRetrySettingsSkipsAutoIdempotentWhenOperationOverrideSet) {
        const auto clientDefault = TRetryOperationSettings().MaxRetries(10);
        const auto operationOverride = TRetryOperationSettings().MaxRetries(2);
        const auto resolved = ResolveRetrySettings(
            clientDefault,
            operationOverride,
            TDuration::Max(),
            ERetryIdempotentDefault::True);

        UNIT_ASSERT_VALUES_EQUAL(resolved.MaxRetries_, 2u);
        UNIT_ASSERT(!resolved.Idempotent_);
    }

    Y_UNIT_TEST(ResolveRetrySettingsSkipsAutoIdempotentWhenExplicitOverrideSet) {
        const auto clientDefault = TRetryOperationSettings().MaxRetries(10);
        const auto operationOverride = TRetryOperationSettings().MaxRetries(2);
        const auto explicitOverride = TRetryOperationSettings().MaxRetries(7);
        const auto resolved = ResolveRetrySettings(
            clientDefault,
            operationOverride,
            explicitOverride,
            TDuration::Max(),
            ERetryIdempotentDefault::True);

        UNIT_ASSERT_VALUES_EQUAL(resolved.MaxRetries_, 7u);
        UNIT_ASSERT(!resolved.Idempotent_);
    }
}
