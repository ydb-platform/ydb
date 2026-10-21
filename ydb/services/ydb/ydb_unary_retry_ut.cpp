#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/threading/future/future.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/common_client/impl/iface.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry_settings.h>
#undef INCLUDE_YDB_INTERNAL_H

using namespace NYdb;
using namespace NYdb::NRetry;

namespace {

class TMockUnaryRetryClientImpl : public IClientImplCommon {
public:
    void ScheduleTask(const std::function<void()>& fn, TDeadline::Duration) override {
        fn();
    }

    void CollectRetryStatAsync(EStatus) {}

    std::shared_ptr<NObservability::TRequestSpan> CreateRetryRootSpan() {
        return nullptr;
    }

    std::shared_ptr<NObservability::TRequestSpan> CreateRetryAttemptSpan(
        std::uint32_t,
        std::int64_t,
        const std::shared_ptr<NObservability::TRequestSpan>& = nullptr)
    {
        return nullptr;
    }
};

thread_local bool MockUnaryRetryClientInRetryOperationContext = false;

struct TMockUnaryRetryClient {
    std::shared_ptr<TMockUnaryRetryClientImpl> Impl_ = std::make_shared<TMockUnaryRetryClientImpl>();

    bool GetInRetryOperationContext() const {
        return MockUnaryRetryClientInRetryOperationContext;
    }

    void SetInRetryOperationContext(bool value) {
        MockUnaryRetryClientInRetryOperationContext = value;
    }
};

TRetryOperationSettings FastUnaryRetrySettings(ui32 maxRetries) {
    return TRetryOperationSettings()
        .MaxRetries(maxRetries)
        .Idempotent(true)
        .FastBackoffSettings(TBackoffSettings().SlotDuration(TDuration::MilliSeconds(1)).Ceiling(1))
        .SlowBackoffSettings(TBackoffSettings().SlotDuration(TDuration::MilliSeconds(1)).Ceiling(1));
}

} // namespace

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

    Y_UNIT_TEST(ResolveRetrySettingsRespectsClientIdempotentFalse) {
        const auto clientDefault = TRetryOperationSettings().MaxRetries(5).Idempotent(false);
        const auto resolved = ResolveRetrySettings(
            clientDefault,
            std::nullopt,
            TDuration::Seconds(10),
            ERetryIdempotentDefault::True);

        UNIT_ASSERT_VALUES_EQUAL(resolved.MaxRetries_, 5u);
        UNIT_ASSERT(!resolved.Idempotent_);
    }

    Y_UNIT_TEST(RunUnaryWithRetrySkipsInnerRetryInRetryOperationContext) {
        TMockUnaryRetryClient client;
        client.SetInRetryOperationContext(true);


        ui32 callCount = 0;
        const auto settings = FastUnaryRetrySettings(5);

        const auto result = RunUnaryWithRetry(client, settings, [&callCount](TDuration) {
            ++callCount;
            return NThreading::MakeFuture(TStatus(EStatus::UNAVAILABLE, NYdb::NIssue::TIssues()));
        }).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(callCount, 1u);
        client.SetInRetryOperationContext(false);
    }

    Y_UNIT_TEST(RunUnaryWithRetryPerformsInnerRetryOutsideRetryOperationContext) {
        TMockUnaryRetryClient client;

        ui32 callCount = 0;
        const ui32 failCount = 2;
        const auto settings = FastUnaryRetrySettings(5);

        const auto result = RunUnaryWithRetry(client, settings, [&callCount](TDuration) {
            ++callCount;
            if (callCount <= failCount) {
                return NThreading::MakeFuture(TStatus(EStatus::UNAVAILABLE, NYdb::NIssue::TIssues()));
            }
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues()));
        }).GetValueSync();

        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(callCount, failCount + 1);
    }
}
