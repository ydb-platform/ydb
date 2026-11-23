#include "retry_test_helpers.h"

#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry_async.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry_settings.h>

using namespace NYdb;
using namespace NYdb::NRetry;
using namespace NYdb::NRetry::NTests;
using namespace NThreading;

Y_UNIT_TEST_SUITE(RetryAsync) {
    Y_UNIT_TEST(ReturnsFinalStatus) {
        for (auto status : {EStatus::SUCCESS, EStatus::BAD_REQUEST}) {
            TTestClient client;
            unsigned attempts = 0;
            auto result = Async::Retry<false>(client, [&](TTestClient&) {
                ++attempts;
                return MakeFuture(Status(status));
            }, Settings());

            UNIT_ASSERT(result.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStatus(), status);
            UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
            UNIT_ASSERT(!client.Impl_->Scheduled);
        }
    }

    Y_UNIT_TEST(RetriesTransientFailure) {
        TTestClient client;
        unsigned attempts = 0;
        auto result = Async::Retry<false>(client, [&](TTestClient&) {
            return MakeFuture(Status(++attempts == 1 ? EStatus::UNAVAILABLE : EStatus::SUCCESS));
        }, Settings());

        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
        UNIT_ASSERT(!result.HasValue());
        client.Impl_->RunScheduled();
        UNIT_ASSERT(result.HasValue());
        UNIT_ASSERT(result.GetValue().IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(attempts, 2);
        UNIT_ASSERT(!client.Impl_->Scheduled);
    }

    Y_UNIT_TEST(RespectsRetryLimit) {
        for (unsigned retries : {0u, 2u}) {
            TTestClient client;
            unsigned attempts = 0;
            auto result = Async::Retry<false>(client, [&](TTestClient&) {
                ++attempts;
                return MakeFuture(Status(EStatus::UNAVAILABLE));
            }, Settings().MaxRetries(retries));

            for (unsigned i = 0; i < retries; ++i) {
                UNIT_ASSERT(!result.HasValue());
                client.Impl_->RunScheduled();
            }
            UNIT_ASSERT(result.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStatus(), EStatus::UNAVAILABLE);
            UNIT_ASSERT_VALUES_EQUAL(attempts, retries + 1);
            UNIT_ASSERT(!client.Impl_->Scheduled);
        }
    }

    Y_UNIT_TEST(CancelledBeforeFirstAttempt) {
        TTestClient client;
        std::stop_source stop;
        stop.request_stop();
        unsigned attempts = 0;
        auto result = Async::Retry<false>(client, [&](TTestClient&) {
            ++attempts;
            return MakeFuture(Status(EStatus::SUCCESS));
        }, Settings().MaxRetries(0).StopToken(stop.get_token()));

        UNIT_ASSERT(result.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStatus(), EStatus::CLIENT_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(attempts, 0);
        UNIT_ASSERT(!client.Impl_->Scheduled);
    }

    Y_UNIT_TEST(CancellationWaitsForAttemptResult) {
        TTestClient client;
        std::stop_source stop;
        auto attempt = NewPromise<TStatus>();
        unsigned attempts = 0;
        auto result = Async::Retry<false>(client, [&](TTestClient&) {
            ++attempts;
            return attempt.GetFuture();
        }, Settings().StopToken(stop.get_token()));

        stop.request_stop();
        UNIT_ASSERT(!result.HasValue());
        attempt.SetValue(Status(EStatus::SUCCESS));
        UNIT_ASSERT(result.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStatus(), EStatus::CLIENT_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
        UNIT_ASSERT(!client.Impl_->Scheduled);
    }

    Y_UNIT_TEST(CancellationDuringBackoffStopsNextAttempt) {
        TTestClient client;
        std::stop_source stop;
        unsigned attempts = 0;
        auto settings = Settings().StopToken(stop.get_token()).FastBackoffSettings(TBackoffSettings().SlotDuration(TDuration::Seconds(1)).UncertainRatio(0));
        auto result = Async::Retry<false>(client, [&](TTestClient&) {
            ++attempts;
            return MakeFuture(Status(EStatus::UNAVAILABLE));
        }, settings);

        UNIT_ASSERT(client.Impl_->Delay > TDeadline::Duration::zero());
        stop.request_stop();
        UNIT_ASSERT(!result.HasValue());
        client.Impl_->RunScheduled();
        UNIT_ASSERT(result.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(result.GetValue().GetStatus(), EStatus::CLIENT_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
        UNIT_ASSERT(!client.Impl_->Scheduled);
    }

    Y_UNIT_TEST(SupportsMutableUnaryCallback) {
        TTestClient client;
        auto result = RunUnaryWithRetry(client, Settings(), [attempts = 0](TDuration) mutable {
            return MakeFuture(Status(++attempts == 1 ? EStatus::UNAVAILABLE : EStatus::SUCCESS));
        });

        UNIT_ASSERT(!result.HasValue());
        client.Impl_->RunScheduled();
        UNIT_ASSERT(result.HasValue());
        UNIT_ASSERT(result.GetValue().IsSuccess());
        UNIT_ASSERT(!client.Impl_->Scheduled);
    }
} // Y_UNIT_TEST_SUITE(RetryAsync)
