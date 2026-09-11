#include "retry_test_helpers.h"

#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry_sync.h>

using namespace NYdb;
using namespace NYdb::NRetry;
using namespace NYdb::NRetry::NTests;

Y_UNIT_TEST_SUITE(RetrySync) {
    Y_UNIT_TEST(ReturnsFinalStatus) {
        for (auto status : {EStatus::SUCCESS, EStatus::BAD_REQUEST}) {
            TTestClient client;
            unsigned attempts = 0;
            auto result = Sync::Retry<false>(client, [&](TTestClient&) {
                ++attempts;
                return Status(status);
            }, Settings());

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), status);
            UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
            UNIT_ASSERT(!client.Impl_->Scheduled);
        }
    }

    Y_UNIT_TEST(RetriesTransientFailure) {
        TTestClient client;
        unsigned attempts = 0;
        auto result = Sync::Retry<false>(client, [&](TTestClient&) {
            return Status(++attempts == 1 ? EStatus::UNAVAILABLE : EStatus::SUCCESS);
        }, Settings());

        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(attempts, 2);
        UNIT_ASSERT(!client.Impl_->Scheduled);
    }

    Y_UNIT_TEST(RespectsRetryLimit) {
        for (unsigned retries : {0u, 2u}) {
            TTestClient client;
            unsigned attempts = 0;
            auto result = Sync::Retry<false>(client, [&](TTestClient&) {
                ++attempts;
                return Status(EStatus::UNAVAILABLE);
            }, Settings().MaxRetries(retries));

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAVAILABLE);
            UNIT_ASSERT_VALUES_EQUAL(attempts, retries + 1);
        }
    }

    Y_UNIT_TEST(CancelledBeforeFirstAttempt) {
        TTestClient client;
        std::stop_source stop;
        stop.request_stop();
        unsigned attempts = 0;
        auto result = Sync::Retry<false>(client, [&](TTestClient&) {
            ++attempts;
            return Status(EStatus::SUCCESS);
        }, Settings().MaxRetries(0).StopToken(stop.get_token()));

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CLIENT_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(attempts, 0);
    }

    Y_UNIT_TEST(CancellationStopsRetries) {
        TTestClient client;
        std::stop_source stop;
        unsigned attempts = 0;
        auto result = Sync::Retry<false>(client, [&](TTestClient&) {
            ++attempts;
            stop.request_stop();
            return Status(EStatus::UNAVAILABLE);
        }, Settings().StopToken(stop.get_token()));

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CLIENT_CANCELLED);
        UNIT_ASSERT_VALUES_EQUAL(attempts, 1);
    }
} // Y_UNIT_TEST_SUITE(RetrySync)
