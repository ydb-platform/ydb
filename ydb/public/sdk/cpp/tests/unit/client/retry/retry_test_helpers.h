#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NRetry::NTests {

    struct TTestClient {
        struct TImpl {
            std::function<void()> Scheduled;
            TDeadline::Duration Delay{};

            void ScheduleTask(const std::function<void()>& fn, TDeadline::Duration delay) {
                UNIT_ASSERT(!Scheduled);
                Scheduled = fn;
                Delay = delay;
            }

            void RunScheduled() {
                UNIT_ASSERT(Scheduled);
                auto fn = std::exchange(Scheduled, {});
                fn();
            }

            void CollectRetryStatSync(EStatus) {
            }

            void CollectRetryStatAsync(EStatus) {
            }

            std::shared_ptr<NObservability::TRequestSpan> CreateRetryRootSpan() {
                return nullptr;
            }

            std::shared_ptr<NObservability::TRequestSpan> CreateRetryAttemptSpan(
                std::uint32_t, std::int64_t, const std::shared_ptr<NObservability::TRequestSpan>&)
            {
                return nullptr;
            }
        };

        std::shared_ptr<TImpl> Impl_ = std::make_shared<TImpl>();
        bool InRetry = false;

        bool GetInRetryOperationContext() const {
            return InRetry;
        }

        void SetInRetryOperationContext(bool value) {
            InRetry = value;
        }
    };

    inline TStatus Status(EStatus code) {
        return TStatus(code, NIssue::TIssues{});
    }

    inline TRetryOperationSettings Settings() {
        return TRetryOperationSettings()
            .MaxRetries(2)
            .FastBackoffSettings(TBackoffSettings().SlotDuration(TDuration::Zero()));
    }

} // namespace NYdb::inline Dev::NRetry::NTests
