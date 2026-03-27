#pragma once

#include <library/cpp/threading/future/future.h>

namespace NUnifiedAgent {
    class TAsyncJoiner {
    public:
        inline TAsyncJoiner()
            : Promise(NThreading::NewPromise())
            , Refs(1)
        {
        }

        inline i64 Ref(i64 count = 1) noexcept {
            const auto result = Refs.fetch_add(count);
            Y_ABORT_UNLESS(result >= 1, "already joined");
            return result;
        }

        // UNIFIEDAGENT-1489: Safe variant of Ref() that does not abort. Returns false if already
        // joined (Refs < 1), e.g. when another thread has called Join() before this ran. Callers
        // (e.g. TLocalTimersQueue::CommitTimer) use this to bail out gracefully instead of crashing.
        inline bool TryRef(i64 count = 1) noexcept {
            i64 current = Refs.load();
            do {
                if (current < 1) {
                    return false;
                }
            } while (!Refs.compare_exchange_weak(current, current + count));
            return true;
        }

        inline i64 UnRef() noexcept {
            const auto prev = Refs.fetch_sub(1);
            Y_ABORT_UNLESS(prev >= 1);
            if (prev == 1) {
                auto p = Promise;
                p.SetValue();
            }
            return prev;
        }

        inline NThreading::TFuture<void> Join() noexcept {
            auto result = Promise;
            UnRef();
            return result;
        }

    private:
        NThreading::TPromise<void> Promise;
        std::atomic<i64> Refs;
    };

    using TAsyncJoinerToken = TIntrusivePtr<TAsyncJoiner>;
}
