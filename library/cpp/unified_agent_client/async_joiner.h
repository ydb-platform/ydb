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
