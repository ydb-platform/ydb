#pragma once
#include "async.h"
#include "symmetric_proxy.h"

namespace NActors {

    namespace NDetail {
        class TAsyncCancellationState;
        class TAsyncCancellationAwaiter;
        template<class T>
        class TAsyncCancellationAwaiterWithCallback;
    }

    class TAsyncCancellationSource {
        friend NDetail::TAsyncCancellationAwaiter;

    public:
        TAsyncCancellationSource();
        TAsyncCancellationSource(TAsyncCancellationSource&& rhs) noexcept;
        TAsyncCancellationSource& operator=(TAsyncCancellationSource&& rhs) noexcept;
        ~TAsyncCancellationSource();

        explicit operator bool() const noexcept {
            return bool(State);
        }

        void Cancel();
        void operator()();

        bool IsCancelled() const;

    private:
        std::unique_ptr<NDetail::TAsyncCancellationState> State;
    };

    template<IsAsyncCoroutineCallback TCallback>
    inline auto ActorWithCancellation(TAsyncCancellationSource& source, TCallback&& callback) {
        using TCallbackResult = decltype(std::forward<TCallback>(callback)());
        using T = TAsyncCoroutineResult<TCallbackResult>;
        return NDetail::TAsyncCancellationAwaiterWithCallback<T>(source, std::forward<TCallback>(callback));
    }

} // namespace NActors
