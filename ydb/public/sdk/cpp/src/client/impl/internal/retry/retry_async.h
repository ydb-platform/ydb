#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry.h>

namespace NYdb::inline Dev::NRetry::Async {

    template <typename TClient, typename TOperation, bool WithSession>
    class TRetryContext: public TThrRefBase,
                         public TRetryContextBase<TRetryContext<TClient, TOperation, WithSession>, typename TFunctionResult<TOperation>::value_type> {
        using TBase = TRetryContextBase<TRetryContext, typename TFunctionResult<TOperation>::value_type>;

    public:
        using TAsyncStatusType = TFunctionResult<TOperation>;
        using TStatusType = typename TAsyncStatusType::value_type;
        using TPtr = TIntrusivePtr<TRetryContext>;

        TRetryContext(const TClient& client, TOperation operation, const TRetryOperationSettings& settings)
            : TBase(settings)
            , Client_(client)
            , Operation_(std::move(operation))
        {
        }

        TAsyncStatusType Execute() {
            TPtr self(this);
            auto future = Promise_.GetFuture();
            this->Start();
            return future;
        }

        void ExecuteImpl() {
            Session_.Execute(*this, [this](auto& target) {
                Await(this->InvokeOperation(Operation_, target), [this](const auto& result) { OnAttemptReady(result); });
            });
        }

        void WaitAndExecute(std::chrono::microseconds delay, std::function<void()> fn) {
            Client_.Impl_->ScheduleTask([self = TPtr(this), fn = std::move(fn)] { fn(); },
                                        std::chrono::duration_cast<TDeadline::Duration>(delay));
        }

        template <typename T, typename F>
        void Await(NThreading::TFuture<T> future, F fn) {
            future.Subscribe([self = TPtr(this), fn = std::move(fn)](const auto& result) mutable {
                self->Guarded([&] { fn(result.GetValue()); });
            });
        }

        bool StopRequested() const {
            return this->Settings_.StopToken_ && this->Settings_.StopToken_->stop_requested();
        }

        auto GetRetryDelay(NextStep step) const {
            // Async backoff has always used a one-based retry index.
            return TBase::GetRetryDelay(step, this->RetryNumber_ + 1);
        }

        void OnAttemptReady(TStatusType result) {
            this->DoNext(std::move(result));
        }

        void OnReady(TStatusType result) {
            Promise_.SetValue(std::move(result));
        }

        void OnReady(std::exception_ptr error) {
            Promise_.SetException(error);
        }

        TClient& GetClient() {
            return Client_;
        }

        auto& GetClientImpl() {
            return Client_.Impl_;
        }

        void ResetSession() {
            Session_.Reset();
        }

        void CollectRetryStat(EStatus status) {
            Client_.Impl_->CollectRetryStatAsync(status);
        }

    private:
        TClient Client_;
        TOperation Operation_;
        TRetrySessionState<TClient, WithSession> Session_;
        NThreading::TPromise<TStatusType> Promise_ = NThreading::NewPromise<TStatusType>();
    };

    template <bool WithSession, typename TClient, typename TOperation>
    auto Retry(TClient& client, TOperation&& operation, const TRetryOperationSettings& settings) {
        using TContext = TRetryContext<TClient, std::decay_t<TOperation>, WithSession>;
        return MakeIntrusive<TContext>(client, std::forward<TOperation>(operation), settings)->Execute();
    }

} // namespace NYdb::inline Dev::NRetry::Async
