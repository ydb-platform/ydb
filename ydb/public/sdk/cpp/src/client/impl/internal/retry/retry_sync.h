#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry.h>

#include <thread>

namespace NYdb::inline Dev::NRetry::Sync {

    template <typename TClient, typename TOperation, bool WithSession>
    class TRetryContext: public TRetryContextBase<TRetryContext<TClient, TOperation, WithSession>, TFunctionResult<TOperation>> {
        using TBase = TRetryContextBase<TRetryContext, TFunctionResult<TOperation>>;

    public:
        using TStatusType = TFunctionResult<TOperation>;

        TRetryContext(TClient& client, const TOperation& operation, const TRetryOperationSettings& settings)
            : TBase(settings)
            , Client_(client)
            , Operation_(operation)
        {
        }

        TStatusType Execute() {
            this->Start();
            while (!this->IsFinished()) {
                this->Guarded([&] { this->DoNext(std::move(*Result_)); });
            }
            if (Exception_) {
                std::rethrow_exception(Exception_);
            }
            return std::move(*Result_);
        }

        void ExecuteImpl() {
            Session_.Execute(*this, [this](auto& target) {
                OnAttemptReady(this->InvokeOperation(Operation_, target));
            });
        }

        void WaitAndExecute(std::chrono::microseconds delay, std::function<void()> fn) {
            std::this_thread::sleep_for(delay);
            fn();
        }

        template <typename T, typename F>
        void Await(NThreading::TFuture<T> future, F fn) {
            fn(future.GetValueSync());
        }

        bool StopRequested() const {
            return this->Settings_.StopToken_ && this->Settings_.StopToken_->stop_requested();
        }

        auto GetRetryDelay(NextStep step) const {
            return TBase::GetRetryDelay(step, this->RetryNumber_);
        }

        void OnAttemptReady(TStatusType result) {
            Result_.emplace(std::move(result));
        }

        void OnReady(TStatusType result) {
            OnAttemptReady(std::move(result));
        }

        void OnReady(std::exception_ptr error) {
            Exception_ = error;
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
            Client_.Impl_->CollectRetryStatSync(status);
        }

    private:
        TClient& Client_;
        const TOperation& Operation_;
        TRetrySessionState<TClient, WithSession> Session_;
        std::optional<TStatusType> Result_;
        std::exception_ptr Exception_;
    };

    template <bool WithSession, typename TClient, typename TOperation>
    auto Retry(TClient& client, const TOperation& operation, const TRetryOperationSettings& settings) {
        return TRetryContext<TClient, TOperation, WithSession>(client, operation, settings).Execute();
    }

} // namespace NYdb::inline Dev::NRetry::Sync
