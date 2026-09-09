#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/span.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <utility>

namespace NYdb::inline Dev::NRetry::Async {

template <typename TClient, typename TAsyncStatusType>
class TRetryContext : public TThrRefBase, public TRetryContextBase {
    enum class EState {
        Running,
        Finished,
        Cancelled,
    };

public:
    using TStatusType = typename TAsyncStatusType::value_type;
    using TPtr = TIntrusivePtr<Async::TRetryContext<TClient, TAsyncStatusType>>;

protected:
    TClient Client_;
    NThreading::TPromise<TStatusType> Promise_;

public:
    TAsyncStatusType Execute() {
        ParentSpan_ = Client_.Impl_->CreateRetryRootSpan();

        this->RetryStartTime_ = TInstant::Now();
        TPtr self(this);
        auto future = Promise_.GetFuture();
        // Retain cancellation registration even if an attempt drops its future.
        future.Subscribe([self](const auto&) {});
        DoRetry(self);
        return future;
    }

    ~TRetryContext() {
        if (IsCancelled()) {
            EndAttemptSpan(EStatus::CLIENT_CANCELLED);
            EndRetrySpan(EStatus::CLIENT_CANCELLED);
        }
    }

protected:
    explicit TRetryContext(const TClient& client, const TRetryOperationSettings& settings)
        : TRetryContextBase(settings)
        , Client_(client)
        , Promise_(NThreading::NewPromise<TStatusType>())
    {
        if (settings.CancellationToken_.stop_possible()) {
            StopCallback_.emplace(settings.CancellationToken_, [this, client = Client_.Impl_, promise = Promise_]() noexcept {
                if (!TryFinish(EState::Cancelled)) {
                    return;
                }
                client->ScheduleTask([promise]() mutable {
                    try {
                        promise.TrySetValue(MakeRetryCancelledResult<TStatusType>());
                    } catch (...) {
                        promise.TrySetException(std::current_exception());
                    }
                }, TDeadline::Duration::zero());
            });
        }
    }

    bool IsFinished() const noexcept {
        return State_.load() != EState::Running;
    }

    bool IsCancelled() const noexcept {
        return State_.load() == EState::Cancelled;
    }

    bool TryFinish(EState state = EState::Finished) noexcept {
        auto expected = EState::Running;
        return State_.compare_exchange_strong(expected, state);
    }

    virtual void Retry() = 0;

    virtual TAsyncStatusType RunOperation() = 0;

    static void DoRetry(TPtr self) {
        if (self->IsFinished()) {
            return;
        }
        try {
            [[maybe_unused]] auto parentScope = self->ParentSpan_ ? self->ParentSpan_->Activate() : nullptr;

            self->StartAttemptSpan();

            [[maybe_unused]] auto attemptScope = self->AttemptSpan_ ? self->AttemptSpan_->Activate() : nullptr;
            self->Retry();
        } catch (...) {
            HandleExceptionAsync(self, std::current_exception());
        }
    }

    static void DoBackoff(TPtr self, bool fast) {
        auto backoffSettings = fast ? self->Settings_.FastBackoffSettings_
                                    : self->Settings_.SlowBackoffSettings_;
        AsyncBackoff(self->Client_.Impl_, backoffSettings, self->RetryNumber_,
            [self](std::chrono::microseconds backoff) {
                self->LastBackoffMs_ =
                    std::chrono::duration_cast<std::chrono::milliseconds>(backoff).count();
                DoRetry(self);
            });
    }

    static void HandleExceptionAsync(TPtr self, std::exception_ptr e) {
        self->Finish([e]() -> TStatusType { std::rethrow_exception(e); });
    }

    static void HandleStatusAsync(TPtr self, const TStatusType& status) {
        if (self->IsFinished()) {
            return;
        }
        const TStatus& retryStatus = GetRetryStatus(status);
        self->EndAttemptSpan(retryStatus.GetStatus());
        auto nextStep = self->GetNextStep(retryStatus);
        if (nextStep != NextStep::Finish) {
            self->RetryNumber_++;
            self->Client_.Impl_->CollectRetryStatAsync(retryStatus.GetStatus());
            self->LogRetry(retryStatus);
        }
        switch (nextStep) {
            case NextStep::RetryImmediately:
                self->LastBackoffMs_ = 0;
                return DoRetry(self);
            case NextStep::RetryFastBackoff:
                return DoBackoff(self, true);
            case NextStep::RetrySlowBackoff:
                return DoBackoff(self, false);
            case NextStep::Finish:
                return self->Finish([&] { return status; });
        }
    }

    static void DoRunOperation(TPtr self) {
        if (self->IsFinished()) {
            return;
        }
        try {
            self->RunOperation().Subscribe(
                [self](const TAsyncStatusType& result) {
                    [[maybe_unused]] auto attemptScope = self->ActivateAttemptSpan();
                    try {
                        HandleStatusAsync(self, result.GetValue());
                    } catch (const NStatusHelpers::TYdbRangeErrorException& e) {
                        HandleStatusAsync(self, MakeRetryResultFromStatus<TStatusType>(TStatus(e.GetStatus())));
                    } catch (...) {
                        HandleExceptionAsync(self, std::current_exception());
                    }
                }
            );
        } catch (const NStatusHelpers::TYdbRangeErrorException& e) {
            HandleStatusAsync(self, MakeRetryResultFromStatus<TStatusType>(TStatus(e.GetStatus())));
        } catch (...) {
            HandleExceptionAsync(self, std::current_exception());
        }
    }

protected:
    std::unique_ptr<NTrace::IScope> ActivateAttemptSpan() {
        return AttemptSpan_ ? AttemptSpan_->Activate() : nullptr;
    }

private:
    template <typename F>
    void Finish(F&& getResult) {
        if (!TryFinish()) {
            return;
        }
        try {
            auto result = std::forward<F>(getResult)();
            EndAttemptSpan(GetRetryStatusCode(result));
            EndRetrySpan(GetRetryStatusCode(result));
            Promise_.TrySetValue(std::move(result));
        } catch (...) {
            EndAttemptSpan(EStatus::CLIENT_INTERNAL_ERROR);
            EndRetrySpan(std::current_exception());
            Promise_.TrySetException(std::current_exception());
        }
    }

    void StartAttemptSpan() {
        AttemptSpan_ = Client_.Impl_->CreateRetryAttemptSpan(
            this->RetryNumber_, LastBackoffMs_, ParentSpan_);
    }

    void EndAttemptSpan(EStatus status) {
        if (AttemptSpan_) {
            AttemptSpan_->End(status);
            AttemptSpan_.reset();
        }
    }

    std::shared_ptr<NObservability::TRequestSpan> AttemptSpan_;
    std::int64_t LastBackoffMs_ = 0;
    std::atomic<EState> State_ = EState::Running;
    std::optional<std::stop_callback<std::function<void()>>> StopCallback_;
};

template <typename TClient, typename TOperation, typename TAsyncStatusType = TFunctionResult<TOperation>>
class TRetryWithoutSession : public TRetryContext<TClient, TAsyncStatusType> {
    using TRetryContext = TRetryContext<TClient, TAsyncStatusType>;
    using TPtr = typename TRetryContext::TPtr;

private:
    TOperation Operation_;

public:
    explicit TRetryWithoutSession(
        const TClient& client, TOperation&& operation, const TRetryOperationSettings& settings)
        : TRetryContext(client, settings)
        , Operation_(std::move(operation))
    {}

    void Retry() override {
        TPtr self(this);
        TRetryContext::DoRunOperation(self);
    }

protected:
    TAsyncStatusType RunOperation() override {
        return this->InvokeOperation(this->Client_, Operation_, this->Client_);
    }
};

template <typename TClient, typename TOperation, typename TAsyncStatusType = TFunctionResult<TOperation>>
class TRetryWithSession : public TRetryContext<TClient, TAsyncStatusType>, public TRetryDeadlineHelper<TClient> {
    using TRetryContextAsync = TRetryContext<TClient, TAsyncStatusType>;
    using TStatusType = typename TRetryContextAsync::TStatusType;
    using TSession = typename TClient::TSession;
    using TCreateSessionSettings = typename TClient::TCreateSessionSettings;
    using TAsyncCreateSessionResult = typename TClient::TAsyncCreateSessionResult;

private:
    const TOperation Operation_;
    const TDeadline Deadline_;
    std::optional<TSession> Session_;

public:
    explicit TRetryWithSession(
        const TClient& client, TOperation&& operation, const TRetryOperationSettings& settings)
        : TRetryContextAsync(client, settings)
        , Operation_(std::move(operation))
        , Deadline_(TDeadline::AfterDuration(this->Settings_.MaxTimeout_))
    {}

    void Retry() override {
        TIntrusivePtr<TRetryWithSession> self(this);
        if (self->IsFinished()) {
            return;
        }
        if (!Session_) {
            auto settings = TCreateSessionSettings()
                .ClientTimeout(this->Settings_.GetSessionClientTimeout_)
                .Deadline(Deadline_);

            this->Client_.GetSession(settings).Subscribe(
                [self](const TAsyncCreateSessionResult& resultFuture) {
                    [[maybe_unused]] auto attemptScope = self->ActivateAttemptSpan();
                    if (self->IsFinished()) {
                        return;
                    }
                    try {
                        auto& result = resultFuture.GetValue();
                        if (!result.IsSuccess()) {
                            return TRetryContextAsync::HandleStatusAsync(
                                self, MakeRetryResultFromStatus<TStatusType>(TStatus(result)));
                        }

                        self->Session_ = result.GetSession();
                        TRetryDeadlineHelper<TClient>::SetDeadline(*self->Session_, self->Deadline_);
                        self->DoRunOperation(self);
                    } catch (...) {
                        return TRetryContextAsync::HandleExceptionAsync(self, std::current_exception());
                    }
                }
            );
        } else {
            TRetryContextAsync::DoRunOperation(self);
        }
    }

private:
    void Reset() override {
        Session_.reset();
    }

    TAsyncStatusType RunOperation() override {
        return this->InvokeOperation(this->Client_, Operation_, this->Session_.value());
    }
};

} // namespace NYdb::NRetry::Async
