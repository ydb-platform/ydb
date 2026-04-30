#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/span.h>

#include <util/generic/function.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <memory>
#include <typeinfo>

namespace NYdb::inline Dev::NRetry::Async {

template <typename TClient, typename TAsyncStatusType>
class TRetryContext : public TThrRefBase, public TRetryContextBase {
public:
    using TStatusType = typename TAsyncStatusType::value_type;
    using TPtr = TIntrusivePtr<Async::TRetryContext<TClient, TAsyncStatusType>>;

protected:
    TClient Client_;
    NThreading::TPromise<TStatusType> Promise_;

public:
    TAsyncStatusType Execute() {
        ParentSpan_ = Client_.Impl_->CreateRetryRootSpan();

        [[maybe_unused]] auto parentScope = ParentSpan_ ? ParentSpan_->Activate() : nullptr;

        this->RetryStartTime_ = TInstant::Now();
        TPtr self(this);
        DoRetry(self);

        return this->Promise_.GetFuture().Apply(
            [self](const auto& f) mutable {
                try {
                    auto value = f.GetValue();
                    if (self->ParentSpan_) {
                        self->ParentSpan_->SetRetryCount(self->RetryNumber_);
                        self->ParentSpan_->End(value.GetStatus());
                    }
                    return value;
                } catch (...) {
                    if (self->ParentSpan_) {
                        self->ParentSpan_->SetRetryCount(self->RetryNumber_);
                        try {
                            std::rethrow_exception(std::current_exception());
                        } catch (const std::exception& e) {
                            self->ParentSpan_->RecordException(typeid(e).name(), e.what());
                        } catch (...) {
                            self->ParentSpan_->RecordException("unknown", "unknown exception");
                        }
                        self->ParentSpan_->End(EStatus::CLIENT_INTERNAL_ERROR);
                    }
                    throw;
                }
            }
        );
    }

protected:
    explicit TRetryContext(const TClient& client, const TRetryOperationSettings& settings)
        : TRetryContextBase(settings)
        , Client_(client)
        , Promise_(NThreading::NewPromise<TStatusType>())
    {}

    virtual void Retry() = 0;

    virtual TAsyncStatusType RunOperation() = 0;

    static void DoRetry(TPtr self) {
        self->StartAttemptSpan();

        [[maybe_unused]] auto scope = self->AttemptSpan_ ? self->AttemptSpan_->Activate() : nullptr;
        self->Retry();
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
        self->EndAttemptSpan(EStatus::CLIENT_INTERNAL_ERROR);
        self->Promise_.SetException(e);
    }

    static void HandleStatusAsync(TPtr self, const TStatusType& status) {
        self->EndAttemptSpan(status.GetStatus());
        auto nextStep = self->GetNextStep(status);
        if (nextStep != NextStep::Finish) {
            self->RetryNumber_++;
            self->Client_.Impl_->CollectRetryStatAsync(status.GetStatus());
            self->LogRetry(status);
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
                return self->Promise_.SetValue(status);
        }
    }

    static void DoRunOperation(TPtr self) {
        self->RunOperation().Subscribe(
            [self](const TAsyncStatusType& result) {
                [[maybe_unused]] auto attemptScope = self->ActivateAttemptSpan();
                try {
                    HandleStatusAsync(self, result.GetValue());
                } catch (...) {
                    HandleExceptionAsync(self, std::current_exception());
                }
            }
        );
    }

protected:
    std::unique_ptr<NTrace::IScope> ActivateAttemptSpan() {
        return AttemptSpan_ ? AttemptSpan_->Activate() : nullptr;
    }

private:
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

    std::shared_ptr<NObservability::TRequestSpan> ParentSpan_;
    std::shared_ptr<NObservability::TRequestSpan> AttemptSpan_;
    std::int64_t LastBackoffMs_ = 0;
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
        , Operation_(operation)
    {}

    void Retry() override {
        TPtr self(this);
        TRetryContext::DoRunOperation(self);
    }

protected:
    TAsyncStatusType RunOperation() override {
        if constexpr (TFunctionArgs<TOperation>::Length == 1) {
            return Operation_(this->Client_);
        } else {
            return Operation_(this->Client_, this->GetRemainingTimeout());
        }
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
        if (!Session_) {
            auto settings = TCreateSessionSettings()
                .ClientTimeout(this->Settings_.GetSessionClientTimeout_)
                .Deadline(Deadline_);

            this->Client_.GetSession(settings).Subscribe(
                [self](const TAsyncCreateSessionResult& resultFuture) {
                    [[maybe_unused]] auto attemptScope = self->ActivateAttemptSpan();
                    try {
                        auto& result = resultFuture.GetValue();
                        if (!result.IsSuccess()) {
                            return TRetryContextAsync::HandleStatusAsync(self, TStatusType(TStatus(result)));
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
        if constexpr (TFunctionArgs<TOperation>::Length == 1) {
            return Operation_(this->Session_.value());
        } else {
            return Operation_(this->Session_.value(), this->GetRemainingTimeout());
        }
    }
};

} // namespace NYdb::NRetry::Async
