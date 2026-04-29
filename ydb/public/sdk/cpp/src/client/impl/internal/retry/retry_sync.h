#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/trace/trace.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/retry.h>
#include <ydb/public/sdk/cpp/src/client/impl/observability/span.h>

#include <exception>
#include <memory>
#include <typeinfo>

namespace NYdb::inline Dev::NRetry::Sync {

template <typename TClient, typename TStatusType>
class TRetryContext : public TRetryContextBase {
protected:
    TClient& Client_;

public:
    TStatusType Execute() {
        ParentSpan_ = Client_.Impl_->CreateRetryRootSpan();

        [[maybe_unused]] auto parentScope = ParentSpan_ ? ParentSpan_->Activate() : nullptr;
        auto& parentSpan = ParentSpan_;

        try {
            auto status = ExecuteImpl();
            if (parentSpan) {
                parentSpan->SetRetryCount(this->RetryNumber_);
                parentSpan->End(status.GetStatus());
            }
            return status;
        } catch (...) {
            if (parentSpan) {
                parentSpan->SetRetryCount(this->RetryNumber_);
                try {
                    std::rethrow_exception(std::current_exception());
                } catch (const std::exception& e) {
                    parentSpan->RecordException(typeid(e).name(), e.what());
                } catch (...) {
                    parentSpan->RecordException("unknown", "unknown exception");
                }
                parentSpan->End(EStatus::CLIENT_INTERNAL_ERROR);
            }
            throw;
        }
    }

protected:
    TRetryContext(TClient& client, const TRetryOperationSettings& settings)
        : TRetryContextBase(settings)
        , Client_(client)
    {}

    virtual TStatusType Retry() = 0;

    virtual TStatusType RunOperation() = 0;

    std::chrono::microseconds DoBackoff(bool fast) {
        const auto &settings = fast ? this->Settings_.FastBackoffSettings_
                                    : this->Settings_.SlowBackoffSettings_;
        return Backoff(settings, this->RetryNumber_);
    }

private:
    TStatusType ExecuteImpl() {
        this->RetryStartTime_ = TInstant::Now();
        std::int64_t lastBackoffMs = 0;

        TStatusType status = RunAttempt(lastBackoffMs);
        for (this->RetryNumber_ = 0; this->RetryNumber_ <= this->Settings_.MaxRetries_;) {
            auto nextStep = this->GetNextStep(status);
            std::chrono::microseconds backoff{};
            switch (nextStep) {
                case NextStep::RetryImmediately:
                    break;
                case NextStep::RetryFastBackoff:
                    backoff = DoBackoff(true);
                    break;
                case NextStep::RetrySlowBackoff:
                    backoff = DoBackoff(false);
                    break;
                case NextStep::Finish:
                    return status;
            }
            this->RetryNumber_++;
            this->LogRetry(status);
            this->Client_.Impl_->CollectRetryStatSync(status.GetStatus());
            lastBackoffMs = std::chrono::duration_cast<std::chrono::milliseconds>(backoff).count();
            status = RunAttempt(lastBackoffMs);
        }
        return status;
    }

    TStatusType RunAttempt(std::int64_t backoffMs) {
        auto attemptSpan = Client_.Impl_->CreateRetryAttemptSpan(this->RetryNumber_, backoffMs, ParentSpan_);
        [[maybe_unused]] std::unique_ptr<NTrace::IScope> scope;
        if (attemptSpan) {
            scope = attemptSpan->Activate();
        }

        TStatusType status = Retry();

        if (attemptSpan) {
            attemptSpan->End(status.GetStatus());
        }
        return status;
    }

    std::shared_ptr<NObservability::TRequestSpan> ParentSpan_;
};

template<typename TClient, typename TOperation, typename TStatusType = TFunctionResult<TOperation>>
class TRetryWithoutSession : public TRetryContext<TClient, TStatusType> {
private:
    const TOperation& Operation_;

public:
    TRetryWithoutSession(TClient& client, const TOperation& operation, const TRetryOperationSettings& settings)
        : TRetryContext<TClient, TStatusType>(client, settings)
        , Operation_(operation)
    {}

protected:
    TStatusType Retry() override {
        return RunOperation();
    }

    TStatusType RunOperation() override {
        if constexpr (TFunctionArgs<TOperation>::Length == 1) {
            return Operation_(this->Client_);
        } else {
            return Operation_(this->Client_, this->GetRemainingTimeout());
        }
    }
};

template<typename TClient, typename TOperation, typename TStatusType = TFunctionResult<TOperation>>
class TRetryWithSession : public TRetryContext<TClient, TStatusType>, public TRetryDeadlineHelper<TClient> {
    using TSession = typename TClient::TSession;
    using TCreateSessionSettings = typename TClient::TCreateSessionSettings;

private:
    const TOperation& Operation_;
    const TDeadline Deadline_;
    std::optional<TSession> Session_;

public:
    TRetryWithSession(TClient& client, const TOperation& operation, const TRetryOperationSettings& settings)
        : TRetryContext<TClient, TStatusType>(client, settings)
        , Operation_(operation)
        , Deadline_(TDeadline::AfterDuration(this->Settings_.MaxTimeout_))
    {}

protected:
    TStatusType Retry() override {
        std::optional<TStatusType> status;

        if (!Session_) {
            auto settings = TCreateSessionSettings()
                .ClientTimeout(this->Settings_.GetSessionClientTimeout_)
                .Deadline(Deadline_);

            auto sessionResult = this->Client_.GetSession(settings).GetValueSync();
            if (sessionResult.IsSuccess()) {
                Session_ = sessionResult.GetSession();
                TRetryDeadlineHelper<TClient>::SetDeadline(*Session_, Deadline_);
            }
            status = TStatusType(TStatus(sessionResult));
        }

        if (Session_) {
            status = RunOperation();
        }

        return *status;
    }

    TStatusType RunOperation() override {
        if constexpr (TFunctionArgs<TOperation>::Length == 1) {
            return Operation_(this->Session_.value());
        } else {
            return Operation_(this->Session_.value(), this->GetRemainingTimeout());
        }
    }

    void Reset() override {
        Session_.reset();
    }
};

} // namespace NYdb::NRetry::Sync
