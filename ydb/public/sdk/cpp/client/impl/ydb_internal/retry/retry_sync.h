#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <util/generic/maybe.h>

namespace NYdb::NRetry::Sync {

template <typename TClient, typename TStatusType>
class TRetryContext : public TRetryContextBase {
protected:
    TClient& Client_;

public:
    TStatusType Execute() {
        this->RetryTimer_.Reset();
        TStatusType status = Retry(); // first attempt
        for (this->RetryNumber_ = 0; this->RetryNumber_ <= this->Settings_.MaxRetries_;) {
            auto nextStep = this->GetNextStep(status);
            switch (nextStep) {
                case NextStep::RetryImmediately:
                    break;
                case NextStep::RetryFastBackoff:
                    DoBackoff(true);
                    break;
                case NextStep::RetrySlowBackoff:
                    DoBackoff(false);
                    break;
                case NextStep::Finish:
                    return status;
            }
            // make next retry
            this->RetryNumber_++;
            this->LogRetry(status);
            this->Client_.Impl_->CollectRetryStatSync(status.GetStatus());
            status = Retry();
        }
        return status;
    }

protected:
    TRetryContext(TClient& client, const TRetryOperationSettings& settings)
        : TRetryContextBase(settings)
        , Client_(client)
    {}

    virtual TStatusType Retry() = 0;

    virtual TStatusType RunOperation() = 0;

    void DoBackoff(bool fast) {
        const auto &settings = fast ? this->Settings_.FastBackoffSettings_
                                    : this->Settings_.SlowBackoffSettings_;
        Backoff(settings, this->RetryNumber_);
    }
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
class TRetryWithSession : public TRetryContext<TClient, TStatusType> {
    using TSession = typename TClient::TSession;
    using TCreateSessionSettings = typename TClient::TCreateSessionSettings;

private:
    const TOperation& Operation_;
    TMaybe<TSession> Session_;

public:
    TRetryWithSession(TClient& client, const TOperation& operation, const TRetryOperationSettings& settings)
        : TRetryContext<TClient, TStatusType>(client, settings)
        , Operation_(operation)
    {}

protected:
    TStatusType Retry() override {
        TMaybe<TStatusType> status;

        if (!Session_) {
            auto settings = TCreateSessionSettings().ClientTimeout(this->Settings_.GetSessionClientTimeout_);
            auto sessionResult = this->Client_.GetSession(settings).GetValueSync();
            if (sessionResult.IsSuccess()) {
                Session_ = sessionResult.GetSession();
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
            return Operation_(this->Session_.GetRef());
        } else {
            return Operation_(this->Session_.GetRef(), this->GetRemainingTimeout());
        }
    }

    void Reset() override {
        Session_.Clear();
    }
};

} // namespace NYdb::NRetry::Sync
