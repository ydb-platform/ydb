#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <util/generic/maybe.h>

namespace NYdb::NRetry::Sync {

template <typename TClient, typename TStatusType>
class TRetryContext : public TRetryContextBase {
protected:
    TClient Client;

public:
    TStatusType Execute() {
        TStatusType status = Retry(); // first attempt
        for (this->RetryNumber = 0; this->RetryNumber <= this->Settings.MaxRetries_;) {
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
            this->RetryNumber++;
            this->LogRetry(status);
            this->Client.Impl_->CollectRetryStatSync(status.GetStatus());
            status = Retry();
        }
        return status;
    }

protected:
    TRetryContext(TClient& client, const TRetryOperationSettings& settings)
        : TRetryContextBase(settings)
        , Client(client)
    {}

    virtual TStatusType Retry() = 0;

    virtual TStatusType RunOperation() = 0;

    void DoBackoff(bool fast) {
        const auto &settings = fast ? this->Settings.FastBackoffSettings_
                                    : this->Settings.SlowBackoffSettings_;
        Backoff(settings, this->RetryNumber);
    }
};

template<typename TClient, typename TOperation, typename TStatusType = TFunctionResult<TOperation>>
class TRetryWithoutSession : public TRetryContext<TClient, TStatusType> {
private:
    const TOperation& Operation;

public:
    TRetryWithoutSession(TClient& client, const TOperation& operation, const TRetryOperationSettings& settings)
        : TRetryContext<TClient, TStatusType>(client, settings)
        , Operation(operation)
    {}

protected:
    TStatusType Retry() override {
        return RunOperation();
    }

    TStatusType RunOperation() override {
        return Operation(this->Client);
    }
};

template<typename TClient, typename TOperation, typename TStatusType = TFunctionResult<TOperation>>
class TRetryWithSession : public TRetryContext<TClient, TStatusType> {
    using TSession = typename TClient::TSession;
    using TCreateSessionSettings = typename TClient::TCreateSessionSettings;

private:
    const TOperation& Operation;
    TMaybe<TSession> Session;

public:
    TRetryWithSession(TClient& client, const TOperation& operation, const TRetryOperationSettings& settings)
        : TRetryContext<TClient, TStatusType>(client, settings)
        , Operation(operation)
    {}

protected:
    TStatusType Retry() override {
        TMaybe<TStatusType> status;

        if (!Session) {
            auto settings = TCreateSessionSettings().ClientTimeout(this->Settings.GetSessionClientTimeout_);
            auto sessionResult = this->Client.GetSession(settings).GetValueSync();
            if (sessionResult.IsSuccess()) {
                Session = sessionResult.GetSession();
            }
            status = TStatusType(TStatus(sessionResult));
        }

        if (Session) {
            status = RunOperation();
        }

        return *status;
    }

    TStatusType RunOperation() override {
        return Operation(this->Session.GetRef());
    }

    void Reset() override {
        Session.Clear();
    }
};

} // namespace NYdb::NRetry::Sync
