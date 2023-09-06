#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <util/generic/maybe.h>

namespace NYdb::NRetry {

template <typename TClient>
class TRetryContextSync : public TRetryContextBase<TClient> {
public:
    TRetryContextSync(TClient& client, const TRetryOperationSettings& settings)
        : TRetryContextBase<TClient>(client, settings)
    {}

    TStatus Execute() {
        TStatus status = RunOperation(); // first attempt
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
            status = RunOperation();
        }
        return status;
    }

protected:
    void DoBackoff(bool fast) {
        const auto &settings = fast ? this->Settings.FastBackoffSettings_
                                    : this->Settings.SlowBackoffSettings_;
        Backoff(settings, this->RetryNumber);
    }

    virtual TStatus RunOperation() = 0;
};

template<typename TClient, typename TOperation>
class TRetryWithoutSessionSync : public TRetryContextSync<TClient> {
private:
    const TOperation& Operation;

public:
    TRetryWithoutSessionSync(TClient& client, const TOperation& operation, const TRetryOperationSettings& settings)
        : TRetryContextSync<TClient>(client, settings)
        , Operation(operation)
    {}

protected:
    TStatus RunOperation() override {
        return Operation(this->Client);
    }
};

template<typename TClient, typename TOperation>
class TRetryWithSessionSync : public TRetryContextSync<TClient> {
    using TSession = typename TClient::TSession;
    using TCreateSessionSettings = typename TClient::TCreateSessionSettings;

private:
    const TOperation& Operation;
    TMaybe<TSession> Session;

public:
    TRetryWithSessionSync(TClient& client, const TOperation& operation, const TRetryOperationSettings& settings)
        : TRetryContextSync<TClient>(client, settings)
        , Operation(operation)
    {}

protected:
    TStatus RunOperation() override {
        TMaybe<NYdb::TStatus> status;

        if (!Session) {
            auto settings = TCreateSessionSettings().ClientTimeout(this->Settings.GetSessionClientTimeout_);
            auto sessionResult = this->Client.GetSession(settings).GetValueSync();
            if (sessionResult.IsSuccess()) {
                Session = sessionResult.GetSession();
            }
            status = sessionResult;
        }

        if (Session) {
            status = Operation(Session.GetRef());
        }

        return *status;
    }

    void Reset() override {
        Session.Clear();
    }
};

} // namespace NYdb::NRetry
