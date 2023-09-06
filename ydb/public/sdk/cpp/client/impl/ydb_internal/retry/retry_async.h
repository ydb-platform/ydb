#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry.h>

namespace NYdb::NRetry {

template <typename TClient, typename TStatusType>
class TRetryContextAsync : public TThrRefBase, public TRetryContextBase<TClient> {
public:
    using TPtr = TIntrusivePtr<NYdb::NRetry::TRetryContextAsync<TClient, TStatusType>>;
    using TAsyncStatusType = typename NThreading::TFuture<TStatusType>;

protected:
    NThreading::TPromise<TStatusType> Promise;

public:
    TAsyncStatusType GetFuture() {
        return this->Promise.GetFuture();
    }

    virtual void Execute() = 0;

protected:
    explicit TRetryContextAsync(const TClient& client, const TRetryOperationSettings& settings)
        : TRetryContextBase<TClient>(client, settings)
        , Promise(NThreading::NewPromise<TStatusType>())
    {}

    static void DoExecute(TPtr self) {
        self->Execute();
    }

    static void DoBackoff(TPtr self, bool fast) {
        auto backoffSettings = fast ? self->Settings.FastBackoffSettings_
                                    : self->Settings.SlowBackoffSettings_;
        AsyncBackoff(self->Client.Impl_, backoffSettings, self->RetryNumber,
            [self]() {DoExecute(self);});
    }

    static void HandleExceptionAsync(TPtr self, std::exception_ptr e) {
        self->Promise.SetException(e);
    }

    static void HandleStatusAsync(TPtr self, const TStatusType& status) {
        auto nextStep = self->GetNextStep(status);
        if (nextStep != NextStep::Finish) {
            self->RetryNumber++;
            self->Client.Impl_->CollectRetryStatAsync(status.GetStatus());
            self->LogRetry(status);
        }
        switch (nextStep) {
            case NextStep::RetryImmediately:
                return DoExecute(self);
            case NextStep::RetryFastBackoff:
                return DoBackoff(self, true);
            case NextStep::RetrySlowBackoff:
                return DoBackoff(self, false);
            case NextStep::Finish:
                return self->Promise.SetValue(status);
        }
    }

    static void DoRunOperation(TPtr self) {
        self->RunOperation().Subscribe(
            [self](const TAsyncStatusType& result) {
                try {
                    HandleStatusAsync(self, result.GetValue());
                } catch (...) {
                    HandleExceptionAsync(self, std::current_exception());
                }
            }
        );
    }

    virtual TAsyncStatusType RunOperation() = 0;
};

template <typename TClient, typename TOperation, typename TStatusType>
class TRetryWithoutSessionAsync : public TRetryContextAsync<TClient, TStatusType> {
    using TRetryContextAsync = TRetryContextAsync<TClient, TStatusType>;
    using TPtr = typename TRetryContextAsync::TPtr;
    using TAsyncStatusType = typename TRetryContextAsync::TAsyncStatusType;

private:
    TOperation Operation;

public:
    explicit TRetryWithoutSessionAsync(const TClient& client, TOperation&& operation,
                                       const TRetryOperationSettings& settings)
        : TRetryContextAsync(client, settings)
        , Operation(operation)
    {}

    void Execute() override {
        TPtr self(this);
        TRetryContextAsync::DoRunOperation(self);
    }

private:
    TAsyncStatusType RunOperation() override {
        return Operation(this->Client);
    }
};

template <typename TClient, typename TOperation, typename TStatusType = typename std::result_of<TOperation>::type>
class TRetryWithSessionAsync : public TRetryContextAsync<TClient, TStatusType> {
    using TRetryContextAsync = TRetryContextAsync<TClient, TStatusType>;
    using TPtr = typename TRetryContextAsync::TPtr;
    using TAsyncStatusType = typename TRetryContextAsync::TAsyncStatusType;
    using TSession = typename TClient::TSession;
    using TCreateSessionSettings = typename TClient::TCreateSessionSettings;
    using TAsyncCreateSessionResult = typename TClient::TAsyncCreateSessionResult;

private:
    TOperation Operation;
    TMaybe<TSession> Session;

public:
    explicit TRetryWithSessionAsync(const TClient& client, TOperation&& operation,
                                    const TRetryOperationSettings& settings)
        : TRetryContextAsync(client, settings)
        , Operation(operation)
    {}

    void Execute() override {
        TPtr self(this);
        if (!Session) {
            auto settings = TCreateSessionSettings().ClientTimeout(this->Settings.GetSessionClientTimeout_);
            this->Client.GetSession(settings).Subscribe(
                [self](const TAsyncCreateSessionResult& resultFuture) {
                    try {
                        auto& result = resultFuture.GetValue();
                        if (!result.IsSuccess()) {
                            return TRetryContextAsync::HandleStatusAsync(self, TStatusType(TStatus(result)));
                        }

                        auto* myself = dynamic_cast<TRetryWithSessionAsync*>(self.Get());
                        myself->Session = result.GetSession();
                        myself->DoRunOperation(self);
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
        Session.Clear();
    }

    TAsyncStatusType RunOperation() override {
        return Operation(this->Session.GetRef());
    }
};

} // namespace NYdb::NRetry
