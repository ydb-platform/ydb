#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry.h>

#include <util/generic/function.h>

namespace NYdb::NRetry::Async {

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
        this->RetryTimer_.Reset();
        this->Retry();
        return this->Promise_.GetFuture();
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
        self->Retry();
    }

    static void DoBackoff(TPtr self, bool fast) {
        auto backoffSettings = fast ? self->Settings_.FastBackoffSettings_
                                    : self->Settings_.SlowBackoffSettings_;
        AsyncBackoff(self->Client_.Impl_, backoffSettings, self->RetryNumber_,
            [self]() {DoRetry(self);});
    }

    static void HandleExceptionAsync(TPtr self, std::exception_ptr e) {
        self->Promise_.SetException(e);
    }

    static void HandleStatusAsync(TPtr self, const TStatusType& status) {
        auto nextStep = self->GetNextStep(status);
        if (nextStep != NextStep::Finish) {
            self->RetryNumber_++;
            self->Client_.Impl_->CollectRetryStatAsync(status.GetStatus());
            self->LogRetry(status);
        }
        switch (nextStep) {
            case NextStep::RetryImmediately:
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
                try {
                    HandleStatusAsync(self, result.GetValue());
                } catch (...) {
                    HandleExceptionAsync(self, std::current_exception());
                }
            }
        );
    }
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
class TRetryWithSession : public TRetryContext<TClient, TAsyncStatusType> {
    using TRetryContextAsync = TRetryContext<TClient, TAsyncStatusType>;
    using TPtr = typename TRetryContextAsync::TPtr;
    using TStatusType = typename TRetryContextAsync::TStatusType;
    using TSession = typename TClient::TSession;
    using TCreateSessionSettings = typename TClient::TCreateSessionSettings;
    using TAsyncCreateSessionResult = typename TClient::TAsyncCreateSessionResult;

private:
    TOperation Operation_;
    TMaybe<TSession> Session_;

public:
    explicit TRetryWithSession(
        const TClient& client, TOperation&& operation, const TRetryOperationSettings& settings)
        : TRetryContextAsync(client, settings)
        , Operation_(operation)
    {}

    void Retry() override {
        TPtr self(this);
        if (!Session_) {
            auto settings = TCreateSessionSettings().ClientTimeout(this->Settings_.GetSessionClientTimeout_);
            this->Client_.GetSession(settings).Subscribe(
                [self](const TAsyncCreateSessionResult& resultFuture) {
                    try {
                        auto& result = resultFuture.GetValue();
                        if (!result.IsSuccess()) {
                            return TRetryContextAsync::HandleStatusAsync(self, TStatusType(TStatus(result)));
                        }

                        auto* myself = dynamic_cast<TRetryWithSession*>(self.Get());
                        myself->Session_ = result.GetSession();
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
        Session_.Clear();
    }

    TAsyncStatusType RunOperation() override {
        if constexpr (TFunctionArgs<TOperation>::Length == 1) {
            return Operation_(this->Session_.GetRef());
        } else {
            return Operation_(this->Session_.GetRef(), this->GetRemainingTimeout());
        }
    }
};

} // namespace NYdb::NRetry::Async
