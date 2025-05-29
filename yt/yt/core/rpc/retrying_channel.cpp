#include "retrying_channel.h"
#include "private.h"
#include "channel_detail.h"
#include "client.h"
#include "config.h"
#include "dispatcher.h"

#include <yt/yt/core/bus/client.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/system/guard.h>

namespace NYT::NRpc {

using namespace NBus;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TRetryingChannel
    : public TChannelWrapper
{
public:
    TRetryingChannel(
        TRetryingChannelConfigPtr config,
        IChannelPtr underlyingChannel,
        TRetryChecker retryChecker)
        : TChannelWrapper(std::move(underlyingChannel))
        , Config_(std::move(config))
        , RetryChecker_(std::move(retryChecker))
    {
        YT_VERIFY(Config_);
    }

    IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        YT_ASSERT(request);
        YT_ASSERT(responseHandler);

        if (request->IsStreamingEnabled()) {
            return UnderlyingChannel_->Send(
                request,
                responseHandler,
                options);
        } else {
            return New<TRetryingRequest>(
                Config_,
                UnderlyingChannel_,
                std::move(request),
                std::move(responseHandler),
                options,
                RetryChecker_)
            ->Send();
        }
    }


private:
    const TRetryingChannelConfigPtr Config_;
    const TCallback<bool(const TError&)> RetryChecker_;


    class TRetryingRequest
        : public IClientResponseHandler
    {
    public:
        TRetryingRequest(
            TRetryingChannelConfigPtr config,
            IChannelPtr underlyingChannel,
            IClientRequestPtr request,
            IClientResponseHandlerPtr responseHandler,
            const TSendOptions& options,
            TCallback<bool(const TError&)> retryChecker)
            : Config_(std::move(config))
            , UnderlyingChannel_(std::move(underlyingChannel))
            , Request_(std::move(request))
            , ResponseHandler_(std::move(responseHandler))
            , Options_(options)
            , RetryChecker_(std::move(retryChecker))
            , BackoffStrategy_(Config_->RetryBackoff)
        {
            YT_ASSERT(Config_);
            YT_ASSERT(UnderlyingChannel_);
            YT_ASSERT(Request_);
            YT_ASSERT(ResponseHandler_);

            Deadline_ = Config_->RetryTimeout
                ? TInstant::Now() + *Config_->RetryTimeout
                : TInstant::Max();
        }

        IClientRequestControlPtr Send()
        {
            DoSend();
            return RequestControlThunk_;
        }

    private:
        class TRetryingRequestControlThunk
            : public IClientRequestControl
        {
        public:
            // NB: In contrast to TClientRequestControlThunk::SetUnderlying,
            // this one may be invoked multiple times.
            void SetNewUnderlying(IClientRequestControlPtr newUnderlying)
            {
                YT_ASSERT_THREAD_AFFINITY_ANY();

                if (!newUnderlying) {
                    return;
                }

                TCompactVector<IClientRequestControlPtr, 2> toCancelList;

                auto guard = Guard(SpinLock_);

                if (Underlying_) {
                    toCancelList.push_back(std::move(Underlying_));
                }

                if (Canceled_.load()) {
                    toCancelList.push_back(std::move(newUnderlying));
                } else {
                    Underlying_ = std::move(newUnderlying);
                }

                guard.Release();

                for (const auto& toCancel : toCancelList) {
                    toCancel->Cancel();
                }
            }

            void Cancel() override
            {
                YT_ASSERT_THREAD_AFFINITY_ANY();

                auto guard = Guard(SpinLock_);
                Canceled_.store(true);
                auto toCancel = std::move(Underlying_);
                guard.Release();

                if (toCancel) {
                    toCancel->Cancel();
                }
            }

            bool IsCanceled() const
            {
                YT_ASSERT_THREAD_AFFINITY_ANY();

                return Canceled_.load();
            }

            TFuture<void> SendStreamingPayload(const TStreamingPayload& /*payload*/) override
            {
                YT_ASSERT_THREAD_AFFINITY_ANY();

                return MakeFuture<void>(TError("Retrying channel does not support streaming"));
            }

            TFuture<void> SendStreamingFeedback(const TStreamingFeedback& /*feedback*/) override
            {
                YT_ASSERT_THREAD_AFFINITY_ANY();

                return MakeFuture<void>(TError("Retrying channel does not support streaming"));
            }

        private:
            YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
            std::atomic<bool> Canceled_ = false;
            IClientRequestControlPtr Underlying_;

        };

        using TRetryingRequestControlThunkPtr = TIntrusivePtr<TRetryingRequestControlThunk>;

        const TRetryingChannelConfigPtr Config_;
        const IChannelPtr UnderlyingChannel_;
        const IClientRequestPtr Request_;
        const IClientResponseHandlerPtr ResponseHandler_;
        const TSendOptions Options_;
        const TCallback<bool(const TError&)> RetryChecker_;
        const TRetryingRequestControlThunkPtr RequestControlThunk_ = New<TRetryingRequestControlThunk>();

        TBackoffStrategy BackoffStrategy_;
        //! The current attempt number (1-based).
        int CurrentAttempt_ = 1;
        TInstant Deadline_;

        std::optional<TError> FirstError_;
        std::optional<TError> LastError_;
        int OmittedInnerErrorCount_ = 0;

        // IClientResponseHandler implementation.

        void HandleAcknowledgement() override
        {
            YT_LOG_DEBUG("Request attempt acknowledged (RequestId: %v)",
                Request_->GetRequestId());

            // NB: The underlying handler is not notified.
        }

        void HandleError(TError error) override
        {
            YT_LOG_DEBUG(error, "Request attempt failed (RequestId: %v, Attempt: %v)",
                Request_->GetRequestId(),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (Config_->EnableExponentialRetryBackoffs) {
                        builder->AppendFormat("%v of %v",
                            BackoffStrategy_.GetInvocationIndex() + 1,
                            BackoffStrategy_.GetInvocationCount());
                    } else {
                        builder->AppendFormat("%v of %v",
                            CurrentAttempt_,
                            Config_->RetryAttempts);
                    }
                }));

            if (!RetryChecker_.Run(error)) {
                ReportError(error);
                return;
            }

            if (!FirstError_) {
                FirstError_ = std::move(error);
            } else {
                if (LastError_) {
                    ++OmittedInnerErrorCount_;
                }
                LastError_ = std::move(error);
            }

            Retry();
        }

        void HandleResponse(TSharedRefArray message, const std::string& address) override
        {
            YT_LOG_DEBUG("Request attempt succeeded (RequestId: %v)",
                Request_->GetRequestId());

            ResponseHandler_->HandleResponse(std::move(message), address);
        }

        void HandleStreamingPayload(const TStreamingPayload& /*payload*/) override
        {
            YT_UNIMPLEMENTED();
        }

        void HandleStreamingFeedback(const TStreamingFeedback& /*feedback*/) override
        {
            YT_UNIMPLEMENTED();
        }


        std::optional<TDuration> ComputeAttemptTimeout(TInstant now)
        {
            auto attemptDeadline = Options_.Timeout ? now + *Options_.Timeout : TInstant::Max();
            auto actualDeadline = std::min(Deadline_, attemptDeadline);
            return actualDeadline == TInstant::Max()
                ? std::optional<TDuration>(std::nullopt)
                : actualDeadline - now;
        }

        void ReportError(const TError& error)
        {
            auto detailedError = error
                << UnderlyingChannel_->GetEndpointAttributes()
                << TErrorAttribute("omitted_inner_error_count", OmittedInnerErrorCount_);
            if (FirstError_) {
                detailedError = detailedError << *FirstError_;
            }
            if (LastError_) {
                detailedError = detailedError << *LastError_;
            }
            ResponseHandler_->HandleError(std::move(detailedError));
        }

        void Retry()
        {
            auto retryAttemptsExhausted = false;
            auto backoffTime = TDuration::Zero();
            if (Config_->EnableExponentialRetryBackoffs) {
                retryAttemptsExhausted = !BackoffStrategy_.Next();
                backoffTime = BackoffStrategy_.GetBackoff();
            } else {
                auto count = ++CurrentAttempt_;
                retryAttemptsExhausted = count > Config_->RetryAttempts;
                backoffTime = Config_->RetryBackoffTime;
            }

            if (retryAttemptsExhausted || TInstant::Now() + backoffTime > Deadline_) {
                ReportError(TError(NRpc::EErrorCode::Unavailable, "Request retries failed"));
                return;
            }

            TDelayedExecutor::Submit(
                BIND(&TRetryingRequest::DoRetry, MakeStrong(this)),
                backoffTime,
                TDispatcher::Get()->GetHeavyInvoker());
        }

        void DoRetry(bool aborted)
        {
            if (aborted) {
                ReportError(TError(NYT::EErrorCode::Canceled, "Request timed out (timer was aborted)"));
                return;
            }

            if (RequestControlThunk_->IsCanceled()) {
                ResponseHandler_->HandleError(TError(NYT::EErrorCode::Canceled, "Request canceled"));
                return;
            }

            DoSend();
        }

        void DoSend()
        {
            YT_LOG_DEBUG("Request attempt started (RequestId: %v, Method: %v.%v, %v%vAttempt: %v, RequestTimeout: %v, RetryTimeout: %v)",
                Request_->GetRequestId(),
                Request_->GetService(),
                Request_->GetMethod(),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (!Request_->GetUser().empty()) {
                        builder->AppendFormat("User: %v, ", Request_->GetUser());
                    }
                }),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (!Request_->GetUserTag().empty() && Request_->GetUserTag() != Request_->GetUser()) {
                        builder->AppendFormat("UserTag: %v, ", Request_->GetUserTag());
                    }
                }),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (Config_->EnableExponentialRetryBackoffs) {
                        builder->AppendFormat("%v of %v",
                            BackoffStrategy_.GetInvocationIndex() + 1,
                            BackoffStrategy_.GetInvocationCount());
                    } else {
                        builder->AppendFormat("%v of %v",
                            CurrentAttempt_,
                            Config_->RetryAttempts);
                    }
                }),
                Options_.Timeout,
                Config_->RetryTimeout);

            auto now = TInstant::Now();
            if (now > Deadline_) {
                ReportError(TError(NYT::EErrorCode::Timeout, "Request retries timed out"));
                return;
            }

            auto adjustedOptions = Options_;
            adjustedOptions.Timeout = ComputeAttemptTimeout(now);
            auto requestControl = UnderlyingChannel_->Send(
                Request_,
                this,
                adjustedOptions);
            RequestControlThunk_->SetNewUnderlying(std::move(requestControl));
        }
    };
};

IChannelPtr CreateRetryingChannel(
    TRetryingChannelConfigPtr config,
    IChannelPtr underlyingChannel,
    TCallback<bool(const TError&)> retryChecker)
{
    static auto DefaultRetryChecker = BIND(&IsRetriableError);
    return New<TRetryingChannel>(
        std::move(config),
        std::move(underlyingChannel),
        retryChecker ? retryChecker : DefaultRetryChecker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
