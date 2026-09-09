#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>

#include <library/cpp/threading/future/core/fwd.h>
#include <util/datetime/base.h>
#include <util/generic/function.h>
#include <util/generic/ptr.h>
#include <util/system/types.h>
#include <util/string/cast.h>

#include <exception>
#include <functional>
#include <memory>
#include <type_traits>

namespace NYdb::inline Dev {
class IClientImplCommon;
namespace NObservability {
class TRequestSpan;
}
}

namespace NYdb::inline Dev::NRetry {

std::chrono::microseconds Backoff(const NRetry::TBackoffSettings& settings, std::uint32_t retryNumber);
std::chrono::microseconds AsyncBackoff(std::shared_ptr<IClientImplCommon> client, const TBackoffSettings& settings,
    std::uint32_t retryNumber, std::function<void(std::chrono::microseconds)> fn);

enum class NextStep {
    RetryImmediately,
    RetryFastBackoff,
    RetrySlowBackoff,
    Finish,
};

struct TRetryDecision {
    NextStep Step;
    bool ResetSession = false;
};

inline TRetryDecision GetRetryDecision(EStatus status, const TRetryOperationSettings& settings) {
    switch (status) {
        case EStatus::SUCCESS:
        case EStatus::CLIENT_CANCELLED:
            return {NextStep::Finish};
        case EStatus::ABORTED:
            return {NextStep::RetryImmediately};
        case EStatus::OVERLOADED:
        case EStatus::CLIENT_RESOURCE_EXHAUSTED:
            return {NextStep::RetrySlowBackoff};
        case EStatus::UNAVAILABLE:
            return {NextStep::RetryFastBackoff};
        case EStatus::BAD_SESSION:
        case EStatus::SESSION_BUSY:
            return {NextStep::RetryImmediately, true};
        case EStatus::NOT_FOUND:
            return {settings.RetryNotFound_ ? NextStep::RetryImmediately : NextStep::Finish};
        case EStatus::UNDETERMINED:
            return {settings.Idempotent_ ? NextStep::RetryFastBackoff : NextStep::Finish};
        case EStatus::TRANSPORT_UNAVAILABLE:
            return {settings.Idempotent_ ? NextStep::RetryFastBackoff : NextStep::Finish, settings.Idempotent_};
        case EStatus::CLIENT_DEADLINE_EXCEEDED:
            return {settings.RetryUndefined_ ? NextStep::RetrySlowBackoff : NextStep::Finish, true};
        default:
            return {settings.RetryUndefined_ ? NextStep::RetrySlowBackoff : NextStep::Finish};
    }
}

inline bool ShouldRetryStatus(EStatus status, const TRetryOperationSettings& settings) {
    return GetRetryDecision(status, settings).Step != NextStep::Finish;
}

template <typename TClient>
class TInRetryOperationContextClientGuard;

class TRetryContextBase : TNonCopyable {
protected:
    TRetryOperationSettings Settings_;
    std::uint32_t RetryNumber_;
    TInstant RetryStartTime_;
    std::shared_ptr<NObservability::TRequestSpan> ParentSpan_;

protected:
    TRetryContextBase(const TRetryOperationSettings& settings)
        : Settings_(settings)
        , RetryNumber_(0)
        , RetryStartTime_(TInstant::Now())
    {}

    virtual void Reset() {}

    bool IsCancellationRequested() const noexcept {
        return Settings_.CancellationToken_.stop_requested();
    }

    void LogRetry(const TStatus& status) {
        if (Settings_.Verbose_) {
            std::cerr << "Previous query attempt was finished with unsuccessful status "
                << ToString(status.GetStatus()) << ": " << status.GetIssues().ToString(true) << std::endl;
            std::cerr << "Sending retry attempt " << RetryNumber_ << " of " << Settings_.MaxRetries_ << std::endl;
        }
    }

    NextStep GetNextStep(const TStatus& status) {
        const auto decision = GetRetryDecision(status.GetStatus(), Settings_);
        if ((decision.Step == NextStep::Finish && !decision.ResetSession)
            || RetryNumber_ >= Settings_.MaxRetries_
            || TInstant::Now() - RetryStartTime_ >= Settings_.MaxTimeout_)
        {
            return NextStep::Finish;
        }
        if (decision.ResetSession) {
            Reset();
        }
        return decision.Step;
    }

    TDuration GetRemainingTimeout() {
        return Settings_.MaxTimeout_ == TDuration::Max()
            ? TDuration::Max() : Settings_.MaxTimeout_ - (TInstant::Now() - RetryStartTime_);
    }

    void EndRetrySpan(EStatus status);
    void EndRetrySpan(std::exception_ptr exception);

    template <typename TClient, typename TOperation, typename TTarget>
    auto InvokeOperation(TClient& client, TOperation& operation, TTarget& target) {
        TInRetryOperationContextClientGuard<TClient> guard(client);
        if constexpr (TFunctionArgs<TOperation>::Length == 1) {
            return operation(target);
        } else {
            return operation(target, GetRemainingTimeout());
        }
    }
};

template <typename TStatusType>
TStatusType MakeRetryResultFromStatus(TStatus&& status) {
    return TStatusType(TStatus(std::move(status)));
}

template <typename TStatusType>
TStatusType MakeRetryCancelledResult() {
    return MakeRetryResultFromStatus<TStatusType>(
        TStatus(EStatus::CLIENT_CANCELLED, NIssue::TIssues{NIssue::TIssue("Retry operation was cancelled")}));
}

template <typename TStatusType, typename F>
TStatusType InvokeWithRangeErrorCatch(F&& f) {
    try {
        return f();
    } catch (const NStatusHelpers::TYdbRangeErrorException& e) {
        return MakeRetryResultFromStatus<TStatusType>(TStatus(e.GetStatus()));
    }
}

template<typename TClient>
class TRetryDeadlineHelper {
public:
    static void SetDeadline(TClient::TSession& session, const TDeadline& deadline) {
        session.SetPropagatedDeadline(deadline);
    }
};

template<typename TClient>
class TInRetryOperationContextClientGuard {
public:
    explicit TInRetryOperationContextClientGuard(TClient& client)
        : Client_(client)
        , Previous_(client.GetInRetryOperationContext())
    {
        Client_.SetInRetryOperationContext(true);
    }

    ~TInRetryOperationContextClientGuard() {
        Client_.SetInRetryOperationContext(Previous_);
    }

private:
    TClient& Client_;
    bool Previous_;
};

template<typename TStatusType>
const TStatus& GetRetryStatus(const TStatusType& status) {
    if constexpr (std::is_base_of_v<TStatus, std::decay_t<TStatusType>>) {
        return status;
    } else {
        return status.Status();
    }
}

template<typename TStatusType>
EStatus GetRetryStatusCode(const TStatusType& status) {
    return GetRetryStatus(status).GetStatus();
}

} // namespace NYdb::NRetry
