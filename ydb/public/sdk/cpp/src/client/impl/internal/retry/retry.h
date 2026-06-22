#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/time/time.h>

#include <library/cpp/threading/future/core/fwd.h>
#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/system/types.h>
#include <util/string/cast.h>

#include <functional>
#include <memory>
#include <type_traits>

namespace NYdb::inline Dev {
class IClientImplCommon;
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

inline bool ShouldRetryStatus(EStatus status, const TRetryOperationSettings& settings) {
    switch (status) {
        case EStatus::ABORTED:
        case EStatus::OVERLOADED:
        case EStatus::CLIENT_RESOURCE_EXHAUSTED:
        case EStatus::UNAVAILABLE:
        case EStatus::BAD_SESSION:
        case EStatus::SESSION_BUSY:
            return true;
        case EStatus::NOT_FOUND:
            return settings.RetryNotFound_;
        case EStatus::UNDETERMINED:
        case EStatus::TRANSPORT_UNAVAILABLE:
            return settings.Idempotent_;
        default:
            return settings.RetryUndefined_;
    }
}

class TRetryContextBase : TNonCopyable {
protected:
    TRetryOperationSettings Settings_;
    std::uint32_t RetryNumber_;
    TInstant RetryStartTime_;

protected:
    TRetryContextBase(const TRetryOperationSettings& settings)
        : Settings_(settings)
        , RetryNumber_(0)
        , RetryStartTime_(TInstant::Now())
    {}

    virtual void Reset() {}

    void LogRetry(const TStatus& status) {
        if (Settings_.Verbose_) {
            std::cerr << "Previous query attempt was finished with unsuccessful status "
                << ToString(status.GetStatus()) << ": " << status.GetIssues().ToString(true) << std::endl;
            std::cerr << "Sending retry attempt " << RetryNumber_ << " of " << Settings_.MaxRetries_ << std::endl;
        }
    }

    NextStep GetNextStep(const TStatus& status) {
        if (status.IsSuccess()) {
            return NextStep::Finish;
        }
        if (RetryNumber_ >= Settings_.MaxRetries_) {
            return NextStep::Finish;
        }
        if (TInstant::Now() - RetryStartTime_ >= Settings_.MaxTimeout_) {
            return NextStep::Finish;
        }
        switch (status.GetStatus()) {
            case EStatus::ABORTED:
                return NextStep::RetryImmediately;

            case EStatus::OVERLOADED:
            case EStatus::CLIENT_RESOURCE_EXHAUSTED:
                return NextStep::RetrySlowBackoff;

            case EStatus::UNAVAILABLE:
                return NextStep::RetryFastBackoff;

            case EStatus::BAD_SESSION:
            case EStatus::SESSION_BUSY:
                Reset();
                return NextStep::RetryImmediately;

            case EStatus::NOT_FOUND:
                if (Settings_.RetryNotFound_) {
                    return NextStep::RetryImmediately;
                } else {
                    return NextStep::Finish;
                }

            case EStatus::UNDETERMINED:
                if (Settings_.Idempotent_) {
                    return NextStep::RetryFastBackoff;
                } else {
                    return NextStep::Finish;
                }

            case EStatus::TRANSPORT_UNAVAILABLE:
                if (Settings_.Idempotent_) {
                    Reset();
                    return NextStep::RetryFastBackoff;
                } else {
                    return NextStep::Finish;
                }

            case EStatus::CLIENT_DEADLINE_EXCEEDED:
                Reset();
                [[fallthrough]];
            default:
                return Settings_.RetryUndefined_ ? NextStep::RetrySlowBackoff : NextStep::Finish;
        }
    }

    TDuration GetRemainingTimeout() {
        return Settings_.MaxTimeout_ - (TInstant::Now() - RetryStartTime_);
    }
};

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
