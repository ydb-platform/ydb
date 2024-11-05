#pragma once

#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

#include <library/cpp/threading/future/core/fwd.h>
#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/system/types.h>

#include <functional>
#include <memory>

namespace NYdb {
class IClientImplCommon;
}

namespace NYdb::NRetry {

ui32 CalcBackoffTime(const TBackoffSettings& settings, ui32 retryNumber);
void Backoff(const NRetry::TBackoffSettings& settings, ui32 retryNumber);
void AsyncBackoff(std::shared_ptr<IClientImplCommon> client, const TBackoffSettings& settings,
    ui32 retryNumber, const std::function<void()>& fn);

enum class NextStep {
    RetryImmediately,
    RetryFastBackoff,
    RetrySlowBackoff,
    Finish,
};

class TRetryContextBase : TNonCopyable {
protected:
    TRetryOperationSettings Settings_;
    ui32 RetryNumber_;
    TSimpleTimer RetryTimer_;

protected:
    TRetryContextBase(const TRetryOperationSettings& settings)
        : Settings_(settings)
        , RetryNumber_(0)
    {}

    virtual void Reset() {}

    void LogRetry(const TStatus& status) {
        if (Settings_.Verbose_) {
            Cerr << "Previous query attempt was finished with unsuccessful status "
                << status.GetStatus() << ": " << status.GetIssues().ToString(true) << Endl;
            Cerr << "Sending retry attempt " << RetryNumber_ << " of " << Settings_.MaxRetries_ << Endl;
        }
    }

    NextStep GetNextStep(const TStatus& status) {
        if (status.IsSuccess()) {
            return NextStep::Finish;
        }
        if (RetryNumber_ >= Settings_.MaxRetries_) {
            return NextStep::Finish;
        }
        if (RetryTimer_.Get() >= Settings_.MaxTimeout_) {
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

            default:
                return Settings_.RetryUndefined_ ? NextStep::RetrySlowBackoff : NextStep::Finish;
        }
    }

    TDuration GetRemainingTimeout() {
        return Settings_.MaxTimeout_ - RetryTimer_.Get();
    }
};

} // namespace NYdb::NRetry
