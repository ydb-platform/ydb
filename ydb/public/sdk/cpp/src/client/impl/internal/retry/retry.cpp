#include "retry.h"

#include <util/random/random.h>

#include <algorithm>

namespace NYdb::inline Dev::NRetry {

    TRetryDecision GetRetryDecision(EStatus status, const TRetryOperationSettings& settings) {
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
            case EStatus::TRANSPORT_UNAVAILABLE:
                return {settings.Idempotent_ ? NextStep::RetryFastBackoff : NextStep::Finish,
                        settings.Idempotent_ && status == EStatus::TRANSPORT_UNAVAILABLE};
            case EStatus::CLIENT_DEADLINE_EXCEEDED:
                return {settings.RetryUndefined_ ? NextStep::RetrySlowBackoff : NextStep::Finish, true};
            default:
                return {settings.RetryUndefined_ ? NextStep::RetrySlowBackoff : NextStep::Finish};
        }
    }

    std::chrono::microseconds CalcBackoffTime(const TBackoffSettings& settings, std::uint32_t retryNumber) {
        using TBackoffDuration = std::chrono::duration<double, std::micro>;
        constexpr TBackoffDuration maxBackoff = std::chrono::hours(1);
        const std::uint32_t slots = 1 << std::min(retryNumber, settings.Ceiling_);
        const TBackoffDuration maxDuration(settings.SlotDuration_.MicroSeconds() * slots);
        const double uncertainty = std::clamp(settings.UncertainRatio_, 0.0, 1.0);
        const double multiplier = RandomNumber<double>() * uncertainty - uncertainty + 1.0;
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::clamp(maxDuration * multiplier, TBackoffDuration::zero(), maxBackoff));
    }

} // namespace NYdb::inline Dev::NRetry
