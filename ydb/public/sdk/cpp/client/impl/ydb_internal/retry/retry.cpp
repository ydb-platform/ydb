#include "retry.h"
#include <util/random/random.h>
#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/iface.h>

namespace NYdb::NRetry {

constexpr ui32 MAX_BACKOFF_DURATION_MS = TDuration::Hours(1).MilliSeconds();

ui32 CalcBackoffTime(const TBackoffSettings& settings, ui32 retryNumber) {
    ui32 backoffSlots = 1 << std::min(retryNumber, settings.Ceiling_);
    TDuration maxDuration = settings.SlotDuration_ * backoffSlots;

    double uncertaintyRatio = std::max(std::min(settings.UncertainRatio_, 1.0), 0.0);
    double uncertaintyMultiplier = RandomNumber<double>() * uncertaintyRatio - uncertaintyRatio + 1.0;

    double durationMs = round(maxDuration.MilliSeconds() * uncertaintyMultiplier);

    return std::max(std::min(durationMs, (double)MAX_BACKOFF_DURATION_MS), 0.0);
}

void Backoff(const NRetry::TBackoffSettings& settings, ui32 retryNumber) {
    auto durationMs = CalcBackoffTime(settings, retryNumber);
    Sleep(TDuration::MilliSeconds(durationMs));
}

void AsyncBackoff(std::shared_ptr<IClientImplCommon> client, const TBackoffSettings& settings,
    ui32 retryNumber, const std::function<void()>& fn)
{
    auto durationMs = CalcBackoffTime(settings, retryNumber);
    client->ScheduleTask(fn, TDuration::MilliSeconds(durationMs));
}

}

