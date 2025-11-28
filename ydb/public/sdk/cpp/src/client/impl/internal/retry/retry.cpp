#include "retry.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>

#include <ydb/public/sdk/cpp/src/client/common_client/impl/iface.h>

#include <util/random/random.h>

#include <thread>

namespace NYdb::inline Dev::NRetry {

using TBackoffDuration = std::chrono::duration<double, std::micro>;

constexpr TBackoffDuration MAX_BACKOFF_DURATION = std::chrono::hours(1);

namespace {

TBackoffDuration CalcBackoffTime(const TBackoffSettings& settings, std::uint32_t retryNumber) {
    std::uint32_t backoffSlots = 1 << std::min(retryNumber, settings.Ceiling_);
    TBackoffDuration maxDuration(settings.SlotDuration_.MicroSeconds() * backoffSlots);

    double uncertaintyRatio = std::max(std::min(settings.UncertainRatio_, 1.0), 0.0);
    double uncertaintyMultiplier = RandomNumber<double>() * uncertaintyRatio - uncertaintyRatio + 1.0;

    return std::max(std::min(maxDuration * uncertaintyMultiplier, MAX_BACKOFF_DURATION), TBackoffDuration::zero());
}

}

void Backoff(const NRetry::TBackoffSettings& settings, std::uint32_t retryNumber) {
    std::this_thread::sleep_for(CalcBackoffTime(settings, retryNumber));
}

void AsyncBackoff(std::shared_ptr<IClientImplCommon> client, const TBackoffSettings& settings,
    std::uint32_t retryNumber, const std::function<void()>& fn)
{
    client->ScheduleTask(fn, std::chrono::duration_cast<TDeadline::Duration>(CalcBackoffTime(settings, retryNumber)));
}

}
