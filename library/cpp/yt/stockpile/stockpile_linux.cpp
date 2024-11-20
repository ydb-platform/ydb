#include "stockpile.h"

#include "library/cpp/yt/logging/logger.h"

#include <thread>
#include <mutex>

#include <sys/mman.h>

#include <util/system/thread.h>
#include <string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const auto Logger = NLogging::TLogger("Stockpile");

constexpr int MADV_STOCKPILE = 0x59410004;

////////////////////////////////////////////////////////////////////////////////

namespace {

void RunWithFixedBreaks(i64 bufferSize, TDuration period)
{
    auto returnCode = -::madvise(nullptr, bufferSize, MADV_STOCKPILE);
    YT_LOG_DEBUG_IF(returnCode, "System call \"madvise\" failed: %v", strerror(returnCode));
    Sleep(period);
}

void RunWithCappedLoad(i64 bufferSize, TDuration period)
{
    auto started = GetApproximateCpuInstant();
    auto returnCode = -::madvise(nullptr, bufferSize, MADV_STOCKPILE);
    YT_LOG_DEBUG_IF(returnCode, "System call \"madvise\" failed: %v", strerror(returnCode));
    auto duration = CpuDurationToDuration(GetApproximateCpuInstant() - started);

    if (duration < period) {
        Sleep(period - duration);
    }
}

std::pair<i64, TDuration> RunWithBackoffs(
    i64 adjustedBufferSize,
    TDuration adjustedPeriod,
    const TStockpileOptions& options,
    i64 pageSize)
{
    int returnCode = -::madvise(nullptr, adjustedBufferSize, MADV_STOCKPILE);
    YT_LOG_DEBUG_IF(returnCode, "System call \"madvise\" failed: %v", strerror(returnCode));

    switch(returnCode) {
        case 0:
            Sleep(options.Period);
            return {options.BufferSize, options.Period};

        case ENOMEM:
            if (adjustedBufferSize / 2 >= pageSize) {
                // Immediately make an attempt to reclaim half as much.
                adjustedBufferSize = adjustedBufferSize / 2;
            } else {
                // Unless there is not even a single reclaimable page.
                Sleep(options.Period);
            }
            return {adjustedBufferSize, options.Period};

        case EAGAIN:
        case EINTR:
            Sleep(adjustedPeriod);
            return {options.BufferSize, adjustedPeriod + options.Period};

        default:
            Sleep(options.Period);
            return {options.BufferSize, options.Period};
    }
}

} // namespace

void RunStockpileThread(TStockpileOptions options, std::atomic<bool>* shouldProceed)
{
    TThread::SetCurrentThreadName("Stockpile");

    const i64 pageSize = sysconf(_SC_PAGESIZE);
    auto bufferSize = options.BufferSize;
    auto period = options.Period;

    while (!shouldProceed || shouldProceed->load()) {
        switch (options.Strategy) {
            case EStockpileStrategy::FixedBreaks:
                RunWithFixedBreaks(options.BufferSize, options.Period);
                break;

            case EStockpileStrategy::FlooredLoad:
                RunWithCappedLoad(options.BufferSize, options.Period);
                break;

            case EStockpileStrategy::ProgressiveBackoff:
                std::tie(bufferSize, period) = RunWithBackoffs(bufferSize, period, options, pageSize);
                break;

            default:
                YT_ABORT();
        }
    }
}

void RunDetachedStockpileThreads(TStockpileOptions options)
{
    static std::once_flag OnceFlag;
    std::call_once(OnceFlag, [options = std::move(options)] {
        for (int i = 0; i < options.ThreadCount; ++i) {
            std::thread(RunStockpileThread, options, nullptr).detach();
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
