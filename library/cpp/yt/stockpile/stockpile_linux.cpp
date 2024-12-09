#include "stockpile.h"

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/misc/global.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

#include <library/cpp/yt/logging/logger.h>

#include <thread>

#include <sys/mman.h>

#include <util/system/thread.h>

#include <string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Stockpile");
constexpr int MADV_STOCKPILE = 0x59410004;

} // namespace

class TStockpileManagerImpl
{
public:
    static TStockpileManagerImpl* Get()
    {
        return LeakySingleton<TStockpileManagerImpl>();
    }

    void Reconfigure(TStockpileOptions options)
    {
        auto guard = Guard(SpinLock_);

        Run_.store(false);
        for (const auto& thread : Threads_) {
            thread->join();
        }

        Threads_.clear();
        Run_.store(true);

        Options_ = options;

        for (int threadIndex = 0; threadIndex < Options_.ThreadCount; ++threadIndex) {
            Threads_.push_back(std::make_unique<std::thread>(&TStockpileManagerImpl::ThreadMain, this));
        }
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND();

    const i64 PageSize_ = sysconf(_SC_PAGESIZE);

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::vector<std::unique_ptr<std::thread>> Threads_;
    TStockpileOptions Options_;
    std::atomic<bool> Run_ = false;

    void ThreadMain()
    {
        TThread::SetCurrentThreadName("Stockpile");

        auto bufferSize = Options_.BufferSize;
        auto period = Options_.Period;

        while (Run_.load()) {
            switch (Options_.Strategy) {
                case EStockpileStrategy::FixedBreaks:
                    RunWithFixedBreaks(Options_.BufferSize, Options_.Period);
                    break;

                case EStockpileStrategy::FlooredLoad:
                    RunWithCappedLoad(Options_.BufferSize, Options_.Period);
                    break;

                case EStockpileStrategy::ProgressiveBackoff:
                    std::tie(bufferSize, period) = RunWithBackoffs(bufferSize, period);
                    break;

                default:
                    YT_ABORT();
            }
        }
    }

    void RunWithFixedBreaks(i64 bufferSize, TDuration period)
    {
        auto returnCode = -::madvise(nullptr, bufferSize, MADV_STOCKPILE);
        YT_LOG_DEBUG_IF(returnCode != 0, "System call \"madvise\" failed: %v", strerror(returnCode));

        Sleep(period);
    }

    void RunWithCappedLoad(i64 bufferSize, TDuration period)
    {
        auto started = GetApproximateCpuInstant();

        auto returnCode = -::madvise(nullptr, bufferSize, MADV_STOCKPILE);
        YT_LOG_DEBUG_IF(returnCode != 0, "System call \"madvise\" failed: %v", strerror(returnCode));

        auto duration = CpuDurationToDuration(GetApproximateCpuInstant() - started);
        if (duration < period) {
            Sleep(period - duration);
        }
    }

    std::pair<i64, TDuration> RunWithBackoffs(
        i64 adjustedBufferSize,
        TDuration adjustedPeriod)
    {
        int returnCode = -::madvise(nullptr, adjustedBufferSize, MADV_STOCKPILE);
        YT_LOG_DEBUG_IF(returnCode != 0, "System call \"madvise\" failed: %v", strerror(returnCode));

        switch(returnCode) {
            case 0:
                Sleep(Options_.Period);
                return {Options_.BufferSize, Options_.Period};

            case ENOMEM:
                if (adjustedBufferSize / 2 >= PageSize_) {
                    // Immediately make an attempt to reclaim half as much.
                    adjustedBufferSize = adjustedBufferSize / 2;
                } else {
                    // Unless there is not even a single reclaimable page.
                    Sleep(Options_.Period);
                }
                return {adjustedBufferSize, Options_.Period};

            case EAGAIN:
            case EINTR:
                Sleep(adjustedPeriod);
                return {Options_.BufferSize, adjustedPeriod + Options_.Period};

            default:
                Sleep(Options_.Period);
                return {Options_.BufferSize, Options_.Period};
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void TStockpileManager::Reconfigure(TStockpileOptions options)
{
    TStockpileManagerImpl::Get()->Reconfigure(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
