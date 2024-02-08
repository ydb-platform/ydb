#include "stockpile.h"

#include <thread>
#include <mutex>

#include <sys/mman.h>

#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

void RunStockpile(const TStockpileOptions& options)
{
    TThread::SetCurrentThreadName("Stockpile");

    constexpr int MADV_STOCKPILE = 0x59410004;

    while (true) {
        ::madvise(nullptr, options.BufferSize, MADV_STOCKPILE);
        Sleep(options.Period);
    }
}

} // namespace

void ConfigureStockpile(const TStockpileOptions& options)
{
    static std::once_flag OnceFlag;
    std::call_once(OnceFlag, [options] {
        for (int i = 0; i < options.ThreadCount; i++) {
            std::thread(RunStockpile, options).detach();
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
