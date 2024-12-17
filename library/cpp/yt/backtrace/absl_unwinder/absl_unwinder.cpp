#include "absl_unwinder.h"

#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>

#include <absl/debugging/stacktrace.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

namespace {

int AbslStackUnwinder(
    void** frames,
    int* /*framesSizes*/,
    int maxFrames,
    int skipFrames,
    const void* /*uc*/,
    int* /*minDroppedFrames*/)
{
    NBacktrace::TLibunwindCursor cursor;

    for (int i = 0; i < skipFrames + 1; ++i) {
        cursor.MoveNext();
    }

    int count = 0;
    for (int i = 0; i < maxFrames; ++i) {
        if (cursor.IsFinished()) {
            return count;
        }

        // IP point's to return address. Subtract 1 to get accurate line information for profiler.
        frames[i] = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(cursor.GetCurrentIP()) - 1);
        count++;

        cursor.MoveNext();
    }
    return count;
}

} // namespace

void SetAbslStackUnwinder()
{
    absl::SetStackUnwinder(AbslStackUnwinder);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
