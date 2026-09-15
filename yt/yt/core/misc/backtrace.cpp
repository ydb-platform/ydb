#include "backtrace.h"

#include <library/cpp/yt/backtrace/backtrace.h>

#ifdef _unix_
#include <library/cpp/yt/backtrace/cursors/libunwind/libunwind_cursor.h>
#else
#include <library/cpp/yt/backtrace/cursors/dummy/dummy_cursor.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TBacktraceBuffer = std::array<const void*, 99>; // 99 is to keep formatting :)

Y_NO_INLINE NBacktrace::TBacktrace GetBacktrace(TBacktraceBuffer* buffer)
{
#ifdef _unix_
    NBacktrace::TLibunwindCursor cursor;
#else
    NBacktrace::TDummyCursor cursor;
#endif
    return NBacktrace::GetBacktrace(
        &cursor,
        TMutableRange(*buffer),
        /*framesToSkip*/ 2);
}

} // namespace NDetail

TCapturedBacktrace CaptureBacktrace()
{
    NDetail::TBacktraceBuffer buffer;
    auto backtraceRange = NDetail::GetBacktrace(&buffer);
    return TCapturedBacktrace(backtraceRange.begin(), backtraceRange.end());
}

Y_NO_INLINE void DumpBacktrace(const std::function<void(TStringBuf)>& writeCallback, void* startPC)
{
    NDetail::TBacktraceBuffer buffer;
    auto backtrace = NDetail::GetBacktrace(&buffer);
    NBacktrace::SymbolizeBacktrace(backtrace, writeCallback, startPC);
}

std::string DumpBacktrace()
{
    NDetail::TBacktraceBuffer buffer;
    auto backtrace = NDetail::GetBacktrace(&buffer);
    return NBacktrace::SymbolizeBacktrace(backtrace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
