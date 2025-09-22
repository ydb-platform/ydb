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

Y_NO_INLINE TBacktraceView GetBacktrace(TBacktraceBuffer* buffer)
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

TBacktrace GetBacktrace()
{
    NDetail::TBacktraceBuffer buffer;
    auto backtraceRange = NDetail::GetBacktrace(&buffer);
    return TBacktrace(backtraceRange.begin(), backtraceRange.end());
}

std::string DumpBacktrace()
{
    NDetail::TBacktraceBuffer buffer;
    auto backtrace = NDetail::GetBacktrace(&buffer);
    return SymbolizeBacktrace(backtrace);
}

std::string SymbolizeBacktrace(TBacktraceView backtrace)
{
    std::string result;
    NDetail::SymbolizeBacktrace(
        backtrace,
        [&] (TStringBuf str) { result += str; });
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
