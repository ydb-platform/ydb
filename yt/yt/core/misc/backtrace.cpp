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

Y_NO_INLINE TBacktrace GetBacktrace(TBacktraceBuffer* buffer)
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

std::string DumpBacktrace(void* startPC)
{
    std::string result;
    DumpBacktrace(
        [&] (TStringBuf str) { result += str; },
        startPC);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
