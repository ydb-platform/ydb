#ifndef CRASH_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include crash_handler.h"
// For the sake of sane code completion.
#include "crash_handler.h"
#endif

#include <library/cpp/yt/backtrace/backtrace.h>

#include <algorithm>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TStackTrace = TRange<const void*>;
using TStackTraceBuffer = std::array<const void*, 99>; // 99 is to keep formatting :)
TStackTrace GetStackTrace(TStackTraceBuffer* buffer);

} // namespace NDetail

template <class TCallback>
Y_NO_INLINE void DumpStackTrace(TCallback writeCallback, void* startPC)
{
    NDetail::TStackTraceBuffer buffer;
    auto frames = NDetail::GetStackTrace(&buffer);
    if (frames.empty()) {
        writeCallback(TStringBuf("<stack trace is not available>"));
    } else {
        NDetail::TStackTraceBuffer::const_iterator it;
        if (startPC && (it = std::find(frames.Begin(), frames.End(), startPC)) != frames.End()) {
            frames = frames.Slice(it - frames.Begin(), frames.Size());
        }
        NBacktrace::SymbolizeBacktrace(frames, writeCallback);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
