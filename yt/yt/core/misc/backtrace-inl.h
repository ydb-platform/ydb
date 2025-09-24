#ifndef BACKTRACE_INL_H_
#error "Direct inclusion of this file is not allowed, include backtrace.h"
// For the sake of sane code completion.
#include "backtrace.h"
#endif

#include <library/cpp/yt/backtrace/backtrace.h>

#include <algorithm>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TBacktraceBuffer = std::array<const void*, 99>; // 99 is to keep formatting :)
TBacktraceView GetBacktrace(TBacktraceBuffer* buffer);

template <class TCallback>
void SymbolizeBacktrace(TBacktraceView backtrace, TCallback writeCallback, void* startPC = nullptr)
{
    if (backtrace.empty()) {
        writeCallback(TStringBuf("<stack trace is not available>"));
    } else {
        NDetail::TBacktraceBuffer::const_iterator it;
        if (startPC && (it = std::find(backtrace.Begin(), backtrace.End(), startPC)) != backtrace.End()) {
            backtrace = backtrace.Slice(it - backtrace.Begin(), backtrace.Size());
        }
        NBacktrace::SymbolizeBacktrace(backtrace, writeCallback);
    }
}

} // namespace NDetail

template <class TCallback>
Y_NO_INLINE void DumpBacktrace(TCallback writeCallback, void* startPC)
{
    NDetail::TBacktraceBuffer buffer;
    auto backtrace = NDetail::GetBacktrace(&buffer);
    NDetail::SymbolizeBacktrace(backtrace, writeCallback, startPC);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
