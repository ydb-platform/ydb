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

using TBacktrace = TRange<const void*>;
using TBacktraceBuffer = std::array<const void*, 99>; // 99 is to keep formatting :)
TBacktrace GetBacktrace(TBacktraceBuffer* buffer);

} // namespace NDetail

template <class TCallback>
Y_NO_INLINE void DumpBacktrace(TCallback writeCallback, void* startPC)
{
    NDetail::TBacktraceBuffer buffer;
    auto frames = NDetail::GetBacktrace(&buffer);
    if (frames.empty()) {
        writeCallback(TStringBuf("<stack trace is not available>"));
    } else {
        NDetail::TBacktraceBuffer::const_iterator it;
        if (startPC && (it = std::find(frames.Begin(), frames.End(), startPC)) != frames.End()) {
            frames = frames.Slice(it - frames.Begin(), frames.Size());
        }
        NBacktrace::SymbolizeBacktrace(frames, writeCallback);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
