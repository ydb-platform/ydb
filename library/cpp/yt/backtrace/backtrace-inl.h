#pragma once
#ifndef BACKTRACE_INL_H_
#error "Direct inclusion of this file is not allowed, include backtrace.h"
// For the sake of sane code completion.
#include "backtrace.h"
#endif

#include <util/system/compiler.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

template <class TCursor>
Y_NO_INLINE TBacktrace GetBacktrace(
    TCursor* cursor,
    TBacktraceBuffer buffer,
    int framesToSkip)
{
    // Account for the current frame.
    ++framesToSkip;
    size_t frameCount = 0;
    while (frameCount < buffer.size() && !cursor->IsFinished()) {
        if (framesToSkip > 0) {
            --framesToSkip;
        } else {
            buffer[frameCount++] = cursor->GetCurrentIP();
        }
        cursor->MoveNext();
    }
    return {buffer.begin(), frameCount};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
