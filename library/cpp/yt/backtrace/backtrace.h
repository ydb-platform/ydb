#pragma once

#include <library/cpp/yt/memory/range.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

using TBacktrace = TRange<const void*>;
using TBacktraceBuffer = TMutableRange<const void*>;

//! Obtains a backtrace via a given cursor.
/*!
 *  \param buffer is the buffer where the backtrace is written to
 *  \param framesToSkip is the number of top frames to skip
 *  \returns the portion of #buffer that has actually been filled
 */
template <class TCursor>
TBacktrace GetBacktrace(
    TCursor* cursor,
    TBacktraceBuffer buffer,
    int framesToSkip);

//! Symbolizes a backtrace invoking a given callback for each frame.
/*!
 *  \param backtrace Backtrace to symbolize
 *  \param frameCallback Callback to invoke per each frame
 */
void SymbolizeBacktrace(
    TBacktrace backtrace,
    const std::function<void(TStringBuf)>& frameCallback);

//! Symbolizes a backtrace to a string.
/*!
 *  \param backtrace Backtrace to symbolize
 */
TString SymbolizeBacktrace(TBacktrace backtrace);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace

#define BACKTRACE_INL_H_
#include "backtrace-inl.h"
#undef BACKTRACE_INL_H_
