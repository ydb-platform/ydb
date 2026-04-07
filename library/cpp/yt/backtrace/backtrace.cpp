#include "backtrace.h"

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void SymbolizeBacktraceImpl(
    TBacktrace backtrace,
    const std::function<void(TStringBuf)>& writeCallback);

} // namespace NDetail

void SymbolizeBacktrace(
    TBacktrace backtrace,
    const std::function<void(TStringBuf)>& writeCallback,
    void* startPC)
{
    if (backtrace.empty()) {
        writeCallback(TStringBuf("<stack trace is not available>"));
    } else {
        TBacktrace::const_iterator it;
        if (startPC && (it = std::find(backtrace.Begin(), backtrace.End(), startPC)) != backtrace.End()) {
            backtrace = backtrace.Slice(it - backtrace.Begin(), backtrace.Size());
        }
        NDetail::SymbolizeBacktraceImpl(backtrace, writeCallback);
    }
}

std::string SymbolizeBacktrace(TBacktrace backtrace)
{
    std::string result;
    SymbolizeBacktrace(
        backtrace,
        [&] (TStringBuf str) { result += str; });
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
