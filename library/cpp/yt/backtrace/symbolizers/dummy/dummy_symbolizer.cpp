#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/string/raw_formatter.h>

namespace NYT::NBacktrace::NDetail {

////////////////////////////////////////////////////////////////////////////////

void SymbolizeBacktraceImpl(
    TBacktrace backtrace,
    const std::function<void(TStringBuf)>& writeCallback)
{
    for (int index = 0; index < std::ssize(backtrace); ++index) {
        TRawFormatter<1024> formatter;
        formatter.AppendNumber(index + 1, 10, 2);
        formatter.AppendString(". 0x");
        formatter.AppendNumber(reinterpret_cast<uintptr_t>(backtrace[index]), /*radix*/ 16, /*width*/ 12);
        formatter.AppendString("\n");
        writeCallback(formatter.GetBuffer());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace::NDetail
