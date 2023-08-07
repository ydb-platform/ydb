#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/yt/string/raw_formatter.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

void SymbolizeBacktrace(
    TBacktrace backtrace,
    const std::function<void(TStringBuf)>& frameCallback)
{
    for (int index = 0; index < std::ssize(backtrace); ++index) {
        TRawFormatter<1024> formatter;
        formatter.AppendNumber(index + 1, 10, 2);
        formatter.AppendString(". ");
        formatter.AppendNumberAsHexWithPadding(reinterpret_cast<uintptr_t>(backtrace[index]), 12);
        formatter.AppendString("\n");
        frameCallback(formatter.GetBuffer());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
