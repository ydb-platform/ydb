#include <library/cpp/yt/backtrace/backtrace.h>

#include <library/cpp/dwarf_backtrace/backtrace.h>

#include <library/cpp/yt/string/raw_formatter.h>

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

void SymbolizeBacktrace(
    TBacktrace backtrace,
    const std::function<void(TStringBuf)>& frameCallback)
{
    auto error = NDwarf::ResolveBacktrace({backtrace.begin(), backtrace.size()}, [&] (const NDwarf::TLineInfo& info) {
        TRawFormatter<1024> formatter;
        formatter.AppendNumber(info.Index + 1, 10, 2);
        formatter.AppendString(". ");
        formatter.AppendString("0x");
        const int width = (sizeof(void*) == 8 ? 12 : 8);
        // 12 for x86_64 because higher bits are always zeroed.
        formatter.AppendNumber(info.Address, 16, width, '0');
        formatter.AppendString(" in ");
        formatter.AppendString(info.FunctionName);
        const int bytesToAppendEstimate = 4 + info.FileName.Size() + 1 + 4 /* who cares about line numbers > 9999 */ + 1;
        if (formatter.GetBytesRemaining() < bytesToAppendEstimate) {
            const int offset = formatter.GetBytesRemaining() - bytesToAppendEstimate;
            if (formatter.GetBytesWritten() + offset >= 0) {
                formatter.Advance(offset);
            }
        }
        formatter.AppendString(" at ");
        formatter.AppendString(info.FileName);
        formatter.AppendChar(':');
        formatter.AppendNumber(info.Line);
        if (formatter.GetBytesRemaining() == 0) {
            formatter.Revert(1);
        }
        formatter.AppendString("\n");
        frameCallback(formatter.GetBuffer());
        // Call the callback exactly `frameCount` times,
        // even if there are inline functions and one frame resolved to several lines.
        // It needs for case when caller uses `frameCount` less than 100 for pretty formatting.
        if (info.Index + 1 == std::ssize(backtrace)) {
            return NDwarf::EResolving::Break;
        }
        return NDwarf::EResolving::Continue;
    });
    if (error) {
        TRawFormatter<1024> formatter;
        formatter.AppendString("*** Error symbolizing backtrace via Dwarf\n");
        formatter.AppendString("***   Code: ");
        formatter.AppendNumber(error->Code);
        formatter.AppendString("\n");
        formatter.AppendString("***   Message: ");
        formatter.AppendString(error->Message);
        formatter.AppendString("\n");
        frameCallback(formatter.GetBuffer());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
