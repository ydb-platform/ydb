#include "backtrace.h"

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

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
