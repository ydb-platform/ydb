#include "backtrace.h"

namespace NYT::NBacktrace {

////////////////////////////////////////////////////////////////////////////////

TString SymbolizeBacktrace(TBacktrace backtrace)
{
    TString result;
    SymbolizeBacktrace(
        backtrace,
        [&] (TStringBuf str) { result += str; });
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBacktrace
