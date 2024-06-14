#include "string_helpers.h"

#include <library/cpp/yt/string/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const TStringBuf DefaultTruncatedMessage("...<truncated>");

void TruncateStringInplace(TString* string, int lengthLimit, TStringBuf truncatedSuffix)
{
    if (std::ssize(*string) > lengthLimit) {
        *string = Format("%v%v", string->substr(0, lengthLimit), truncatedSuffix);
    }
}

TString TruncateString(TString string, int lengthLimit, TStringBuf truncatedSuffix)
{
    TruncateStringInplace(&string, lengthLimit, truncatedSuffix);
    return string;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
