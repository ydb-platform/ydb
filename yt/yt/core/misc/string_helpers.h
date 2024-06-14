#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

extern const TStringBuf DefaultTruncatedMessage;

void TruncateStringInplace(TString* string, int lengthLimit, TStringBuf truncatedSuffix = DefaultTruncatedMessage);

TString TruncateString(TString string, int lengthLimit, TStringBuf truncatedSuffix = DefaultTruncatedMessage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
