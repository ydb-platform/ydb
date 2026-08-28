#pragma once

#include "issues.h"

namespace NKikimr::NPDiskTool {

// Accepts decimal, 0x-hex, or the token YdbDefaultPDiskSequence / default.
ui64 ParseMainKeyArg(TStringBuf value);

TMainKey MakeMainKey(
    const TVector<ui64>& numericKeys,
    const TString& keyFile,
    const TString& pin,
    bool keySpecified,
    TIssueLog& issues);

} // namespace NKikimr::NPDiskTool
