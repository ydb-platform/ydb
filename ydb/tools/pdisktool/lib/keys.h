#pragma once

#include "issues.h"

namespace NKikimr::NPDiskTool {

TMainKey MakeMainKey(
    const TVector<ui64>& numericKeys,
    const TString& keyFile,
    const TString& pin,
    bool keySpecified,
    TIssueLog& issues);

} // namespace NKikimr::NPDiskTool
