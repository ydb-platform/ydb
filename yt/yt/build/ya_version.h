#pragma once

#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString CreateBranchCommitVersion(TStringBuf branch);
TString CreateYTVersion(int major, int minor, int patch, TStringBuf branch);
TString GetYaHostName();
TString GetYaBuildDate();

const TString& GetRpcUserAgent();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

