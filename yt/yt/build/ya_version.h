#pragma once

#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::string CreateBranchCommitVersion(TStringBuf branch);
std::string CreateYTVersion(int major, int minor, int patch, TStringBuf branch);
std::string GetYaHostName();
std::string GetYaBuildDate();

const std::string& GetRpcUserAgent();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

