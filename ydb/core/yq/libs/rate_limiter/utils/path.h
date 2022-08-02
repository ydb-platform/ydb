#pragma once
#include <util/generic/string.h>

namespace NYq {

TString GetRateLimiterResourcePath(TStringBuf cloud, TStringBuf folder, TStringBuf query);

} // namespace NYq