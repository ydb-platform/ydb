#pragma once
#include <util/generic/string.h>

namespace NFq {

TString GetRateLimiterResourcePath(TStringBuf cloud, TStringBuf folder, TStringBuf query);
TString GetRateLimiterResourcePath(TStringBuf cloud);

} // namespace NFq
