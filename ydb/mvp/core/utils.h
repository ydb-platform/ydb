#pragma once

#include <util/generic/string.h>

namespace NMVP {

extern const TStringBuf CONFIG_ERROR_PREFIX;

bool TryLoadTokenFromFile(const TString& tokenPath, TString& token, TString& error, const TString& tokenName = TString());

} // namespace NMVP
