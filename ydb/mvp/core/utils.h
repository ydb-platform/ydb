#pragma once

#include <util/generic/string.h>

namespace NMVP {

bool TryLoadTokenFromFile(const TString& tokenPath, TString& token, TString& error, const TString& tokenName = TString());

} // namespace NMVP
