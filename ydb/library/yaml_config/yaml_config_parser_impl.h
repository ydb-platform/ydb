#pragma once

#include <util/generic/string.h>

namespace NKikimr::NYaml {

ui64 PdiskCategoryFromString(const TString& data);
ui32 ErasureStrToNum(const TString& info);

} // namespace NKikimr::NYaml
