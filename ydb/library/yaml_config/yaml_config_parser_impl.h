#pragma once

#include <util/generic/string.h>

namespace NKikimr::NYaml {

ui32 ErasureStrToNum(const TString& info);

ui64 PdiskCategoryFromString(const TString& data);

} // namespace NKikimr::NYaml
