#pragma once

#include <util/generic/fwd.h>
#include <util/system/types.h>

namespace NYql::NUdf {

TString AbiVersionToStr(ui32 version);

} // namespace NYql::NUdf

extern "C" ui32 YqlCheckAbiVersion(ui32 loaded, ui32 built);
