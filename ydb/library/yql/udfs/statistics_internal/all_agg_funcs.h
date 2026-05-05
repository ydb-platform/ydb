#pragma once

#include "cms_agg_func.h"
#include "ewh_agg_func.h"
#include "hll_agg_func.h"

namespace NKikimr::NStat::NAggFuncs {

using TAllAggFuncsList = TTypeList<
    TCMSAggFunc,
    TEWHAggFunc,
    THLLAggFunc
>;

} // NKikimr::NStat::NAggFuncs
