#pragma once

#include "cms_agg_func.h"
#include "eqh_agg_func.h"
#include "ewh_agg_func.h"
#include "hll_agg_func.h"

namespace NKikimr::NStat::NAggFuncs {

using TAllAggFuncsList = TTypeList<
    TCMSAggFunc,
    TEWHAggFunc,
    TEQHAggFunc,
    THLLAggFunc
>;

} // NKikimr::NStat::NAggFuncs
