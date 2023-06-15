#include "common.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexedReader {

TString TBatchAddress::ToString() const {
    return TStringBuilder() << GranuleId << "," << BatchGranuleIdx;
}

TBatchAddress::TBatchAddress(const ui32 granuleId, const ui32 batchGranuleIdx)
    : GranuleId(granuleId)
    , BatchGranuleIdx(batchGranuleIdx)
{

}

}
