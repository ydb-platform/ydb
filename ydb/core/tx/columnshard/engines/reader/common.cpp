#include "common.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap::NIndexedReader {

TString TBatchAddress::ToString() const {
    return TStringBuilder() << GranuleIdx << "," << BatchGranuleIdx;
}

TBatchAddress::TBatchAddress(const ui32 granuleIdx, const ui32 batchGranuleIdx)
    : GranuleIdx(granuleIdx)
    , BatchGranuleIdx(batchGranuleIdx)
{

}

}
