#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexedReader {

class TBatchAddress {
private:
    ui32 GranuleIdx = 0;
    ui32 BatchGranuleIdx = 0;
public:
    TString ToString() const;

    TBatchAddress(const ui32 granuleIdx, const ui32 batchGranuleIdx);

    ui32 GetGranuleIdx() const {
        return GranuleIdx;
    }

    ui32 GetBatchGranuleIdx() const {
        return BatchGranuleIdx;
    }
};

}
