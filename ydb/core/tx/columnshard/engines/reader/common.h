#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NIndexedReader {

class TBatchAddress {
private:
    YDB_READONLY(ui32, GranuleIdx, 0);
    YDB_READONLY(ui32, BatchGranuleIdx, 0);
public:
    TString ToString() const;

    TBatchAddress(const ui32 granuleIdx, const ui32 batchGranuleIdx);
};

}
