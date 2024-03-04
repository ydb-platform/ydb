#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/system/types.h>

namespace NKikimr::NOlap {

class TSplitSettings {
private:
    static const inline i64 DefaultMaxBlobSize = 8 * 1024 * 1024;
    static const inline i64 DefaultMinBlobSize = 4 * 1024 * 1024;
    static const inline i64 DefaultMinRecordsCount = 10000;
    static const inline i64 DefaultMaxPortionSize = 4 * DefaultMaxBlobSize;
    YDB_ACCESSOR(i64, MaxBlobSize, DefaultMaxBlobSize);
    YDB_ACCESSOR(i64, MinBlobSize, DefaultMinBlobSize);
    YDB_ACCESSOR(i64, MinRecordsCount, DefaultMinRecordsCount);
    YDB_ACCESSOR(i64, MaxPortionSize, DefaultMaxPortionSize);
public:
};
}
