#pragma once
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TSettings {
private:
    static const ui32 SparsedDetectorKff = 4;

public:
    static const ui32 ColumnAccessorsCountLimit = 1024;

    static bool IsSparsed(const ui32 keyUsageCount, const ui32 recordsCount) {
        AFL_VERIFY(recordsCount);
        return keyUsageCount * SparsedDetectorKff < recordsCount;
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
