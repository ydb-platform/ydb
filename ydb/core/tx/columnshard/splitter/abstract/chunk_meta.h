#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <optional>
#include <memory>

namespace NKikimr::NOlap {

class TSimpleChunkMeta {
protected:
    std::shared_ptr<arrow::Scalar> Max;
    ui32 NumRows = 0;
    ui32 RawBytes = 0;
    TSimpleChunkMeta() = default;
public:
    TSimpleChunkMeta(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column, const bool needMinMax, const bool isSortedColumn);

    ui64 GetMetadataSize() const {
        return sizeof(ui32) + sizeof(ui32) + 8 * 3 * 2;
    }

    std::shared_ptr<arrow::Scalar> GetMax() const {
        return Max;
    }
    ui32 GetNumRows() const {
        return NumRows;
    }
    ui32 GetRecordsCount() const {
        return NumRows;
    }
    ui32 GetRawBytes() const {
        return RawBytes;
    }

    bool HasMax() const noexcept {
        return Max.get();
    }

};
}
