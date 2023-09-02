#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <optional>
#include <memory>

namespace NKikimr::NOlap {

class TSimpleChunkMeta {
protected:
    std::shared_ptr<arrow::Scalar> Min;
    std::shared_ptr<arrow::Scalar> Max;
    std::optional<ui32> NumRows;
    std::optional<ui32> RawBytes;
    TSimpleChunkMeta() = default;
public:
    TSimpleChunkMeta(const std::shared_ptr<arrow::Array>& column, const bool needMinMax, const bool isSortedColumn);


    ui64 GetMetadataSize() const {
        return sizeof(ui32) + sizeof(ui32) + 8 * 3 * 2;
    }

    std::shared_ptr<arrow::Scalar> GetMin() const {
        return Min;
    }
    std::shared_ptr<arrow::Scalar> GetMax() const {
        return Max;
    }
    std::optional<ui32> GetNumRows() const {
        return NumRows;

    }
    std::optional<ui32> GetRawBytes() const {
        return RawBytes;
    }

    ui32 GetNumRowsVerified() const {
        Y_VERIFY(NumRows);
        return *NumRows;
    }

    ui32 GetRawBytesVerified() const {
        Y_VERIFY(RawBytes);
        return *RawBytes;
    }

    bool HasMinMax() const noexcept {
        return Min.get() && Max.get();
    }

};
}
