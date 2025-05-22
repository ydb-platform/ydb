#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <memory>
#include <optional>

namespace NKikimr::NArrow::NAccessor {
class IChunkedArray;
}

namespace NKikimr::NOlap {

class TSimpleChunkMeta {
protected:
    ui32 RecordsCount = 0;
    ui32 RawBytes = 0;
    TSimpleChunkMeta() = default;

public:
    TSimpleChunkMeta(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& column);

    ui64 GetMetadataSize() const {
        return sizeof(ui32) + sizeof(ui32);
    }

    ui32 GetRecordsCount() const {
        return RecordsCount;
    }
    ui32 GetRawBytes() const {
        return RawBytes;
    }
    void SetRawBytes(const ui32 value) {
        RawBytes = value;
    }
};
}   // namespace NKikimr::NOlap
