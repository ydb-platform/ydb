#pragma once
#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <optional>
#include <memory>

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

};
}
