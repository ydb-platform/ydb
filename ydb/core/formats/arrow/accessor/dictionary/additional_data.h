#pragma once

#include <ydb/core/formats/arrow/accessor/common/additional_data.h>

#include <library/cpp/json/writer/json_value.h>
#include <util/system/types.h>

namespace NKikimr::NArrow::NAccessor {

// Dictionary accessor metadata (dictionary blob + positions blob layout).
// Only the dictionary accessor and storage layer (e.g. columnshard) need to know this type.
struct TDictionaryAccessorData : IAdditionalAccessorData {
    ui32 DictionaryBlobSize = 0;
    ui32 PositionsBlobSize = 0;

    TDictionaryAccessorData() = default;
    TDictionaryAccessorData(ui32 dictionaryBlobSize, ui32 positionsBlobSize)
        : DictionaryBlobSize(dictionaryBlobSize)
        , PositionsBlobSize(positionsBlobSize) {
    }

    void AddToProto(NKikimrTxColumnShard::TIndexColumnMeta* meta) const override;

    // For ChunkDetails in .sys: uses only meta (no blob read). Returns JSON with dictionary_blob_size, positions_blob_size.
    NJson::TJsonValue DebugJson() const;
};

}   // namespace NKikimr::NArrow::NAccessor
