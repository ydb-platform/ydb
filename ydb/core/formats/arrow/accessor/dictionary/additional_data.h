#pragma once

#include <ydb/core/formats/arrow/accessor/common/additional_data.h>

#include <library/cpp/json/writer/json_value.h>
#include <util/system/types.h>

namespace NKikimr::NArrow::NAccessor {

struct TDictionaryAccessorData : IAdditionalAccessorData {
    ui32 DictionaryBlobSize = 0;
    ui32 PositionsBlobSize = 0;

    TDictionaryAccessorData() = default;
    TDictionaryAccessorData(ui32 dictionaryBlobSize, ui32 positionsBlobSize)
        : DictionaryBlobSize(dictionaryBlobSize)
        , PositionsBlobSize(positionsBlobSize) {
    }

    bool HasDataToSerialize() const override {
        return true;
    };

    void AddToProto(NKikimrTxColumnShard::TIndexColumnMeta& meta) const override;

    NJson::TJsonValue DebugJson() const override;
};

}   // namespace NKikimr::NArrow::NAccessor
