#include "additional_data.h"

#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>

namespace NKikimr::NArrow::NAccessor {

NJson::TJsonValue TDictionaryAccessorData::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("dictionary_blob_size", DictionaryBlobSize);
    result.InsertValue("positions_blob_size", PositionsBlobSize);
    return result;
}

void TDictionaryAccessorData::AddToProto(NKikimrTxColumnShard::TIndexColumnMeta* meta) const {
    if (!meta) {
        return;
    }
    auto* add = meta->MutableAdditionalAccessorData();
    auto* acc = add->MutableDictionaryAccessorData();
    acc->SetDictionaryBlobSize(DictionaryBlobSize);
    acc->SetPositionsBlobSize(PositionsBlobSize);
}

}   // namespace NKikimr::NArrow::NAccessor
