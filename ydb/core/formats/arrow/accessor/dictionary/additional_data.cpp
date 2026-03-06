#include "additional_data.h"

#include <ydb/core/tx/columnshard/engines/protos/portion_info.pb.h>

namespace NKikimr::NArrow::NAccessor {

NJson::TJsonValue TDictionaryAccessorData::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("variants_blob_size", VariantsBlobSize);
    result.InsertValue("records_blob_size", RecordsBlobSize);
    return result;
}

void TDictionaryAccessorData::AddToProto(NKikimrTxColumnShard::TIndexColumnMeta* meta) const {
    if (!meta) {
        return;
    }
    auto* add = meta->MutableAdditionalAccessorData();
    auto* acc = add->MutableDictionaryAccessorData();
    acc->SetVariantsBlobSize(VariantsBlobSize);
    acc->SetRecordsBlobSize(RecordsBlobSize);
}

}   // namespace NKikimr::NArrow::NAccessor
