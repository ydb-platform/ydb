#include "additional_data.h"

namespace NKikimr::NArrow::NAccessor {

NJson::TJsonValue TDictionaryAccessorData::DebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue("dictionary_blob_size", DictionaryBlobSize);
    result.InsertValue("positions_blob_size", PositionsBlobSize);
    return result;
}

std::optional<NKikimrArrowAccessorProto::TAdditionalAccessorData> TDictionaryAccessorData::SerializeToProto() const {
    NKikimrArrowAccessorProto::TAdditionalAccessorData result;
    auto* acc = result.MutableDictionaryAccessorData();
    acc->SetDictionaryBlobSize(DictionaryBlobSize);
    acc->SetPositionsBlobSize(PositionsBlobSize);
    return result;
}

std::shared_ptr<IAdditionalAccessorData> BuildAdditionalAccessorData(const NKikimrArrowAccessorProto::TAdditionalAccessorData& proto) {
    if (proto.HasDictionaryAccessorData()) {
        const auto& acc = proto.GetDictionaryAccessorData();
        return std::make_shared<TDictionaryAccessorData>(acc.GetDictionaryBlobSize(), acc.GetPositionsBlobSize());
    }
    return std::make_shared<TEmptyAdditionalData>();
}

}   // namespace NKikimr::NArrow::NAccessor
