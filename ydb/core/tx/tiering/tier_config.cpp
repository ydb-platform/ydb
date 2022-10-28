#include "tier_config.h"
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

namespace NKikimr::NColumnShard::NTiers {

NJson::TJsonValue TTierConfig::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::OwnerPath, OwnerPath);
    result.InsertValue(TDecoder::TierName, TierName);
    NProtobufJson::Proto2Json(ProtoConfig, result.InsertValue(TDecoder::TierConfig, NJson::JSON_MAP));
    return result;
}

bool TTierConfig::IsSame(const TTierConfig& item) const {
    return TierName == item.TierName && ProtoConfig.SerializeAsString() == item.ProtoConfig.SerializeAsString();
}

bool TTierConfig::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r) {
    if (!decoder.Read(decoder.GetOwnerPathIdx(), OwnerPath, r)) {
        return false;
    }
    if (!decoder.Read(decoder.GetTierNameIdx(), TierName, r)) {
        return false;
    }
    if (!decoder.ReadDebugProto(decoder.GetTierConfigIdx(), ProtoConfig, r)) {
        return false;
    }
    return true;
}

}
