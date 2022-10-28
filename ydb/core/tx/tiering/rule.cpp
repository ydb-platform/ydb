#include "rule.h"

namespace NKikimr::NColumnShard::NTiers {
NJson::TJsonValue TTieringRule::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::OwnerPath, OwnerPath);
    result.InsertValue(TDecoder::TierName, TierName);
    result.InsertValue(TDecoder::TablePath, TablePath);
    result.InsertValue("tablePathId", TablePathId);
    result.InsertValue(TDecoder::Column, Column);
    result.InsertValue(TDecoder::DurationForEvict, DurationForEvict.ToString());
    return result;
}

NJson::TJsonValue TTableTiering::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TTieringRule::TDecoder::TablePath, TablePath);
    result.InsertValue(TTieringRule::TDecoder::Column, Column);
    auto&& jsonRules = result.InsertValue("rules", NJson::JSON_ARRAY);
    for (auto&& i : Rules) {
        jsonRules.AppendValue(i.GetDebugJson());
    }
    return result;
}

}
