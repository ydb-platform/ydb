#include "object.h"
#include "behaviour.h"

#include <ydb/core/tx/tiering/rule/checker.h>
#include <ydb/core/tx/tiering/snapshot.h>

#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NColumnShard::NTiers {

NJson::TJsonValue TTieringRule::GetDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    result.InsertValue(TDecoder::TieringRuleId, TieringRuleId);
    result.InsertValue(TDecoder::DefaultColumn, DefaultColumn);
    result.InsertValue(TDecoder::Description, SerializeDescriptionToJson());
    return result;
}

NJson::TJsonValue TTieringRule::SerializeDescriptionToJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonRules = result.InsertValue("rules", NJson::JSON_ARRAY);
    for (auto&& i : Intervals) {
        jsonRules.AppendValue(i.SerializeToJson());
    }
    return result;
}

bool TTieringRule::DeserializeDescriptionFromJson(const NJson::TJsonValue& jsonInfo) {
    const NJson::TJsonValue::TArray* rules;
    if (!jsonInfo["rules"].GetArrayPointer(&rules)) {
        return false;
    }
    if (rules->empty()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "tiering_rule_deserialization_failed")("reason", "empty_rules");
        return false;
    }
    for (auto&& i : *rules) {
        TTieringInterval interval;
        if (!interval.DeserializeFromJson(i)) {
            return false;
        }
        Intervals.emplace_back(std::move(interval));
    }
    std::sort(Intervals.begin(), Intervals.end());
    return true;
}

NMetadata::NInternal::TTableRecord TTieringRule::SerializeToRecord() const {
    NMetadata::NInternal::TTableRecord result;
    result.SetColumn(TDecoder::TieringRuleId, NMetadata::NInternal::TYDBValue::Utf8(TieringRuleId));
    result.SetColumn(TDecoder::DefaultColumn, NMetadata::NInternal::TYDBValue::Utf8(DefaultColumn));
    {
        auto jsonDescription = SerializeDescriptionToJson();
        NJsonWriter::TBuf sout;
        sout.WriteJsonValue(&jsonDescription, true);
        result.SetColumn(TDecoder::Description, NMetadata::NInternal::TYDBValue::Utf8(sout.Str()));
    }
    return result;
}

bool TTieringRule::DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& r) {
    if (!decoder.Read(decoder.GetTieringRuleIdIdx(), TieringRuleId, r)) {
        return false;
    }
    if (!decoder.Read(decoder.GetDefaultColumnIdx(), DefaultColumn, r)) {
        return false;
    }
    if (DefaultColumn.Empty()) {
        return false;
    }
    NJson::TJsonValue jsonDescription;
    if (!decoder.ReadJson(decoder.GetDescriptionIdx(), jsonDescription, r)) {
        return false;
    }
    if (!DeserializeDescriptionFromJson(jsonDescription)) {
        return false;
    }
    return true;
}

NKikimr::NOlap::TTiering TTieringRule::BuildOlapTiers() const {
    AFL_VERIFY(!Intervals.empty());
    NOlap::TTiering result;
    for (auto&& r : Intervals) {
        AFL_VERIFY(result.Add(std::make_shared<NOlap::TTierInfo>(r.GetTierName(), r.GetDurationForEvict(), GetDefaultColumn())));
    }
    return result;
}

bool TTieringRule::ContainsTier(const TString& tierName) const {
    for (auto&& i : Intervals) {
        if (i.GetTierName() == tierName) {
            return true;
        }
    }
    return false;
}

NMetadata::IClassBehaviour::TPtr TTieringRule::GetBehaviour() {
    static std::shared_ptr<TTieringRuleBehaviour> result = std::make_shared<TTieringRuleBehaviour>();
    return result;
}

}
