#include "snapshot.h"

#include <ydb/core/base/path.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

bool TConfigsSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_ABORT_UNLESS(rawDataResult.result_sets().size() == 2);
    ParseSnapshotObjects<TTierConfig>(rawDataResult.result_sets()[0], [this](TTierConfig&& s) {TierConfigs.emplace(s.GetTierName(), s); });
    ParseSnapshotObjects<TTieringRule>(rawDataResult.result_sets()[1], [this](TTieringRule&& s) {TableTierings.emplace(s.GetTieringRuleId(), s); });
    return true;
}

std::optional<TTierConfig> TConfigsSnapshot::GetTierById(const TString& tierName) const {
    auto it = TierConfigs.find(tierName);
    if (it == TierConfigs.end()) {
        return {};
    } else {
        return it->second;
    }
}

const TTieringRule* TConfigsSnapshot::GetTieringById(const TString& tieringId) const {
    auto it = TableTierings.find(tieringId);
    if (it == TableTierings.end()) {
        return nullptr;
    } else {
        return &it->second;
    }
}

std::set<TString> TConfigsSnapshot::GetTieringIdsForTier(const TString& tierName) const {
    std::set<TString> result;
    for (auto&& i : TableTierings) {
        for (auto&& t : i.second.GetIntervals()) {
            if (t.GetTierName() == tierName) {
                result.emplace(i.second.GetTieringRuleId());
                break;
            }
        }
    }
    return result;
}

TString NTiers::TConfigsSnapshot::DoSerializeToString() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonTiers = result.InsertValue("tiers", NJson::JSON_MAP);
    for (auto&& i : TierConfigs) {
        jsonTiers.InsertValue(i.first, i.second.GetDebugJson());
    }
    auto& jsonTiering = result.InsertValue("rules", NJson::JSON_MAP);
    for (auto&& i : TableTierings) {
        jsonTiering.InsertValue(i.first, i.second.GetDebugJson());
    }
    return result.GetStringRobust();
}

}
