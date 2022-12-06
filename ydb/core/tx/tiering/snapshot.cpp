#include "snapshot.h"

#include <ydb/core/base/path.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

bool TConfigsSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_VERIFY(rawDataResult.result_sets().size() == 2);
    {
        auto& rawData = rawDataResult.result_sets()[0];
        TTierConfig::TDecoder decoder(rawData);
        for (auto&& r : rawData.rows()) {
            TTierConfig config;
            if (!config.DeserializeFromRecord(decoder, r)) {
                ALS_ERROR(NKikimrServices::TX_TIERING) << "cannot parse tier config from snapshot";
                continue;
            }
            TierConfigs.emplace(config.GetTierName(), config);
        }
    }
    {
        auto& rawData = rawDataResult.result_sets()[1];
        TTieringRule::TDecoder decoder(rawData);
        TVector<TTieringRule> rulesLocal;
        rulesLocal.reserve(rawData.rows().size());
        for (auto&& r : rawData.rows()) {
            TTieringRule tr;
            if (!tr.DeserializeFromRecord(decoder, r)) {
                ALS_WARN(NKikimrServices::TX_TIERING) << "cannot parse record for tiering info";
                continue;
            }
            rulesLocal.emplace_back(std::move(tr));
        }
        for (auto&& i : rulesLocal) {
            TableTierings.emplace(i.GetTieringRuleId(), std::move(i));
        }
    }
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
