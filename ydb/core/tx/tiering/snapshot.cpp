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
            TierConfigs.emplace(config.GetGlobalTierId(), config);
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
        std::sort(rulesLocal.begin(), rulesLocal.end());
        for (auto&& i : rulesLocal) {
            TableTierings[i.GetTablePath()].AddRule(std::move(i));
        }
    }
    return true;
}

std::optional<TTierConfig> TConfigsSnapshot::GetValue(const TGlobalTierId& key) const {
    auto it = TierConfigs.find(key);
    if (it == TierConfigs.end()) {
        return {};
    } else {
        return it->second;
    }
}

void TConfigsSnapshot::RemapTablePathToId(const TString& path, const ui64 pathId) {
    auto it = TableTierings.find(path);
    Y_VERIFY(it != TableTierings.end());
    it->second.SetTablePathId(pathId);
}

const TTableTiering* TConfigsSnapshot::GetTableTiering(const TString& tablePath) const {
    auto it = TableTierings.find(tablePath);
    if (it == TableTierings.end()) {
        return nullptr;
    } else {
        return &it->second;
    }
}

std::vector<NKikimr::NColumnShard::NTiers::TTierConfig> TConfigsSnapshot::GetTiersForPathId(const ui64 pathId) const {
    std::vector<TTierConfig> result;
    std::set<TGlobalTierId> readyIds;
    for (auto&& i : TableTierings) {
        for (auto&& r : i.second.GetRules()) {
            if (r.GetTablePathId() == pathId) {
                auto it = TierConfigs.find(r.GetGlobalTierId());
                if (it == TierConfigs.end()) {
                    ALS_ERROR(NKikimrServices::TX_TIERING) << "inconsistency tiering for " << r.GetGlobalTierId().ToString();
                    continue;
                } else if (readyIds.emplace(r.GetGlobalTierId()).second) {
                    result.emplace_back(it->second);
                }
            }
        }
    }
    return result;
}

TString NTiers::TConfigsSnapshot::DoSerializeToString() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonTiers = result.InsertValue("tiers", NJson::JSON_MAP);
    for (auto&& i : TierConfigs) {
        jsonTiers.InsertValue(i.first.ToString(), i.second.GetDebugJson());
    }
    auto& jsonTiering = result.InsertValue("rules", NJson::JSON_MAP);
    for (auto&& i : TableTierings) {
        jsonTiering.InsertValue(i.first, i.second.GetDebugJson());
    }
    return result.GetStringRobust();
}

}
