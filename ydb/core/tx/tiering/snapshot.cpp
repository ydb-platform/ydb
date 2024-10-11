#include "snapshot.h"

#include <ydb/core/base/path.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

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
}
