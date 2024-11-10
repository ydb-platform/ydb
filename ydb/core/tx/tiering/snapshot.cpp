#include "snapshot.h"

#include <ydb/core/base/path.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

bool TTiersSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_ABORT_UNLESS(rawDataResult.result_sets().size() == 1);
    ParseSnapshotObjects<TTierConfig>(rawDataResult.result_sets()[0], [this](TTierConfig&& s) {TierConfigs.emplace(s.GetTierName(), s); });
    return true;
}

std::optional<TTierConfig> TTiersSnapshot::GetTierById(const TString& tierName) const {
    auto it = TierConfigs.find(tierName);
    if (it == TierConfigs.end()) {
        return {};
    } else {
        return it->second;
    }
}

TString NTiers::TTiersSnapshot::DoSerializeToString() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonTiers = result.InsertValue("tiers", NJson::JSON_MAP);
    for (auto&& i : TierConfigs) {
        jsonTiers.InsertValue(i.first, i.second.GetDebugJson());
    }
    return result.GetStringRobust();
}

}
