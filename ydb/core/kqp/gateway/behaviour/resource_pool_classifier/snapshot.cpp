#include "snapshot.h"


namespace NKikimr::NKqp {

bool TResourcePoolClassifierSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) {
    Y_ABORT_UNLESS(rawData.result_sets().size() == 1);
    ParseSnapshotObjects<TResourcePoolClassifierConfig>(rawData.result_sets()[0], [this](TResourcePoolClassifierConfig&& config) {
        ResourcePoolClassifierConfigs[config.GetDatabase()].emplace(config.GetName(), config);
    });
    return true;
}

TString TResourcePoolClassifierSnapshot::DoSerializeToString() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonResourcePoolClassifiers = result.InsertValue("resource_pool_classifiers", NJson::JSON_ARRAY);
    for (const auto& [_, configsMap] : ResourcePoolClassifierConfigs) {
        for (const auto& [_, config] : configsMap) {
            jsonResourcePoolClassifiers.AppendValue(config.GetDebugJson());
        }
    }
    return result.GetStringRobust();
}

std::optional<TResourcePoolClassifierConfig> TResourcePoolClassifierSnapshot::GetClassifierConfig(const TString& database, const TString& name) const {
    const auto databaseIt = ResourcePoolClassifierConfigs.find(database);
    if (databaseIt == ResourcePoolClassifierConfigs.end()) {
        return std::nullopt;
    }
    const auto configIt = databaseIt->second.find(name);
    if (configIt == databaseIt->second.end()) {
        return std::nullopt;
    }
    return configIt->second;
}

}  // namespace NKikimr::NKqp
