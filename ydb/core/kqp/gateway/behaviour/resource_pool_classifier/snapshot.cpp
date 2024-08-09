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

}  // namespace NKikimr::NKqp
