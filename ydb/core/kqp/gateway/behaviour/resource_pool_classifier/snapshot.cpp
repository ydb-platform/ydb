#include "snapshot.h"


namespace NKikimr::NKqp {

bool TResourcePoolClassifierSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawData) {
    Y_ABORT_UNLESS(rawData.result_sets().size() == 1);
    ParseSnapshotObjects<TResourcePoolClassifierConfig>(rawData.result_sets()[0], [this](TResourcePoolClassifierConfig&& config) {
        ResourcePoolClassifierConfigs.emplace(config.GetName(), config);
    });
    return true;
}

TString TResourcePoolClassifierSnapshot::DoSerializeToString() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    auto& jsonResourcePoolClassifiers = result.InsertValue("resource_pool_classifiers", NJson::JSON_MAP);
    for (const auto& [name, config] : ResourcePoolClassifierConfigs) {
        jsonResourcePoolClassifiers.InsertValue(name, config.GetDebugJson());
    }
    return result.GetStringRobust();
}

}  // namespace NKikimr::NKqp
