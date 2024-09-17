// TODO: remove file
#include "snapshot.h"

namespace NKikimr::NColumnShard::NTiers {

bool TTieringRulesSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_ABORT_UNLESS(rawDataResult.result_sets().size() == 2);
    ParseSnapshotObjects<TTieringRule>(rawDataResult.result_sets()[0], [this](TTieringRule&& s) {
        Rules.emplace(s, s);
    });
    return true;
}

TString TTieringRulesSnapshot::DebugString() const {
    NJson::TJsonValue result;
    for (const auto& [id, rule] : Rules) {
        result.AppendValue(rule.GetDebugJson());
    }
    return result.GetStringRobust();
}

}   // namespace NKikimr::NColumnShard::NTiers
