// TODO: remove file
#include "snapshot.h"

namespace NKikimr::NColumnShard::NTiers {

bool TSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_ABORT_UNLESS(rawDataResult.result_sets().size() == 2);
    ParseSnapshotObjects<TTierConfig>(rawDataResult.result_sets()[0], [this](TTierConfig&& s) {
        Tiers.emplace(s, s);
    });
    return true;
}

TString TSnapshot::DebugString() const {
    NJson::TJsonValue result;
    for (const auto& [id, config] : Tiers) {
        result.AppendValue(config.GetDebugJson());
    }
    return result.GetStringRobust();
}

}   // namespace NKikimr::NColumnShard::NTiers
