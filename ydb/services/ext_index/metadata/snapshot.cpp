#include "snapshot.h"

namespace NKikimr::NMetadata::NCSIndex {

bool TSnapshot::DoDeserializeFromResultSet(const Ydb::Table::ExecuteQueryResult& rawDataResult) {
    Y_ABORT_UNLESS(rawDataResult.result_sets().size() == 1);
    ParseSnapshotObjects<TObject>(rawDataResult.result_sets()[0], [this](TObject&& s) {
        const TString tablePath = s.GetTablePath();
        Objects[tablePath].emplace_back(std::move(s));
        });
    return true;
}

TString TSnapshot::DoSerializeToString() const {
    TStringBuilder sb;
    sb << "OBJECTS:";
    for (auto&& i : Objects) {
        for (auto&& object : i.second) {
            sb << object.DebugString() << ";";
        }
    }
    return sb;
}

void TSnapshot::GetObjectsForActivity(std::vector<TObject>& activation, std::vector<TObject>& remove) const {
    for (auto&& i : Objects) {
        for (auto&& object : i.second) {
            if (!object.IsActive() && !object.IsDelete()) {
                activation.emplace_back(object);
            }
            if (object.IsDelete()) {
                remove.emplace_back(object);
            }
        }
    }
}

std::vector<NKikimr::NMetadata::NCSIndex::TObject> TSnapshot::GetIndexes(const TString& tablePath) const {
    auto it = Objects.find(TFsPath(tablePath).Fix().GetPath());
    if (it == Objects.end()) {
        return {};
    }
    return it->second;
}

}
