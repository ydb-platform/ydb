#include "fetcher.h"
#include <util/string/join.h>

namespace NKikimr::NMetadata::NFetcher {

ISnapshot::TPtr ISnapshotsFetcher::ParseSnapshot(const Ydb::Table::ExecuteQueryResult& rawData, const TInstant actuality) const {
    ISnapshot::TPtr result = CreateSnapshot(actuality);
    Y_ABORT_UNLESS(result);
    if (!result->DeserializeFromResultSet(rawData)) {
        return nullptr;
    }
    return result;
}

TString ISnapshotsFetcher::GetComponentId() const {
    auto managers = GetManagers();
    std::vector<TString> ids;
    for (auto&& i : managers) {
        ids.emplace_back(i->GetStorageTablePath());
    }
    return JoinSeq("-", ids);
}

}
