#include "fetcher.h"
#include <util/string/join.h>

namespace NKikimr::NMetadataProvider {

ISnapshot::TPtr ISnapshotParser::ParseSnapshot(const Ydb::Table::ExecuteQueryResult& rawData, const TInstant actuality) const {
    ISnapshot::TPtr result = CreateSnapshot(actuality);
    Y_VERIFY(result);
    if (!result->DeserializeFromResultSet(rawData)) {
        return nullptr;
    }
    return result;
}

TString ISnapshotParser::GetComponentId() const {
    auto managers = GetManagers();
    std::vector<TString> ids;
    for (auto&& i : managers) {
        ids.emplace_back(i->GetTablePath());
    }
    return JoinSeq("-", ids);
}

}
