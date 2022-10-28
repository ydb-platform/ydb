#include "common.h"
#include <util/string/join.h>

namespace NKikimr::NMetadataProvider {

TString ISnapshotParser::GetSnapshotId() const {
    if (!SnapshotId) {
        SnapshotId = JoinSeq(",", GetTables());
    }
    return *SnapshotId;
}

ISnapshot::TPtr ISnapshotParser::ParseSnapshot(const Ydb::Table::ExecuteQueryResult& rawData, const TInstant actuality) const {
    ISnapshot::TPtr result = CreateSnapshot(actuality);
    Y_VERIFY(result);
    if (!result->DeserializeFromResultSet(rawData)) {
        return nullptr;
    }
    return result;
}

}
