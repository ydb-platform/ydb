#include "path_info.h"
#include "rt_insertion.h"
#include <ydb/core/tx/columnshard/engines/column_engine.h>

namespace NKikimr::NOlap {

bool TPathInfo::SetCommittedOverload(const bool value) {
    const bool startOverloaded = IsOverloaded();
    CommittedOverload = value;
    return startOverloaded != IsOverloaded();
}

bool TPathInfo::SetInsertedOverload(const bool value) {
    const bool startOverloaded = IsOverloaded();
    InsertedOverload = value;
    return startOverloaded != IsOverloaded();
}

void TPathInfo::AddCommittedSize(const i64 size, const ui64 overloadLimit) {
    CommittedSize += size;
    Y_VERIFY(CommittedSize >= 0);
    Summary->CommittedSize += size;
    Y_VERIFY(Summary->CommittedSize >= 0);
    SetCommittedOverload((ui64)CommittedSize > overloadLimit);
}

void TPathInfo::AddInsertedSize(const i64 size, const ui64 overloadLimit) {
    InsertedSize += size;
    Y_VERIFY(InsertedSize >= 0);
    Summary->InsertedSize += size;
    Y_VERIFY(Summary->InsertedSize >= 0);
    PathIdCounters.Committed.OnPathIdDataInfo(InsertedSize, 0);
    SetInsertedOverload((ui64)InsertedSize > overloadLimit);
}

bool TPathInfo::EraseCommitted(const TInsertedData& data) {
    Summary->RemovePriority(*this);
    const bool result = Committed.erase(data);
    AddCommittedSize(-1 * (i64)data.BlobSize(), TCompactionLimits::OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID);
    Summary->AddPriority(*this);
    PathIdCounters.Committed.OnPathIdDataInfo(CommittedSize, Committed.size());
    return result;
}

bool TPathInfo::AddCommitted(TInsertedData&& data) {
    Summary->RemovePriority(*this);
    AddCommittedSize(data.BlobSize(), TCompactionLimits::OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID);
    bool result = Committed.emplace(std::move(data)).second;
    Summary->AddPriority(*this);
    PathIdCounters.Committed.OnPathIdDataInfo(CommittedSize, Committed.size());
    return result;
}

TPathInfo::TPathInfo(TInsertionSummary& summary, const ui64 pathId)
    : PathId(pathId)
    , Summary(&summary)
    , PathIdCounters(Summary->GetCounters().GetPathIdCounters())
{

}

}
