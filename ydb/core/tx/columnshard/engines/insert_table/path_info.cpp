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
    Y_ABORT_UNLESS(CommittedSize >= 0);
    SetCommittedOverload((ui64)CommittedSize > overloadLimit);
}

void TPathInfo::AddInsertedSize(const i64 size, const ui64 overloadLimit) {
    InsertedSize += size;
    Y_ABORT_UNLESS(InsertedSize >= 0);
    PathIdCounters.Inserted.OnPathIdDataInfo(InsertedSize, 0);
    SetInsertedOverload((ui64)InsertedSize > overloadLimit);
}

bool TPathInfo::EraseCommitted(const TCommittedData& data) {
    Summary->RemovePriority(*this);
    const bool result = Committed.erase(data);
    AddCommittedSize(-1 * (i64)data.BlobSize(), TCompactionLimits::OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID);
    Summary->AddPriority(*this);
    PathIdCounters.Committed.OnPathIdDataInfo(CommittedSize, Committed.size());
    Summary->OnEraseCommitted(*this, data.BlobSize());
    return result;
}

bool TPathInfo::HasCommitted(const TCommittedData& data) {
    return Committed.contains(data);
}

bool TPathInfo::AddCommitted(TCommittedData&& data, const bool load) {
    const ui64 dataSize = data.BlobSize();
    Summary->RemovePriority(*this);
    AddCommittedSize(data.BlobSize(), TCompactionLimits::OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID);
    bool result = Committed.emplace(std::move(data)).second;
    Summary->AddPriority(*this);
    Summary->OnNewCommitted(dataSize, load);
    PathIdCounters.Committed.OnPathIdDataInfo(CommittedSize, Committed.size());
    return result;
}

TPathInfo::TPathInfo(TInsertionSummary& summary, const ui64 pathId)
    : PathId(pathId)
    , Summary(&summary)
    , PathIdCounters(Summary->GetCounters().GetPathIdCounters())
{

}

NKikimr::NOlap::TPathInfoIndexPriority TPathInfo::GetIndexationPriority() const {
    if (CommittedSize > (i64)TCompactionLimits::WARNING_INSERT_TABLE_SIZE_BY_PATH_ID) {
        return TPathInfoIndexPriority(TPathInfoIndexPriority::EIndexationPriority::PreventOverload, CommittedSize);
    } else if (Committed.size() > TCompactionLimits::WARNING_INSERT_TABLE_COUNT_BY_PATH_ID) {
        return TPathInfoIndexPriority(TPathInfoIndexPriority::EIndexationPriority::PreventManyPortions, Committed.size());
    } else {
        return TPathInfoIndexPriority(TPathInfoIndexPriority::EIndexationPriority::NoPriority, CommittedSize * Committed.size() * Committed.size());
    }
}

}
