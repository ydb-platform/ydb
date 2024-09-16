#include "rt_insertion.h"
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/remove.h>

namespace NKikimr::NOlap {

void TInsertionSummary::OnNewCommitted(const ui64 dataSize, const bool load) noexcept {
    Counters.Committed.Add(dataSize, load);
    ++StatsCommitted.Rows;
    StatsCommitted.Bytes += dataSize;
    Y_ABORT_UNLESS(Counters.Committed.GetDataSize() == (i64)StatsCommitted.Bytes);
}

void TInsertionSummary::OnEraseCommitted(TPathInfo& /*pathInfo*/, const ui64 dataSize) noexcept {
    Counters.Committed.Erase(dataSize);
    Y_ABORT_UNLESS(--StatsCommitted.Rows >= 0);
    Y_ABORT_UNLESS(StatsCommitted.Bytes >= dataSize);
    StatsCommitted.Bytes -= dataSize;
    Y_ABORT_UNLESS(Counters.Committed.GetDataSize() == (i64)StatsCommitted.Bytes);
}

void TInsertionSummary::RemovePriority(const TPathInfo& pathInfo) noexcept {
    const auto priority = pathInfo.GetIndexationPriority();
    auto it = Priorities.find(priority);
    if (it == Priorities.end()) {
        AFL_VERIFY(!priority);
        return;
    }
    AFL_VERIFY(!!priority);
    Y_ABORT_UNLESS(it->second.erase(&pathInfo) || !priority);
    if (it->second.empty()) {
        Priorities.erase(it);
    }
}

void TInsertionSummary::AddPriority(const TPathInfo& pathInfo) noexcept {
    if (!!pathInfo.GetIndexationPriority()) {
        Y_ABORT_UNLESS(Priorities[pathInfo.GetIndexationPriority()].emplace(&pathInfo).second);
    }
}

NKikimr::NOlap::TPathInfo& TInsertionSummary::GetPathInfo(const ui64 pathId) {
    auto it = PathInfo.find(pathId);
    if (it == PathInfo.end()) {
        it = PathInfo.emplace(pathId, TPathInfo(*this, pathId)).first;
    }
    return it->second;
}

NKikimr::NOlap::TPathInfo* TInsertionSummary::GetPathInfoOptional(const ui64 pathId) {
    auto it = PathInfo.find(pathId);
    if (it == PathInfo.end()) {
        return nullptr;
    }
    return &it->second;
}

const NKikimr::NOlap::TPathInfo* TInsertionSummary::GetPathInfoOptional(const ui64 pathId) const {
    auto it = PathInfo.find(pathId);
    if (it == PathInfo.end()) {
        return nullptr;
    }
    return &it->second;
}

bool TInsertionSummary::IsOverloaded(const ui64 pathId) const {
    auto* pathInfo = GetPathInfoOptional(pathId);
    if (!pathInfo) {
        return false;
    } else {
        return (ui64)pathInfo->GetCommittedSize() > TCompactionLimits::OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID;
    }
}

void TInsertionSummary::OnNewInserted(TPathInfo& pathInfo, const ui64 dataSize, const bool load) noexcept {
    Counters.Inserted.Add(dataSize, load);
    pathInfo.AddInsertedSize(dataSize, TCompactionLimits::OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID);
    ++StatsPrepared.Rows;
    StatsPrepared.Bytes += dataSize;
    AFL_VERIFY(Counters.Inserted.GetDataSize() == (i64)StatsPrepared.Bytes);
}

void TInsertionSummary::OnEraseInserted(TPathInfo& pathInfo, const ui64 dataSize) noexcept {
    Counters.Inserted.Erase(dataSize);
    pathInfo.AddInsertedSize(-1 * (i64)dataSize, TCompactionLimits::OVERLOAD_INSERT_TABLE_SIZE_BY_PATH_ID);
    Y_ABORT_UNLESS(--StatsPrepared.Rows >= 0);
    Y_ABORT_UNLESS(StatsPrepared.Bytes >= dataSize);
    StatsPrepared.Bytes -= dataSize;
    AFL_VERIFY(Counters.Inserted.GetDataSize() == (i64)StatsPrepared.Bytes);
}

THashSet<TInsertWriteId> TInsertionSummary::GetInsertedByPathId(const ui64 pathId) const {
    THashSet<TInsertWriteId> result;
    for (auto& [writeId, data] : Inserted) {
        if (data.GetPathId() == pathId) {
            result.insert(writeId);
        }
    }

    return result;
}

THashSet<TInsertWriteId> TInsertionSummary::GetExpiredInsertions(const TInstant timeBorder, const ui64 limit) const {
    if (timeBorder < MinInsertedTs) {
        return {};
    }

    THashSet<TInsertWriteId> toAbort;
    TInstant newMin = TInstant::Max();
    for (auto& [writeId, data] : Inserted) {
        const TInstant dataInsertTs = data.GetMeta().GetDirtyWriteTime();
        if (data.IsNotAbortable()) {
            continue;
        }
        if (dataInsertTs < timeBorder && toAbort.size() < limit) {
            toAbort.insert(writeId);
        } else {
            newMin = Min(newMin, dataInsertTs);
        }
    }
    MinInsertedTs = (toAbort.size() == Inserted.size()) ? TInstant::Zero() : newMin;
    return toAbort;
}

bool TInsertionSummary::EraseAborted(const TInsertWriteId writeId) {
    auto it = Aborted.find(writeId);
    if (it == Aborted.end()) {
        return false;
    }
    Counters.Aborted.Erase(it->second.BlobSize());
    Aborted.erase(it);
    return true;
}

bool TInsertionSummary::HasAborted(const TInsertWriteId writeId) {
    auto it = Aborted.find(writeId);
    if (it == Aborted.end()) {
        return false;
    }
    return true;
}

bool TInsertionSummary::EraseCommitted(const TCommittedData& data) {
    TPathInfo* pathInfo = GetPathInfoOptional(data.GetPathId());
    if (!pathInfo) {
        Counters.Committed.SkipErase(data.BlobSize());
        return false;
    }

    if (!pathInfo->EraseCommitted(data)) {
        Counters.Committed.SkipErase(data.BlobSize());
        return false;
    } else {
        return true;
    }
}

bool TInsertionSummary::HasCommitted(const TCommittedData& data) {
    TPathInfo* pathInfo = GetPathInfoOptional(data.GetPathId());
    if (!pathInfo) {
        return false;
    }
    return pathInfo->HasCommitted(data);
}

const NKikimr::NOlap::TInsertedData* TInsertionSummary::AddAborted(TInsertedData&& data, const bool load /*= false*/) {
    const TInsertWriteId writeId = data.GetInsertWriteId();
    Counters.Aborted.Add(data.BlobSize(), load);
    AFL_VERIFY_DEBUG(!Inserted.contains(writeId));
    auto insertInfo = Aborted.emplace(writeId, std::move(data));
    AFL_VERIFY(insertInfo.second)("write_id", writeId);
    return &insertInfo.first->second;
}

std::optional<NKikimr::NOlap::TInsertedData> TInsertionSummary::ExtractInserted(const TInsertWriteId id) {
    auto it = Inserted.find(id);
    if (it == Inserted.end()) {
        return {};
    } else {
        auto pathInfo = GetPathInfoOptional(it->second.GetPathId());
        if (pathInfo) {
            OnEraseInserted(*pathInfo, it->second.BlobSize());
        }
        std::optional<TInsertedData> result = std::move(it->second);
        Inserted.erase(it);
        return result;
    }
}

const NKikimr::NOlap::TInsertedData* TInsertionSummary::AddInserted(TInsertedData&& data, const bool load /*= false*/) {
    const TInsertWriteId writeId = data.GetInsertWriteId();
    const ui32 dataSize = data.BlobSize();
    const ui64 pathId = data.GetPathId();
    auto insertInfo = Inserted.emplace(writeId, std::move(data));
    AFL_VERIFY_DEBUG(!Aborted.contains(writeId));
    if (insertInfo.second) {
        OnNewInserted(GetPathInfo(pathId), dataSize, load);
        return &insertInfo.first->second;
    } else {
        Counters.Inserted.SkipAdd(dataSize);
        return nullptr;
    }
}

}
