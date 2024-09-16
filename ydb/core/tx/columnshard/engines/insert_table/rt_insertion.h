#pragma once
#include "inserted.h"
#include "path_info.h"

#include <ydb/core/tx/columnshard/counters/insert_table.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {
class IBlobsDeclareRemovingAction;

class TInsertedDataInstant {
private:
    const TInsertedData* Data;
    const TInstant WriteTime;

public:
    TInsertedDataInstant(const TInsertedData& data)
        : Data(&data)
        , WriteTime(Data->GetMeta().GetDirtyWriteTime())
    {

    }

    const TInsertedData& GetData() const {
        return *Data;
    }
    TInstant GetWriteTime() const {
        return WriteTime;
    }

    bool operator<(const TInsertedDataInstant& item) const {
        if (WriteTime == item.WriteTime) {
            return Data->GetInsertWriteId() < item.Data->GetInsertWriteId();
        } else {
            return WriteTime < item.WriteTime;
        }
    }
};

class TInsertedContainer {
private:
    THashMap<TInsertWriteId, TInsertedData> Inserted;
    std::set<TInsertedDataInstant> InsertedByWriteTime;

public:
    size_t size() const {
        return Inserted.size();
    }

    bool contains(const TInsertWriteId id) const {
        return Inserted.contains(id);
    }

    THashMap<TInsertWriteId, TInsertedData>::const_iterator begin() const {
        return Inserted.begin();
    }

    THashMap<TInsertWriteId, TInsertedData>::const_iterator end() const {
        return Inserted.end();
    }

    THashSet<TInsertWriteId> GetExpired(const TInstant timeBorder, const ui64 limit) const {
        THashSet<TInsertWriteId> result;
        for (auto& data : InsertedByWriteTime) {
            if (timeBorder < data.GetWriteTime()) {
                break;
            }
            if (data.GetData().IsNotAbortable()) {
                continue;
            }
            result.emplace(data.GetData().GetInsertWriteId());
            if (limit <= result.size()) {
                break;
            }
        }
        return result;
    }

    TInsertedData* AddVerified(TInsertedData&& data) {
        const TInsertWriteId writeId = data.GetInsertWriteId();
        auto itInsertion = Inserted.emplace(writeId, std::move(data));
        AFL_VERIFY(itInsertion.second);
        auto* dataPtr = &itInsertion.first->second;
        InsertedByWriteTime.emplace(TInsertedDataInstant(*dataPtr));
        return dataPtr;
    }

    const TInsertedData* GetOptional(const TInsertWriteId id) const {
        auto it = Inserted.find(id);
        if (it == Inserted.end()) {
            return nullptr;
        } else {
            return &it->second;
        }
    }

    TInsertedData* MutableOptional(const TInsertWriteId id) {
        auto it = Inserted.find(id);
        if (it == Inserted.end()) {
            return nullptr;
        } else {
            return &it->second;
        }
    }

    std::optional<TInsertedData> ExtractOptional(const TInsertWriteId id) {
        auto it = Inserted.find(id);
        if (it == Inserted.end()) {
            return std::nullopt;
        }
        AFL_VERIFY(InsertedByWriteTime.erase(TInsertedDataInstant(it->second)));
        TInsertedData result = std::move(it->second);
        Inserted.erase(it);
        return result;
    }
};

class TInsertionSummary {
public:
    struct TCounters {
        ui64 Rows{};
        ui64 Bytes{};
        ui64 RawBytes{};
    };

private:
    friend class TPathInfo;
    TCounters StatsPrepared;
    TCounters StatsCommitted;
    const NColumnShard::TInsertTableCounters Counters;

    TInsertedContainer Inserted;
    THashMap<TInsertWriteId, TInsertedData> Aborted;

    std::map<TPathInfoIndexPriority, std::set<const TPathInfo*>> Priorities;
    THashMap<ui64, TPathInfo> PathInfo;
    void RemovePriority(const TPathInfo& pathInfo) noexcept;
    void AddPriority(const TPathInfo& pathInfo) noexcept;

    void OnNewCommitted(const ui64 dataSize, const bool load = false) noexcept;
    void OnEraseCommitted(TPathInfo& pathInfo, const ui64 dataSize) noexcept;
    void OnNewInserted(TPathInfo& pathInfo, const ui64 dataSize, const bool load) noexcept;
    void OnEraseInserted(TPathInfo& pathInfo, const ui64 dataSize) noexcept;
    static TAtomicCounter CriticalInserted;

public:
    bool HasPathIdData(const ui64 pathId) const {
        auto it = PathInfo.find(pathId);
        if (it == PathInfo.end()) {
            return false;
        }
        return !it->second.IsEmpty();
    }

    void ErasePath(const ui64 pathId) {
        auto it = PathInfo.find(pathId);
        if (it == PathInfo.end()) {
            return;
        }
        RemovePriority(it->second);
        AFL_VERIFY(it->second.IsEmpty());
        PathInfo.erase(it);
    }

    void MarkAsNotAbortable(const TInsertWriteId writeId) {
        auto* data = Inserted.MutableOptional(writeId);
        if (!data) {
            return;
        }
        data->MarkAsNotAbortable();
    }

    THashSet<TInsertWriteId> GetExpiredInsertions(const TInstant timeBorder, const ui64 limit) const;

    const TInsertedContainer& GetInserted() const {
        return Inserted;
    }
    const THashMap<TInsertWriteId, TInsertedData>& GetAborted() const {
        return Aborted;
    }

    const TInsertedData* AddAborted(TInsertedData&& data, const bool load = false);
    bool EraseAborted(const TInsertWriteId writeId);
    bool HasAborted(const TInsertWriteId writeId);

    bool EraseCommitted(const TCommittedData& data);
    bool HasCommitted(const TCommittedData& data);

    const TInsertedData* AddInserted(TInsertedData&& data, const bool load = false);
    std::optional<TInsertedData> ExtractInserted(const TInsertWriteId id);

    const TCounters& GetCountersPrepared() const {
        return StatsPrepared;
    }
    const TCounters& GetCountersCommitted() const {
        return StatsCommitted;
    }
    const NColumnShard::TInsertTableCounters& GetCounters() const {
        return Counters;
    }
    NKikimr::NOlap::TPathInfo& GetPathInfo(const ui64 pathId);
    TPathInfo* GetPathInfoOptional(const ui64 pathId);
    const TPathInfo* GetPathInfoOptional(const ui64 pathId) const;

    const THashMap<ui64, TPathInfo>& GetPathInfo() const {
        return PathInfo;
    }

    bool IsOverloaded(const ui64 pathId) const;

    const std::map<TPathInfoIndexPriority, std::set<const TPathInfo*>>& GetPathPriorities() const {
        return Priorities;
    }
};

}   // namespace NKikimr::NOlap
