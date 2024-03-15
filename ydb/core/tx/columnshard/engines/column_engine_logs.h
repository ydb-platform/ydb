#pragma once

#include "defs.h"
#include "column_engine.h"
#include <ydb/core/tx/columnshard/common/scalars.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/columnshard_ttl.h>
#include "scheme/tier_info.h"
#include "storage/granule.h"
#include "storage/storage.h"
#include "changes/indexation.h"
#include "changes/ttl.h"
#include "changes/with_appended.h"

namespace NKikimr::NArrow {
struct TSortDescription;
}

namespace NKikimr::NOlap {

struct TReadMetadata;
class TGranulesTable;
class TColumnsTable;
class TCountersTable;

class TEvictionsController {
public:
    class TTieringWithPathId {
    private:
        const ui64 PathId;
        TTiering TieringInfo;
    public:
        TTieringWithPathId(const ui64 pathId, TTiering&& tieringInfo)
            : PathId(pathId)
            , TieringInfo(std::move(tieringInfo))
        {

        }

        ui64 GetPathId() const {
            return PathId;
        }

        const TTiering& GetTieringInfo() const {
            return TieringInfo;
        }
    };
private:
    THashMap<ui64, TTiering> OriginalTierings;
    std::map<TMonotonic, std::vector<TTieringWithPathId>> NextCheckInstantForTierings;

    std::map<TMonotonic, std::vector<TTieringWithPathId>> BuildNextInstantCheckers(THashMap<ui64, TTiering>&& info) {
        std::map<TMonotonic, std::vector<TTieringWithPathId>> result;
        std::vector<TTieringWithPathId> newTasks;
        for (auto&& i : info) {
            newTasks.emplace_back(i.first, std::move(i.second));
        }
        result.emplace(TMonotonic::Zero(), std::move(newTasks));
        return result;
    }
public:
    std::map<TMonotonic, std::vector<TTieringWithPathId>>& MutableNextCheckInstantForTierings() {
        return NextCheckInstantForTierings;
    }

    void RefreshTierings(std::optional<THashMap<ui64, TTiering>>&& tierings, const NColumnShard::TTtl& ttl);
};


/// Engine with 2 tables:
/// - Granules: PK -> granules (use part of PK)
/// - Columns: granule -> blobs
///
/// @note One instance per tablet.
class TColumnEngineForLogs : public IColumnEngine {
    friend class TCompactColumnEngineChanges;
    friend class TTTLColumnEngineChanges;
    friend class TChangesWithAppend;
    friend class TCompactColumnEngineChanges;
    friend class TCleanupColumnEngineChanges;
private:
    const NColumnShard::TEngineLogsCounters SignalCounters;
    std::shared_ptr<TGranulesStorage> GranulesStorage;
    std::shared_ptr<IStoragesManager> StoragesManager;
    TEvictionsController EvictionsController;
    class TTieringProcessContext {
    private:
        const ui64 MemoryUsageLimit;
        ui64 MemoryUsage = 0;
        std::shared_ptr<TTTLColumnEngineChanges::TMemoryPredictorSimplePolicy> MemoryPredictor;
    public:
        bool AllowEviction = true;
        bool AllowDrop = true;
        const TInstant Now;
        std::shared_ptr<TTTLColumnEngineChanges> Changes;
        std::map<ui64, TDuration> DurationsForced;
        const THashSet<TPortionAddress>& BusyPortions;

        void AppPortionForCheckMemoryUsage(const TPortionInfo& info) {
            MemoryUsage = MemoryPredictor->AddPortion(info);
        }

        bool HasMemoryForEviction() const {
            return MemoryUsage < MemoryUsageLimit;
        }

        TTieringProcessContext(const ui64 memoryUsageLimit, std::shared_ptr<TTTLColumnEngineChanges> changes,
            const THashSet<TPortionAddress>& busyPortions);
    };

    TDuration ProcessTiering(const ui64 pathId, const TTiering& tiering, TTieringProcessContext& context) const;
    bool DrainEvictionQueue(std::map<TMonotonic, std::vector<TEvictionsController::TTieringWithPathId>>& evictionsQueue, TTieringProcessContext& context) const;
public:
    enum ETableIdx {
        GRANULES = 0,
    };

    enum ECounterIds {
        LAST_PORTION = 1,
        LAST_GRANULE,
        LAST_PLAN_STEP,
        LAST_TX_ID,
    };

    enum class EStatsUpdateType {
        DEFAULT = 0,
        ERASE,
        ADD,
    };

    TColumnEngineForLogs(ui64 tabletId, const TCompactionLimits& limits, const std::shared_ptr<IStoragesManager>& storagesManager);

    virtual void OnTieringModified(std::shared_ptr<NColumnShard::TTiersManager> manager, const NColumnShard::TTtl& ttl) override;

    const TVersionedIndex& GetVersionedIndex() const override {
        return VersionedIndex;
    }

    TString SerializeMark(const NArrow::TReplaceKey& key) const override {
        return TMark::SerializeComposite(key, MarkSchema());
    }

    NArrow::TReplaceKey DeserializeMark(const TString& key, std::optional<ui32> markNumKeys) const override {
        if (markNumKeys) {
            Y_ABORT_UNLESS(*markNumKeys == (ui32)MarkSchema()->num_fields());
            return TMark::DeserializeComposite(key, MarkSchema());
        } else {
            NArrow::TReplaceKey markKey = TMark::DeserializeScalar(key, MarkSchema());
            return TMark::ExtendBorder(markKey, MarkSchema());
            return markKey;
        }
    }

    const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const override;
    const TColumnEngineStats& GetTotalStats() override;
    ui64 MemoryUsage() const override;
    TSnapshot LastUpdate() const override { return LastSnapshot; }

    virtual void DoRegisterTable(const ui64 pathId) override;
public:
    bool Load(IDbWrapper& db) override;

    std::shared_ptr<TInsertColumnEngineChanges> StartInsert(std::vector<TInsertedData>&& dataToIndex) noexcept override;
    std::shared_ptr<TColumnEngineChanges> StartCompaction(const TCompactionLimits& limits, const THashSet<TPortionAddress>& busyPortions) noexcept override;
    std::shared_ptr<TCleanupColumnEngineChanges> StartCleanup(const TSnapshot& snapshot, THashSet<ui64>& pathsToDrop, ui32 maxRecords) noexcept override;
    std::shared_ptr<TTTLColumnEngineChanges> StartTtl(const THashMap<ui64, TTiering>& pathEviction,
        const THashSet<TPortionAddress>& busyPortions, const ui64 memoryUsageLimit) noexcept override;

    bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges,
                      const TSnapshot& snapshot) noexcept override;

    void RegisterSchemaVersion(const TSnapshot& snapshot, TIndexInfo&& info) override;

    std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                        const TPKRangesFilter& pkRangesFilter) const override;

    bool IsPortionExists(const ui64 pathId, const ui64 portionId) const {
        auto it = Tables.find(pathId);
        if (it == Tables.end()) {
            return false;
        }
        return !!it->second->GetPortionPtr(portionId);
    }

    virtual bool HasDataInPathId(const ui64 pathId) const override {
        auto g = GetGranuleOptional(pathId);
        if (!g) {
            return false;
        }
        if (g->GetPortions().size()) {
            return false;
        }
        return true;
    }

    bool IsGranuleExists(const ui64 pathId) const {
        return !!GetGranuleOptional(pathId);
    }

    const TGranuleMeta& GetGranuleVerified(const ui64 pathId) const {
        auto it = Tables.find(pathId);
        AFL_VERIFY(it != Tables.end())("path_id", pathId)("count", Tables.size());
        return *it->second;
    }

    std::shared_ptr<TGranuleMeta> GetGranulePtrVerified(const ui64 pathId) const {
        auto result = GetGranuleOptional(pathId);
        AFL_VERIFY(result)("path_id", pathId);
        return result;
    }

    std::shared_ptr<TGranuleMeta> GetGranuleOptional(const ui64 pathId) const {
        auto it = Tables.find(pathId);
        if (it == Tables.end()) {
            return nullptr;
        }
        return it->second;
    }

    ui64 GetTabletId() const {
        return TabletId;
    }

private:
    using TMarksMap = std::map<TMark, ui64, TMark::TCompare>;

    TVersionedIndex VersionedIndex;
    ui64 TabletId;
    std::shared_ptr<TColumnsTable> ColumnsTable;
    std::shared_ptr<TCountersTable> CountersTable;
    THashMap<ui64, std::shared_ptr<TGranuleMeta>> Tables; // pathId into Granule that equal to Table
    TMap<ui64, std::shared_ptr<TColumnEngineStats>> PathStats; // per path_id stats sorted by path_id
    std::map<TSnapshot, std::vector<TPortionInfo>> CleanupPortions;
    TColumnEngineStats Counters;
    ui64 LastPortion;
    ui64 LastGranule;
    TSnapshot LastSnapshot = TSnapshot::Zero();
    mutable std::optional<TMark> CachedDefaultMark;
    bool Loaded = false;
private:
    const std::shared_ptr<arrow::Schema>& MarkSchema() const noexcept {
        return VersionedIndex.GetIndexKey();
    }

    const TMark& DefaultMark() const {
        if (!CachedDefaultMark) {
            CachedDefaultMark = TMark(TMark::MinBorder(MarkSchema()));
        }
        return *CachedDefaultMark;
    }

    bool LoadColumns(IDbWrapper& db);
    bool LoadCounters(IDbWrapper& db);

    void EraseTable(const ui64 pathId);

    void UpsertPortion(const TPortionInfo& portionInfo, const TPortionInfo* exInfo = nullptr);
    bool ErasePortion(const TPortionInfo& portionInfo, bool updateStats = true);
    void UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType = EStatsUpdateType::DEFAULT,
                            const TPortionInfo* exPortionInfo = nullptr);
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                            EStatsUpdateType updateType,
                            const TPortionInfo* exPortionInfo = nullptr) const;

    /// Return lists of adjacent empty granules for the path.
    std::vector<std::vector<std::pair<TMark, ui64>>> EmptyGranuleTracks(const ui64 pathId) const;
};

} // namespace NKikimr::NOlap
