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
#include "changes/compaction.h"
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
    std::map<TMonotonic, std::vector<TTieringWithPathId>> NextCheckInstantForTTL;
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
    TEvictionsController EvictionsController;
    class TTieringProcessContext {
    public:
        bool AllowEviction = true;
        bool AllowDrop = true;
        const TInstant Now;
        const ui64 MaxEvictBytes;
        std::shared_ptr<TTTLColumnEngineChanges> Changes;
        std::map<ui64, TDuration> DurationsForced;
        const THashSet<ui64>& BusyGranules;
        TTieringProcessContext(const ui64 maxEvictBytes, std::shared_ptr<TTTLColumnEngineChanges> changes, const THashSet<ui64>& busyGranules);
    };

    TDuration ProcessTiering(const ui64 pathId, const TTiering& tiering, TTieringProcessContext& context) const;
    bool DrainEvictionQueue(std::map<TMonotonic, std::vector<TEvictionsController::TTieringWithPathId>>& evictionsQueue, TTieringProcessContext& context) const;
public:
    class TChangesConstructor : public TColumnEngineChanges {
    public:
        static std::shared_ptr<TInsertColumnEngineChanges> BuildInsertChanges(const TMark& defaultMark,
                 std::vector<NOlap::TInsertedData>&& blobsToIndex, const TSnapshot& initSnapshot);

        static std::shared_ptr<TCompactColumnEngineChanges> BuildCompactionChanges(std::unique_ptr<TCompactionInfo>&& info,
                 const TCompactionLimits& limits, const TSnapshot& initSnapshot, const TCompactionSrcGranule& srcGranules);

        static std::shared_ptr<TCleanupColumnEngineChanges> BuildCleanupChanges(const TSnapshot& initSnapshot);

        static std::shared_ptr<TTTLColumnEngineChanges> BuildTtlChanges();
    };

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

    TColumnEngineForLogs(ui64 tabletId, const TCompactionLimits& limits = {});

    virtual void OnTieringModified(std::shared_ptr<NColumnShard::TTiersManager> manager, const NColumnShard::TTtl& ttl) override;

    const TVersionedIndex& GetVersionedIndex() const override {
        return VersionedIndex;
    }

    const THashSet<ui64>* GetOverloadedGranules(const ui64 pathId) const override {
        return GranulesStorage->GetOverloaded(pathId);
    }

    TString SerializeMark(const NArrow::TReplaceKey& key) const override {
        if (UseCompositeMarks()) {
            return TMark::SerializeComposite(key, MarkSchema());
        } else {
            return TMark::SerializeScalar(key, MarkSchema());
        }
    }

    NArrow::TReplaceKey DeserializeMark(const TString& key, std::optional<ui32> markNumKeys) const override {
        if (markNumKeys) {
            Y_VERIFY(*markNumKeys == (ui32)MarkSchema()->num_fields());
            return TMark::DeserializeComposite(key, MarkSchema());
        } else {
            NArrow::TReplaceKey markKey = TMark::DeserializeScalar(key, MarkSchema());
            if (UseCompositeMarks()) {
                return TMark::ExtendBorder(markKey, MarkSchema());
            }
            return markKey;
        }
    }

    const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const override;
    const TColumnEngineStats& GetTotalStats() override;
    ui64 MemoryUsage() const override;
    TSnapshot LastUpdate() const override { return LastSnapshot; }

public:
    bool Load(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs, const THashSet<ui64>& pathsToDrop = {}) override;

    std::shared_ptr<TInsertColumnEngineChanges> StartInsert(std::vector<TInsertedData>&& dataToIndex) noexcept override;
    std::shared_ptr<TCompactColumnEngineChanges> StartCompaction(std::unique_ptr<TCompactionInfo>&& compactionInfo, const TCompactionLimits& limits) noexcept override;
    std::shared_ptr<TCleanupColumnEngineChanges> StartCleanup(const TSnapshot& snapshot, THashSet<ui64>& pathsToDrop, ui32 maxRecords) noexcept override;
    std::shared_ptr<TTTLColumnEngineChanges> StartTtl(const THashMap<ui64, TTiering>& pathEviction, const THashSet<ui64>& busyGranules,
                                                   ui64 maxEvictBytes = TCompactionLimits::DEFAULT_EVICTION_BYTES) noexcept override;

    bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges,
                      const TSnapshot& snapshot) noexcept override;

    void UpdateDefaultSchema(const TSnapshot& snapshot, TIndexInfo&& info) override;



    std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                        const THashSet<ui32>& columnIds,
                                        const TPKRangesFilter& pkRangesFilter) const override;
    std::unique_ptr<TCompactionInfo> Compact(const TCompactionLimits& limits, const THashSet<ui64>& busyGranuleIds) override;

    bool IsPortionExists(const ui64 granuleId, const ui64 portionId) const {
        auto it = Granules.find(granuleId);
        if (it == Granules.end()) {
            return false;
        }
        return it->second->GetPortionPointer(portionId);
    }

    bool IsGranuleExists(const ui64 granuleId) const {
        return !!GetGranuleOptional(granuleId);
    }

    const TGranuleMeta& GetGranuleVerified(const ui64 granuleId) const {
        auto it = Granules.find(granuleId);
        Y_VERIFY(it != Granules.end());
        return *it->second;
    }

    std::shared_ptr<TGranuleMeta> GetGranulePtrVerified(const ui64 granuleId) const {
        auto result = GetGranuleOptional(granuleId);
        Y_VERIFY(result);
        return result;
    }

    std::shared_ptr<TGranuleMeta> GetGranuleOptional(const ui64 granuleId) const {
        auto it = Granules.find(granuleId);
        if (it == Granules.end()) {
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
    std::shared_ptr<TGranulesTable> GranulesTable;
    std::shared_ptr<TColumnsTable> ColumnsTable;
    std::shared_ptr<TCountersTable> CountersTable;
    THashMap<ui64, std::shared_ptr<TGranuleMeta>> Granules; // granule -> meta
    THashMap<ui64, TMarksMap> PathGranules; // path_id -> {mark, granule}
    TMap<ui64, std::shared_ptr<TColumnEngineStats>> PathStats; // per path_id stats sorted by path_id
    /// Set of empty granules.
    /// Just for providing count of empty granules.
    THashSet<ui64> EmptyGranules;
    TSet<TPortionAddress> CleanupPortions;
    TColumnEngineStats Counters;
    ui64 LastPortion;
    ui64 LastGranule;
    TSnapshot LastSnapshot = TSnapshot::Zero();
    mutable std::optional<TMark> CachedDefaultMark;

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

    bool UseCompositeMarks() const noexcept {
        return VersionedIndex.GetLastSchema()->GetIndexInfo().IsCompositeIndexKey();
    }

    void ClearIndex() {
        Granules.clear();
        PathGranules.clear();
        PathStats.clear();
        EmptyGranules.clear();
        CleanupPortions.clear();
        Counters.Clear();

        LastPortion = 0;
        LastGranule = 0;
        LastSnapshot = TSnapshot::Zero();
    }

    bool LoadGranules(IDbWrapper& db);
    bool LoadColumns(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs);
    bool LoadCounters(IDbWrapper& db);

    void EraseGranule(ui64 pathId, ui64 granule, const TMark& mark);

    /// Insert granule or check if same granule was already inserted.
    void SetGranule(const TGranuleRecord& rec);
    void UpsertPortion(const TPortionInfo& portionInfo, const TPortionInfo* exInfo = nullptr);
    bool ErasePortion(const TPortionInfo& portionInfo, bool updateStats = true);
    void UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType = EStatsUpdateType::DEFAULT,
                            const TPortionInfo* exPortionInfo = nullptr);
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                            EStatsUpdateType updateType,
                            const TPortionInfo* exPortionInfo = nullptr) const;

    bool CanInsert(const TColumnEngineChanges& changes, const TSnapshot& commitSnap) const;

    /// Return lists of adjacent empty granules for the path.
    std::vector<std::vector<std::pair<TMark, ui64>>> EmptyGranuleTracks(const ui64 pathId) const;
};

} // namespace NKikimr::NOlap
