#pragma once

#include "defs.h"
#include "column_engine.h"
#include "scalars.h"

namespace NKikimr::NArrow {
struct TSortDescription;
}

namespace NKikimr::NOlap {

struct TReadMetadata;
class TGranulesTable;
class TColumnsTable;
class TCountersTable;

/// Engine with 2 tables:
/// - Granules: PK -> granules (use part of PK)
/// - Columns: granule -> blobs
///
/// @note One instance per tablet.
class TColumnEngineForLogs : public IColumnEngine {
public:
    class TMarksGranules {
    public:
        using TPair = std::pair<TMark, ui64>;

        TMarksGranules() = default;
        TMarksGranules(std::vector<TPair>&& marks) noexcept;
        TMarksGranules(std::vector<TMark>&& points);
        TMarksGranules(const TSelectInfo& selectInfo);

        const std::vector<TPair>& GetOrderedMarks() const noexcept {
             return Marks;
        }

        bool Empty() const noexcept {
            return Marks.empty();
        }

        bool MakePrecedingMark(const TIndexInfo& indexInfo);

        THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>
        SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch, const TIndexInfo& indexInfo);

    private:
        std::vector<TPair> Marks;
    };

    class TChanges : public TColumnEngineChanges {
    public:
        struct TSrcGranule {
            ui64 PathId{0};
            ui64 Granule{0};
            TMark Mark;

            TSrcGranule(ui64 pathId, ui64 granule, const TMark& mark)
                : PathId(pathId), Granule(granule), Mark(mark)
            {}
        };

        TChanges(const TMark& defaultMark,
                 std::vector<NOlap::TInsertedData>&& blobsToIndex, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::INSERT)
            , DefaultMark(defaultMark)
        {
            Limits = limits;
            DataToIndex = std::move(blobsToIndex);
        }

        TChanges(const TMark& defaultMark,
                 std::unique_ptr<TCompactionInfo>&& info, const TCompactionLimits& limits, const TSnapshot& snapshot)
            : TColumnEngineChanges(TColumnEngineChanges::COMPACTION)
            , DefaultMark(defaultMark)
        {
            Limits = limits;
            CompactionInfo = std::move(info);
            InitSnapshot = snapshot;
        }

        TChanges(const TMark& defaultMark,
                 const TSnapshot& snapshot, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::CLEANUP)
            , DefaultMark(defaultMark)
        {
            Limits = limits;
            InitSnapshot = snapshot;
        }

        TChanges(const TMark& defaultMark,
                 TColumnEngineChanges::EType type, const TSnapshot& applySnapshot)
            : TColumnEngineChanges(type)
            , DefaultMark(defaultMark)
        {
            ApplySnapshot = applySnapshot;
        }

        bool AddPathIfNotExists(ui64 pathId) {
            if (PathToGranule.contains(pathId)) {
                return false;
            }

            Y_VERIFY(FirstGranuleId && ReservedGranuleIds);
            ui64 granule = FirstGranuleId;
            ++FirstGranuleId;
            --ReservedGranuleIds;

            NewGranules.emplace(granule, std::make_pair(pathId, DefaultMark));
            PathToGranule[pathId].emplace_back(DefaultMark, granule);
            return true;
        }

        ui64 SetTmpGranule(ui64 pathId, const TMark& mark) {
            Y_VERIFY(pathId == SrcGranule->PathId);
            if (!TmpGranuleIds.contains(mark)) {
                TmpGranuleIds[mark] = FirstGranuleId;
                ++FirstGranuleId;
            }
            return TmpGranuleIds[mark];
        }

        THashMap<ui64, ui64> TmpToNewGranules(ui64 start) {
            THashMap<ui64, ui64> granuleRemap;
            for (const auto& [mark, counter] : TmpGranuleIds) {
                ui64 granule = start + counter;
                if (mark == SrcGranule->Mark) {
                    Y_VERIFY(!counter);
                    granule = SrcGranule->Granule;
                } else {
                    Y_VERIFY(counter);
                    NewGranules.emplace(granule, std::make_pair(SrcGranule->PathId, mark));
                }
                granuleRemap[counter] = granule;
            }
            return granuleRemap;
        }

        ui32 NumSplitInto(const ui32 srcRows) const {
            Y_VERIFY(srcRows > 1);
            const ui64 totalBytes = TotalBlobsSize();
            const ui32 numSplitInto = (totalBytes / Limits.GranuleExpectedSize) + 1;
            return std::max<ui32>(2, numSplitInto);
        }

        const TMark DefaultMark;
        THashMap<ui64, std::vector<std::pair<TMark, ui64>>> PathToGranule; // pathId -> {mark, granule}
        std::optional<TSrcGranule> SrcGranule;
        THashMap<ui64, std::pair<ui64, TMark>> NewGranules; // granule -> {pathId, key}
        THashMap<TMark, ui32> TmpGranuleIds; // mark -> tmp granule id
        TMarksGranules MergeBorders;
        ui64 FirstGranuleId{0};
        ui32 ReservedGranuleIds{0};
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
        LOAD,
        EVICT
    };

    TColumnEngineForLogs(ui64 tabletId, const TCompactionLimits& limits = {});

    const TIndexInfo& GetIndexInfo() const override {
        return VersionedIndex.GetLastSchema()->GetIndexInfo();
    }

    const TVersionedIndex& GetVersionedIndex() const override {
        return VersionedIndex;
    }

    const THashSet<ui64>* GetOverloadedGranules(ui64 pathId) const override {
        if (auto pi = PathsGranulesOverloaded.find(pathId); pi != PathsGranulesOverloaded.end()) {
            return &pi->second;
        }
        return nullptr;
    }

    bool HasOverloadedGranules() const override { return !PathsGranulesOverloaded.empty(); }

    TString SerializeMark(const NArrow::TReplaceKey& key) const override {
        if (UseCompositeMarks()) {
            return TMark::SerializeComposite(key, MarkSchema());
        } else {
            return TMark::SerializeScalar(key, MarkSchema());
        }
    }

    NArrow::TReplaceKey DeserializeMark(const TString& key) const override {
        if (UseCompositeMarks()) {
            return TMark::DeserializeComposite(key, MarkSchema());
        } else {
            return TMark::DeserializeScalar(key, MarkSchema());
        }
    }

    const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const override;
    const TColumnEngineStats& GetTotalStats() override;
    ui64 MemoryUsage() const override;
    TSnapshot LastUpdate() const override { return LastSnapshot; }

public:
    bool Load(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs, const THashSet<ui64>& pathsToDrop = {}) override;

    std::shared_ptr<TColumnEngineChanges> StartInsert(std::vector<TInsertedData>&& dataToIndex) override;
    std::shared_ptr<TColumnEngineChanges> StartCompaction(std::unique_ptr<TCompactionInfo>&& compactionInfo,
                                                          const TSnapshot& outdatedSnapshot) override;
    std::shared_ptr<TColumnEngineChanges> StartCleanup(const TSnapshot& snapshot, THashSet<ui64>& pathsToDrop,
                                                       ui32 maxRecords) override;
    std::shared_ptr<TColumnEngineChanges> StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<arrow::Schema>& schema,
                                                   ui64 maxEvictBytes = TCompactionLimits::DEFAULT_EVICTION_BYTES) override;

    bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges,
                      const TSnapshot& snapshot) override;

    void FreeLocks(std::shared_ptr<TColumnEngineChanges> changes) override;

    void UpdateDefaultSchema(const TSnapshot& snapshot, TIndexInfo&& info) override;
    void UpdateCompactionLimits(const TCompactionLimits& limits) override { Limits = limits; }



    std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                        const THashSet<ui32>& columnIds,
                                        const TPKRangesFilter& pkRangesFilter) const override;
    std::unique_ptr<TCompactionInfo> Compact(ui64& lastCompactedGranule) override;

private:
    using TMarksMap = std::map<TMark, ui64, TMark::TCompare>;

    struct TGranuleMeta {
        const TGranuleRecord Record;
        THashMap<ui64, TPortionInfo> Portions; // portion -> portionInfo

        explicit TGranuleMeta(const TGranuleRecord& rec)
            : Record(rec)
        {}

        ui64 PathId() const noexcept { return Record.PathId; }
        bool Empty() const noexcept { return Portions.empty(); }
    };

    TVersionedIndex VersionedIndex;
    TCompactionLimits Limits;
    ui64 TabletId;
    std::shared_ptr<TGranulesTable> GranulesTable;
    std::shared_ptr<TColumnsTable> ColumnsTable;
    std::shared_ptr<TCountersTable> CountersTable;
    THashMap<ui64, std::shared_ptr<TGranuleMeta>> Granules; // granule -> meta
    THashMap<ui64, TMarksMap> PathGranules; // path_id -> {mark, granule}
    TMap<ui64, std::shared_ptr<TColumnEngineStats>> PathStats; // per path_id stats sorted by path_id
    THashSet<ui64> GranulesInSplit;
    /// Set of empty granules.
    /// Just for providing count of empty granules.
    THashSet<ui64> EmptyGranules;
    THashMap<ui64, THashSet<ui64>> PathsGranulesOverloaded;
    TSet<ui64> CompactionGranules;
    THashSet<ui64> CleanupGranules;
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

    bool UseCompositeMarks() const {
        return MarkSchema()->num_fields() > 1;
    }

    void ClearIndex() {
        Granules.clear();
        PathGranules.clear();
        PathStats.clear();
        GranulesInSplit.clear();
        EmptyGranules.clear();
        PathsGranulesOverloaded.clear();
        CompactionGranules.clear();
        CleanupGranules.clear();
        Counters.Clear();

        LastPortion = 0;
        LastGranule = 0;
        LastSnapshot = TSnapshot::Zero();
    }

    bool LoadGranules(IDbWrapper& db);
    bool LoadColumns(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs);
    bool LoadCounters(IDbWrapper& db);
    bool ApplyChanges(IDbWrapper& db, const TChanges& changes, const TSnapshot& snapshot, bool apply);

    void EraseGranule(ui64 pathId, ui64 granule, const TMark& mark);

    /// Insert granule or check if same granule was already inserted.
    bool SetGranule(const TGranuleRecord& rec, bool apply);
    bool UpsertPortion(const TPortionInfo& portionInfo, bool apply, bool updateStats = true);
    bool ErasePortion(const TPortionInfo& portionInfo, bool apply, bool updateStats = true);
    void UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType = EStatsUpdateType::DEFAULT);
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                            EStatsUpdateType updateType) const;

    bool CanInsert(const TChanges& changes, const TSnapshot& commitSnap) const;
    void UpdateOverloaded(const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& granules);

    /// Return lists of adjacent empty granules for the path.
    std::vector<std::vector<std::pair<TMark, ui64>>> EmptyGranuleTracks(const ui64 pathId) const;
};

} // namespace NKikimr::NOlap
