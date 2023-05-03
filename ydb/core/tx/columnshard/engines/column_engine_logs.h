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

        TChanges(const TColumnEngineForLogs& engine,
                 TVector<NOlap::TInsertedData>&& blobsToIndex, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::INSERT)
            , DefaultMark(engine.GetDefaultMark())
        {
            Limits = limits;
            DataToIndex = std::move(blobsToIndex);
        }

        TChanges(const TColumnEngineForLogs& engine,
                 std::unique_ptr<TCompactionInfo>&& info, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::COMPACTION)
            , DefaultMark(engine.GetDefaultMark())
        {
            Limits = limits;
            CompactionInfo = std::move(info);
        }

        TChanges(const TColumnEngineForLogs& engine,
                 const TSnapshot& snapshot, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::CLEANUP)
            , DefaultMark(engine.GetDefaultMark())
        {
            Limits = limits;
            InitSnapshot = snapshot;
        }

        TChanges(const TColumnEngineForLogs& engine,
                 TColumnEngineChanges::EType type, const TSnapshot& applySnapshot)
            : TColumnEngineChanges(type)
            , DefaultMark(engine.GetDefaultMark())
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

        TMark DefaultMark;
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

    TColumnEngineForLogs(TIndexInfo&& info, ui64 tabletId, const TCompactionLimits& limits = {});

    const TIndexInfo& GetIndexInfo() const override { return IndexInfo; }

    const THashSet<ui64>* GetOverloadedGranules(ui64 pathId) const override {
        if (PathsGranulesOverloaded.contains(pathId)) {
            return &PathsGranulesOverloaded.find(pathId)->second;
        }
        return nullptr;
    }

    bool HasOverloadedGranules() const override { return !PathsGranulesOverloaded.empty(); }

    TString SerializeMark(const NArrow::TReplaceKey& key) const override {
        return TMark::Serialize(key, MarkSchema);
    }

    NArrow::TReplaceKey DeserializeMark(const TString& key) const override {
        return TMark::Deserialize(key, MarkSchema);
    }

    TMark GetDefaultMark() const {
        return TMark(MarkSchema);
    }

    bool Load(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs, const THashSet<ui64>& pathsToDrop = {}) override;
    std::shared_ptr<TColumnEngineChanges> StartInsert(TVector<TInsertedData>&& dataToIndex) override;
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
    const TMap<ui64, std::shared_ptr<TColumnEngineStats>>& GetStats() const override;
    const TColumnEngineStats& GetTotalStats() override;
    ui64 MemoryUsage() const override;
    TSnapshot LastUpdate() const override { return LastSnapshot; }

    std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                        const THashSet<ui32>& columnIds,
                                        std::shared_ptr<TPredicate> from,
                                        std::shared_ptr<TPredicate> to) const override;
    std::unique_ptr<TCompactionInfo> Compact(ui64& lastCompactedGranule) override;

    // Static part of IColumnEngine iface (called from actors). It's static cause there's no threads sync.

    /// @note called from IndexingActor
    static TVector<TString> IndexBlobs(const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap, std::shared_ptr<TColumnEngineChanges> changes);

    /// @note called from CompactionActor
    static TVector<TString> CompactBlobs(const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap, std::shared_ptr<TColumnEngineChanges> changes);

    /// @note called from EvictionActor
    static TVector<TString> EvictBlobs(const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap, std::shared_ptr<TColumnEngineChanges> changes);

private:
    struct TGranuleMeta {
        TGranuleRecord Record;
        THashMap<ui64, TPortionInfo> Portions; // portion -> portionInfo

        TGranuleMeta(const TGranuleRecord& rec)
            : Record(rec)
        {}

        ui64 PathId() const { return Record.PathId; }
        bool Empty() const { return Portions.empty(); }
    };

    TIndexInfo IndexInfo;
    TCompactionLimits Limits;
    ui64 TabletId;
    std::shared_ptr<arrow::Schema> MarkSchema;
    std::shared_ptr<TGranulesTable> GranulesTable;
    std::shared_ptr<TColumnsTable> ColumnsTable;
    std::shared_ptr<TCountersTable> CountersTable;
    THashMap<ui64, std::shared_ptr<TGranuleMeta>> Granules; // granule -> meta
    THashMap<ui64, TMap<TMark, ui64>> PathGranules; // path_id -> {mark, granule}
    TMap<ui64, std::shared_ptr<TColumnEngineStats>> PathStats; // per path_id stats sorted by path_id
    THashSet<ui64> GranulesInSplit;
    THashSet<ui64> EmptyGranules;
    THashMap<ui64, THashSet<ui64>> PathsGranulesOverloaded;
    TSet<ui64> CompactionGranules;
    THashSet<ui64> CleanupGranules;
    TColumnEngineStats Counters;
    ui64 LastPortion;
    ui64 LastGranule;
    TSnapshot LastSnapshot;

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
        LastSnapshot = {};
    }

    bool LoadGranules(IDbWrapper& db);
    bool LoadColumns(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs);
    bool LoadCounters(IDbWrapper& db);
    bool ApplyChanges(IDbWrapper& db, const TChanges& changes, const TSnapshot& snapshot, bool apply);

    void EraseGranule(ui64 pathId, ui64 granule, const TMark& mark);
    bool SetGranule(const TGranuleRecord& rec, bool apply);
    bool UpsertPortion(const TPortionInfo& portionInfo, bool apply, bool updateStats = true);
    bool ErasePortion(const TPortionInfo& portionInfo, bool apply, bool updateStats = true);
    void AddColumnRecord(const TColumnRecord& row);
    void UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType = EStatsUpdateType::DEFAULT);
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                            EStatsUpdateType updateType) const;

    bool CanInsert(const TChanges& changes, const TSnapshot& commitSnap) const;
    TMap<TSnapshot, TVector<ui64>> GetOrderedPortions(ui64 granule, const TSnapshot& snapshot = TSnapshot::Max()) const;
    void UpdateOverloaded(const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& granules);

    TVector<TVector<std::pair<TMark, ui64>>> EmptyGranuleTracks(ui64 pathId) const;
};

}
