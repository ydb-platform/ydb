#pragma once
#include "defs.h"
#include "column_engine.h"

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
    class TChanges : public TColumnEngineChanges {
    public:
        struct TSrcGranule {
            ui64 PathId{0};
            ui64 Granule{0};
            ui64 Ts{0};

            bool Good() const { return PathId && Granule; }
        };

        TChanges(TVector<NOlap::TInsertedData>&& blobsToIndex, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::INSERT)
        {
            Limits = limits;
            DataToIndex = std::move(blobsToIndex);
        }

        TChanges(std::unique_ptr<TCompactionInfo>&& info, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::COMPACTION)
        {
            Limits = limits;
            CompactionInfo = std::move(info);
        }

        TChanges(const TSnapshot& snapshot, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::CLEANUP)
        {
            Limits = limits;
            InitSnapshot = snapshot;
        }

        TChanges(TColumnEngineChanges::EType type, const TSnapshot& applySnapshot)
            : TColumnEngineChanges(type)
        {
            ApplySnapshot = applySnapshot;
        }

        bool AddPathIfNotExists(ui64 pathId) {
            if (PathToGranule.count(pathId)) {
                return false;
            }

            Y_VERIFY(FirstGranuleId && ReservedGranuleIds);
            ui64 granule = FirstGranuleId;
            ++FirstGranuleId;
            --ReservedGranuleIds;
            ui64 ts = 0;
            NewGranules.emplace(granule, std::make_pair(pathId, ts));
            PathToGranule[pathId].emplace_back(ts, granule);
            return true;
        }

        ui64 SetTmpGranule(ui64 pathId, ui64 ts) {
            Y_VERIFY(pathId == SrcGranule.PathId);
            if (!TmpGranuleIds.count(ts)) {
                TmpGranuleIds[ts] = FirstGranuleId;
                ++FirstGranuleId;
            }
            return TmpGranuleIds[ts];
        }

        THashMap<ui64, ui64> TmpToNewGranules(ui64 start) {
            THashMap<ui64, ui64> granuleRemap;
            for (auto& [ts, counter] : TmpGranuleIds) {
                ui64 granule = start + counter;
                if (ts == SrcGranule.Ts) {
                    Y_VERIFY(!counter);
                    granule = SrcGranule.Granule;
                } else {
                    Y_VERIFY(counter);
                    NewGranules[granule] = {SrcGranule.PathId, ts};
                }
                granuleRemap[counter] = granule;
            }
            return granuleRemap;
        }

        ui32 NumSplitInto(ui32 srcRows) const {
            Y_VERIFY(srcRows > 1);
            ui64 totalBytes = TotalBlobsSize();
            ui32 numSplitInto = totalBytes / Limits.GranuleExpectedSize + 1;
            if (numSplitInto < 2) {
                numSplitInto = 2;
            }
            return numSplitInto;
        }

        THashMap<ui64, std::vector<std::pair<ui64, ui64>>> PathToGranule; // pathId -> {timestamp, granule}
        TSrcGranule SrcGranule;
        THashMap<ui64, std::pair<ui64, ui64>> NewGranules; // granule -> {pathId, key}
        THashMap<ui64, ui32> TmpGranuleIds; // ts -> tmp granule id
        TMap<ui64, ui64> MergeBorders;
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

    TColumnEngineForLogs(TIndexInfo&& info, ui64 tabletId, const TCompactionLimits& limits = {});

    const TIndexInfo& GetIndexInfo() const override { return IndexInfo; }
    const THashSet<ui64>* GetOverloadedGranules(ui64 pathId) const override {
        if (PathsGranulesOverloaded.count(pathId)) {
            return &PathsGranulesOverloaded.find(pathId)->second;
        }
        return nullptr;
    }

    bool Load(IDbWrapper& db, const THashSet<ui64>& pathsToDrop = {}) override;
    std::shared_ptr<TColumnEngineChanges> StartInsert(TVector<TInsertedData>&& dataToIndex) override;
    std::shared_ptr<TColumnEngineChanges> StartCompaction(std::unique_ptr<TCompactionInfo>&& compactionInfo,
                                                          const TSnapshot& outdatedSnapshot) override;
    std::shared_ptr<TColumnEngineChanges> StartCleanup(const TSnapshot& snapshot,
                                                       THashSet<ui64>& pathsToDrop) override;
    std::shared_ptr<TColumnEngineChanges> StartTtl(const THashMap<ui64, TTiersInfo>& pathTtls,
                                                   ui64 maxEvictBytes = TCompactionLimits::DEFAULT_EVICTION_BYTES) override;
    bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges,
                      const TSnapshot& snapshot) override;
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
    std::unique_ptr<TCompactionInfo> Compact() override;

    // Static part of IColumnEngine iface (called from actors). It's static cause there's no threads sync.

    /// @note called from IndexingActor
    static TVector<TString> IndexBlobs(const TIndexInfo& indexInfo, std::shared_ptr<TColumnEngineChanges> changes);

    /// @note called from CompactionActor
    static TVector<TString> CompactBlobs(const TIndexInfo& indexInfo, std::shared_ptr<TColumnEngineChanges> changes);

    /// @note called from EvictionActor
    static TVector<TString> EvictBlobs(const TIndexInfo& indexInfo, std::shared_ptr<TColumnEngineChanges> changes);

    static ui64 ExtractKey(const TString& key) {
        Y_VERIFY(key.size() == 8);
        return *reinterpret_cast<const ui64*>(key.data());
    }

    static ui64 ExtractKey(const arrow::Scalar& scalar) {
        return static_cast<const arrow::TimestampScalar&>(scalar).value;
    }

private:
    struct TGranuleMeta {
        TGranuleRecord Record;
        THashMap<ui64, TPortionInfo> Portions; // portion -> portionInfo

        TGranuleMeta(const TGranuleRecord& rec)
            : Record(rec)
        {}

        ui64 PathId() const { return Record.PathId; }
        bool Empty() const { return Portions.empty(); }

        bool AllActive() const {
            for (auto& [_, portionInfo] : Portions) {
                if (!portionInfo.IsActive()) {
                    return false;
                }
            }
            return true;
        }
    };

    TIndexInfo IndexInfo;
    TCompactionLimits Limits;
    ui64 TabletId;
    std::shared_ptr<TGranulesTable> GranulesTable;
    std::shared_ptr<TColumnsTable> ColumnsTable;
    std::shared_ptr<TCountersTable> CountersTable;
    THashMap<ui64, std::shared_ptr<TGranuleMeta>> Granules; // granule -> meta
    THashMap<ui64, TMap<ui64, ui64>> PathGranules; // path_id -> {timestamp, granule}
    TMap<ui64, std::shared_ptr<TColumnEngineStats>> PathStats; // per path_id stats sorted by path_id
    THashSet<ui64> GranulesInSplit;
    THashSet<ui64> EmptyGranules;
    THashMap<ui64, THashSet<ui64>> PathsGranulesOverloaded;
    THashSet<ui64> CompactionGranules;
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
    bool LoadColumns(IDbWrapper& db);
    bool LoadCounters(IDbWrapper& db);
    bool ApplyChanges(IDbWrapper& db, const TChanges& changes, const TSnapshot& snapshot, bool apply);

    void EraseGranule(ui64 pathId, ui64 granule, ui64 ts);
    bool SetGranule(const TGranuleRecord& rec, bool apply);
    bool UpsertPortion(const TPortionInfo& portionInfo, bool apply, bool updateStats = true);
    bool ErasePortion(const TPortionInfo& portionInfo, bool apply, bool updateStats = true);
    void AddColumnRecord(const TColumnRecord& row);
    void UpdatePortionStats(const TPortionInfo& portionInfo, bool isErase = false, bool isLoad = false);
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                            bool isErase = false, bool isLoad = false) const;

    bool CanInsert(const TChanges& changes, const TSnapshot& commitSnap) const;
    TMap<TSnapshot, TVector<ui64>> GetOrderedPortions(ui64 granule, const TSnapshot& snapshot = TSnapshot::Max()) const;
    void UpdateOverloaded(const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& granules,
                          const TChanges::TSrcGranule& splitted);

    TVector<TVector<std::pair<ui64, ui64>>> EmptyGranuleTracks(ui64 pathId) const;
};


std::shared_ptr<arrow::TimestampArray> GetTimestampColumn(const TIndexInfo& indexInfo,
                                                          const std::shared_ptr<arrow::RecordBatch>& batch);
THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>
SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch, const TMap<ui64, ui64>& tsGranules,
                  const TIndexInfo& indexInfo);
THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>
SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::pair<ui64, ui64>>& tsGranules,
                  const TIndexInfo& indexInfo);

}
