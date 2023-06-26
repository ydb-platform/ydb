#pragma once

#include "defs.h"
#include "column_engine.h"
#include "scalars.h"
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include "storage/granule.h"
#include "storage/storage.h"

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
private:
    const NColumnShard::TEngineLogsCounters SignalCounters;
    std::shared_ptr<TGranulesStorage> GranulesStorage;
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
        struct TPrivateTag {
        };

    public:
        const TGranuleMeta* GetGranuleMeta() const {
            if (CompactionInfo) {
                return &CompactionInfo->GetObject<TGranuleMeta>();
            } else {
                return nullptr;
            }
        }

        struct TSrcGranule {
            ui64 PathId{0};
            ui64 Granule{0};
            TMark Mark;

            TSrcGranule(ui64 pathId, ui64 granule, const TMark& mark)
                : PathId(pathId), Granule(granule), Mark(mark)
            {}
        };

        TChanges(TColumnEngineChanges::EType type, const TMark& defaultMark, const TCompactionLimits& limits, const TPrivateTag&)
            : TColumnEngineChanges(type, limits)
            , DefaultMark(defaultMark)
        {
        }

        static std::shared_ptr<TChanges> BuildInsertChanges(const TMark& defaultMark,
                 std::vector<NOlap::TInsertedData>&& blobsToIndex, const TSnapshot& initSnapshot,
                 const TCompactionLimits& limits) {
            auto changes = std::make_shared<TChanges>(TColumnEngineChanges::INSERT, defaultMark, limits, TPrivateTag());
            changes->DataToIndex = std::move(blobsToIndex);
            changes->InitSnapshot = initSnapshot;
            return changes;
        }

        static std::shared_ptr<TChanges> BuildCompactionChanges(const TMark& defaultMark,
                 std::unique_ptr<TCompactionInfo>&& info,
                 const TCompactionLimits& limits,
                 const TSnapshot& initSnapshot) {
            auto changes = std::make_shared<TChanges>(TColumnEngineChanges::COMPACTION, defaultMark, limits, TPrivateTag());
            changes->CompactionInfo = std::move(info);
            changes->InitSnapshot = initSnapshot;
            return changes;
        }

        static std::shared_ptr<TChanges> BuildCleanupChanges(const TMark& defaultMark, const TSnapshot& initSnapshot, const TCompactionLimits& limits) {
            auto changes = std::make_shared<TChanges>(TColumnEngineChanges::CLEANUP, defaultMark, limits, TPrivateTag());
            changes->InitSnapshot = initSnapshot;
            return changes;
        }

        static std::shared_ptr<TChanges> BuildTtlChanges(const TMark& defaultMark) {
            return std::make_shared<TChanges>(TColumnEngineChanges::TTL, defaultMark, TCompactionLimits(), TPrivateTag());
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
        LOAD
    };

    TColumnEngineForLogs(ui64 tabletId, const TCompactionLimits& limits = {});

    const TIndexInfo& GetIndexInfo() const override {
        return VersionedIndex.GetLastSchema()->GetIndexInfo();
    }

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

    std::shared_ptr<TColumnEngineChanges> StartInsert(const TCompactionLimits& limits, std::vector<TInsertedData>&& dataToIndex) override;
    std::shared_ptr<TColumnEngineChanges> StartCompaction(std::unique_ptr<TCompactionInfo>&& compactionInfo,
                                                          const TSnapshot& outdatedSnapshot, const TCompactionLimits& limits) override;
    std::shared_ptr<TColumnEngineChanges> StartCleanup(const TSnapshot& snapshot, const TCompactionLimits& limits, THashSet<ui64>& pathsToDrop,
                                                       ui32 maxRecords) override;
    std::shared_ptr<TColumnEngineChanges> StartTtl(const THashMap<ui64, TTiering>& pathEviction, const std::shared_ptr<arrow::Schema>& schema,
                                                   ui64 maxEvictBytes = TCompactionLimits::DEFAULT_EVICTION_BYTES) override;

    bool ApplyChanges(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> indexChanges,
                      const TSnapshot& snapshot) noexcept override;

    void FreeLocks(std::shared_ptr<TColumnEngineChanges> changes) override;

    void UpdateDefaultSchema(const TSnapshot& snapshot, TIndexInfo&& info) override;



    std::shared_ptr<TSelectInfo> Select(ui64 pathId, TSnapshot snapshot,
                                        const THashSet<ui32>& columnIds,
                                        const TPKRangesFilter& pkRangesFilter) const override;
    std::unique_ptr<TCompactionInfo> Compact(const TCompactionLimits& limits) override;

private:
    using TMarksMap = std::map<TMark, ui64, TMark::TCompare>;

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
        return GetIndexInfo().IsCompositeIndexKey();
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
    bool ApplyChanges(IDbWrapper& db, const TChanges& changes, const TSnapshot& snapshot, bool apply) noexcept;

    void EraseGranule(ui64 pathId, ui64 granule, const TMark& mark);

    /// Insert granule or check if same granule was already inserted.
    bool SetGranule(const TGranuleRecord& rec, bool apply);
    bool UpsertPortion(const TPortionInfo& portionInfo, bool apply, bool updateStats = true, const TPortionInfo* exInfo = nullptr);
    bool ErasePortion(const TPortionInfo& portionInfo, bool apply, bool updateStats = true);
    void UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType = EStatsUpdateType::DEFAULT,
                            std::optional<TPortionMeta::EProduced> exProduced = {});
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                            EStatsUpdateType updateType,
                            std::optional<TPortionMeta::EProduced> exProduced = {}) const;

    bool CanInsert(const TChanges& changes, const TSnapshot& commitSnap) const;

    /// Return lists of adjacent empty granules for the path.
    std::vector<std::vector<std::pair<TMark, ui64>>> EmptyGranuleTracks(const ui64 pathId) const;
};

} // namespace NKikimr::NOlap
