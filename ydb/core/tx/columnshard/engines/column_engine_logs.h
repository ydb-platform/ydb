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
    struct TMark {
        std::shared_ptr<arrow::Scalar> Border;

        explicit TMark(const std::shared_ptr<arrow::Scalar>& s)
            : Border(s)
        {
            Y_VERIFY_DEBUG(NArrow::IsGoodScalar(Border));
        }

        explicit TMark(const std::shared_ptr<arrow::DataType>& type)
            : Border(MinScalar(type))
        {
            Y_VERIFY_DEBUG(NArrow::IsGoodScalar(Border));
        }

        TMark(const TString& key, const std::shared_ptr<arrow::DataType>& type) {
            Deserialize(key, type);
            Y_VERIFY_DEBUG(NArrow::IsGoodScalar(Border));
        }

        TMark(const TMark& m) = default;
        TMark& operator = (const TMark& m) = default;

        bool operator == (const TMark& m) const {
            Y_VERIFY(Border);
            Y_VERIFY(m.Border);
            return Border->Equals(*m.Border);
        }

        bool operator < (const TMark& m) const {
            Y_VERIFY(Border);
            Y_VERIFY(m.Border);
            return NArrow::ScalarLess(*Border, *m.Border);
        }

        bool operator <= (const TMark& m) const {
            Y_VERIFY(Border);
            Y_VERIFY(m.Border);
            return Border->Equals(*m.Border) || NArrow::ScalarLess(*Border, *m.Border);
        }

        bool operator > (const TMark& m) const {
            return !(*this <= m);
        }

        bool operator >= (const TMark& m) const {
            return !(*this < m);
        }

        ui64 Hash() const {
            return Border->hash();
        }

        operator size_t () const {
            return Hash();
        }

        operator bool () const {
            Y_VERIFY(false);
        }

        TString Serialize() const {
            return SerializeKeyScalar(Border);
        }

        void Deserialize(const TString& key, const std::shared_ptr<arrow::DataType>& type) {
            Border = DeserializeKeyScalar(key, type);
        }

        static std::shared_ptr<arrow::Scalar> MinScalar(const std::shared_ptr<arrow::DataType>& type) {
            if (type->id() == arrow::Type::TIMESTAMP) {
                // TODO: support negative timestamps in index
                return std::make_shared<arrow::TimestampScalar>(0, type);
            }
            return NArrow::MinScalar(type);
        }
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
            , DefaultMark(engine.GetMarkType())
        {
            Limits = limits;
            DataToIndex = std::move(blobsToIndex);
        }

        TChanges(const TColumnEngineForLogs& engine,
                 std::unique_ptr<TCompactionInfo>&& info, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::COMPACTION)
            , DefaultMark(engine.GetMarkType())
        {
            Limits = limits;
            CompactionInfo = std::move(info);
        }

        TChanges(const TColumnEngineForLogs& engine,
                 const TSnapshot& snapshot, const TCompactionLimits& limits)
            : TColumnEngineChanges(TColumnEngineChanges::CLEANUP)
            , DefaultMark(engine.GetMarkType())
        {
            Limits = limits;
            InitSnapshot = snapshot;
        }

        TChanges(const TColumnEngineForLogs& engine,
                 TColumnEngineChanges::EType type, const TSnapshot& applySnapshot)
            : TColumnEngineChanges(type)
            , DefaultMark(engine.GetMarkType())
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

            NewGranules.emplace(granule, std::make_pair(pathId, DefaultMark));
            PathToGranule[pathId].emplace_back(DefaultMark, granule);
            return true;
        }

        ui64 SetTmpGranule(ui64 pathId, const TMark& mark) {
            Y_VERIFY(pathId == SrcGranule->PathId);
            if (!TmpGranuleIds.count(mark)) {
                TmpGranuleIds[mark] = FirstGranuleId;
                ++FirstGranuleId;
            }
            return TmpGranuleIds[mark];
        }

        THashMap<ui64, ui64> TmpToNewGranules(ui64 start) {
            THashMap<ui64, ui64> granuleRemap;
            for (auto& [mark, counter] : TmpGranuleIds) {
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

        ui32 NumSplitInto(ui32 srcRows) const {
            Y_VERIFY(srcRows > 1);
            ui64 totalBytes = TotalBlobsSize();
            ui32 numSplitInto = totalBytes / Limits.GranuleExpectedSize + 1;
            if (numSplitInto < 2) {
                numSplitInto = 2;
            }
            return numSplitInto;
        }

        TMark DefaultMark;
        THashMap<ui64, std::vector<std::pair<TMark, ui64>>> PathToGranule; // pathId -> {mark, granule}
        std::optional<TSrcGranule> SrcGranule;
        THashMap<ui64, std::pair<ui64, TMark>> NewGranules; // granule -> {pathId, key}
        THashMap<TMark, ui32> TmpGranuleIds; // mark -> tmp granule id
        TMap<TMark, ui64> MergeBorders;
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
        ADD,
    };

    TColumnEngineForLogs(TIndexInfo&& info, ui64 tabletId, const TCompactionLimits& limits = {});

    const TIndexInfo& GetIndexInfo() const override { return IndexInfo; }

    const THashSet<ui64>* GetOverloadedGranules(ui64 pathId) const override {
        if (PathsGranulesOverloaded.count(pathId)) {
            return &PathsGranulesOverloaded.find(pathId)->second;
        }
        return nullptr;
    }

    bool HasOverloadedGranules() const override { return !PathsGranulesOverloaded.empty(); }

    TString SerializeMark(const std::shared_ptr<arrow::Scalar>& scalar) const override {
        Y_VERIFY_S(scalar->type->Equals(MarkType), scalar->type->ToString() + ", expected " + MarkType->ToString());
        return TMark(scalar).Serialize();
    }

    std::shared_ptr<arrow::Scalar> DeserializeMark(const TString& key) const override {
        return TMark(key, MarkType).Border;
    }

    const std::shared_ptr<arrow::DataType>& GetMarkType() const {
        return MarkType;
    }

    bool Load(IDbWrapper& db, THashSet<TUnifiedBlobId>& lostBlobs, const THashSet<ui64>& pathsToDrop = {}) override;
    std::shared_ptr<TColumnEngineChanges> StartInsert(TVector<TInsertedData>&& dataToIndex) override;
    std::shared_ptr<TColumnEngineChanges> StartCompaction(std::unique_ptr<TCompactionInfo>&& compactionInfo,
                                                          const TSnapshot& outdatedSnapshot) override;
    std::shared_ptr<TColumnEngineChanges> StartCleanup(const TSnapshot& snapshot, THashSet<ui64>& pathsToDrop,
                                                       ui32 maxRecords) override;
    std::shared_ptr<TColumnEngineChanges> StartTtl(const THashMap<ui64, TTiering>& pathEviction,
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
    static TVector<TString> IndexBlobs(const TIndexInfo& indexInfo, std::shared_ptr<TColumnEngineChanges> changes);

    /// @note called from CompactionActor
    static TVector<TString> CompactBlobs(const TIndexInfo& indexInfo, std::shared_ptr<TColumnEngineChanges> changes);

    /// @note called from EvictionActor
    static TVector<TString> EvictBlobs(const TIndexInfo& indexInfo, std::shared_ptr<TColumnEngineChanges> changes);

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
    std::shared_ptr<arrow::DataType> MarkType;
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
    bool ReadOnly = false;

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
    bool UpsertPortion(const TPortionInfo& portionInfo, bool apply, const TPortionInfo* exInfo = nullptr);
    bool ErasePortion(const TPortionInfo& portionInfo, bool apply, bool updateStats = true);
    void AddColumnRecord(const TColumnRecord& row);
    void UpdatePortionStats(const TPortionInfo& portionInfo, EStatsUpdateType updateType = EStatsUpdateType::DEFAULT,
                            const TPortionInfo* exPortionInfo = nullptr);
    void UpdatePortionStats(TColumnEngineStats& engineStats, const TPortionInfo& portionInfo,
                            EStatsUpdateType updateType,
                            const TPortionInfo* exPortionInfo = nullptr) const;

    bool CanInsert(const TChanges& changes, const TSnapshot& commitSnap) const;
    TMap<TSnapshot, TVector<ui64>> GetOrderedPortions(ui64 granule, const TSnapshot& snapshot = TSnapshot::Max()) const;
    void UpdateOverloaded(const THashMap<ui64, std::shared_ptr<TGranuleMeta>>& granules);

    TVector<TVector<std::pair<TMark, ui64>>> EmptyGranuleTracks(ui64 pathId) const;
};


std::shared_ptr<arrow::Array> GetFirstPKColumn(const TIndexInfo& indexInfo,
                                               const std::shared_ptr<arrow::RecordBatch>& batch);
THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>
SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch,
                  const TMap<TColumnEngineForLogs::TMark, ui64>& tsGranules,
                  const TIndexInfo& indexInfo);
THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>
SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch,
                  const std::vector<std::pair<TColumnEngineForLogs::TMark, ui64>>& tsGranules,
                  const TIndexInfo& indexInfo);

}
