#pragma once

#include "defs.h"
#include "portion_info.h"
#include "column_engine_logs.h"

namespace NKikimr::NOlap {

class TIndexLogicBase {
protected:
    const TVersionedIndex& SchemaVersions;
private:
    const THashMap<ui64, NKikimr::NOlap::TTiering>* TieringMap = nullptr;

public:
    TIndexLogicBase(const TVersionedIndex& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap)
        : SchemaVersions(indexInfo)
        , TieringMap(&tieringMap)
    {
    }

    TIndexLogicBase(const TVersionedIndex& indexInfo)
        : SchemaVersions(indexInfo)
    {
    }

    virtual ~TIndexLogicBase() {
    }
    virtual std::vector<TString> Apply(std::shared_ptr<TColumnEngineChanges> indexChanges) const = 0;

    static THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                                                const std::vector<std::pair<TMark, ui64>>& granules,
                                                                                const TIndexInfo& indexInfo);

protected:
    std::vector<TPortionInfo> MakeAppendedPortions(const ui64 pathId,
                                            const std::shared_ptr<arrow::RecordBatch> batch,
                                            const ui64 granule,
                                            const TSnapshot& minSnapshot,
                                            std::vector<TString>& blobs) const;

    static std::shared_ptr<arrow::RecordBatch> GetEffectiveKey(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                            const TIndexInfo& indexInfo);

    const THashMap<ui64, NKikimr::NOlap::TTiering>& GetTieringMap() const {
        if (TieringMap) {
            return *TieringMap;
        }
        return Default<THashMap<ui64, NKikimr::NOlap::TTiering>>();
    }
};

class TIndexationLogic: public TIndexLogicBase {
public:
    using TIndexLogicBase::TIndexLogicBase;

    std::vector<TString> Apply(std::shared_ptr<TColumnEngineChanges> indexChanges) const override;

private:
    // Although source batches are ordered only by PK (sorting key) resulting pathBatches are ordered by extended key.
    // They have const snapshot columns that do not break sorting inside batch.
    std::shared_ptr<arrow::RecordBatch> AddSpecials(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                    const TIndexInfo& indexInfo, const TInsertedData& inserted) const;
};

class TCompactionLogic: public TIndexLogicBase {
public:
    using TIndexLogicBase::TIndexLogicBase;

    std::vector<TString> Apply(std::shared_ptr<TColumnEngineChanges> indexChanges) const override;

private:
    std::vector<TString> CompactSplitGranule(const std::shared_ptr<TColumnEngineForLogs::TChanges>& changes) const;
    std::vector<TString> CompactInGranule(std::shared_ptr<TColumnEngineForLogs::TChanges> changes) const;
    std::pair<std::shared_ptr<arrow::RecordBatch>, TSnapshot> CompactInOneGranule(ui64 granule, const std::vector<TPortionInfo>& portions, const THashMap<TBlobRange, TString>& blobs) const;

    /// @return vec({ts, batch}). ts0 <= ts1 <= ... <= tsN
    /// @note We use ts from PK for split but there could be lots PK with the same ts.
    std::vector<std::pair<TMark, std::shared_ptr<arrow::RecordBatch>>>
    SliceGranuleBatches(const TIndexInfo& indexInfo,
                        const TColumnEngineForLogs::TChanges& changes,
                        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                        const TMark& ts0) const;

    /// @param[in,out] portions unchanged or only inserted portions in the same orders
    /// @param[in,out] tsIds    unchanged or marks from compacted portions ordered by mark
    /// @param[in,out] toMove   unchanged or compacted portions ordered by primary key
    ui64 TryMovePortions(const TMark& ts0,
                        std::vector<TPortionInfo>& portions,
                        std::vector<std::pair<TMark, ui64>>& tsIds,
                        std::vector<std::pair<TPortionInfo, ui64>>& toMove) const;

    std::pair<std::vector<std::shared_ptr<arrow::RecordBatch>>, TSnapshot> PortionsToBatches(const std::vector<TPortionInfo>& portions,
                                                                    const THashMap<TBlobRange, TString>& blobs,
                                                                    bool insertedOnly = false) const;
};

class TEvictionLogic: public TIndexLogicBase {
public:
    using TIndexLogicBase::TIndexLogicBase;

    std::vector<TString> Apply(std::shared_ptr<TColumnEngineChanges> indexChanges) const override;

private:
    bool UpdateEvictedPortion(TPortionInfo& portionInfo,
                            TPortionEvictionFeatures& evictFeatures, const THashMap<TBlobRange, TString>& srcBlobs,
                            std::vector<TColumnRecord>& evictedRecords, std::vector<TString>& newBlobs) const;
};

}
