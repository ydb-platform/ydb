#pragma once

#include "defs.h"
#include "portion_info.h"
#include "column_engine_logs.h"

namespace NKikimr::NOlap {

class TIndexLogicBase {
protected:
    const TIndexInfo& IndexInfo;
private:
    const THashMap<ui64, NKikimr::NOlap::TTiering>* TieringMap = nullptr;

public:
    TIndexLogicBase(const TIndexInfo& indexInfo, const THashMap<ui64, NKikimr::NOlap::TTiering>& tieringMap)
        : IndexInfo(indexInfo)
        , TieringMap(&tieringMap)
    {
    }

    TIndexLogicBase(const TIndexInfo& indexInfo)
        : IndexInfo(indexInfo)        {
    }

    virtual ~TIndexLogicBase() {
    }
    virtual TVector<TString> Apply(std::shared_ptr<TColumnEngineChanges> indexChanges) const = 0;

    static THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                                                const std::vector<std::pair<TMark, ui64>>& granules,
                                                                                const TIndexInfo& indexInfo);

protected:
    TVector<TPortionInfo> MakeAppendedPortions(const ui64 pathId,
                                            const std::shared_ptr<arrow::RecordBatch> batch,
                                            const ui64 granule,
                                            const TSnapshot& minSnapshot,
                                            TVector<TString>& blobs) const;

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

    TVector<TString> Apply(std::shared_ptr<TColumnEngineChanges> indexChanges) const override;

private:
    // Although source batches are ordered only by PK (sorting key) resulting pathBatches are ordered by extended key.
    // They have const snapshot columns that do not break sorting inside batch.
    std::shared_ptr<arrow::RecordBatch> AddSpecials(const std::shared_ptr<arrow::RecordBatch>& srcBatch,
                                                    const TIndexInfo& indexInfo, const TInsertedData& inserted) const;
};

class TCompactionLogic: public TIndexLogicBase {
public:
    using TIndexLogicBase::TIndexLogicBase;

    TVector<TString> Apply(std::shared_ptr<TColumnEngineChanges> indexChanges) const override;

private:
    TVector<TString> CompactSplitGranule(const std::shared_ptr<TColumnEngineForLogs::TChanges>& changes) const;
    TVector<TString> CompactInGranule(std::shared_ptr<TColumnEngineForLogs::TChanges> changes) const;
    std::shared_ptr<arrow::RecordBatch> CompactInOneGranule(ui64 granule, const TVector<TPortionInfo>& portions, const THashMap<TBlobRange, TString>& blobs) const;

    /// @return vec({ts, batch}). ts0 <= ts1 <= ... <= tsN
    /// @note We use ts from PK for split but there could be lots PK with the same ts.
    TVector<std::pair<TMark, std::shared_ptr<arrow::RecordBatch>>>
    SliceGranuleBatches(const TIndexInfo& indexInfo,
                        const TColumnEngineForLogs::TChanges& changes,
                        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                        const TMark& ts0) const;

    /// @param[in,out] portions unchanged or only inserted portions in the same orders
    /// @param[in,out] tsIds    unchanged or marks from compacted portions ordered by mark
    /// @param[in,out] toMove   unchanged or compacted portions ordered by primary key
    ui64 TryMovePortions(const TMark& ts0,
                        TVector<TPortionInfo>& portions,
                        std::vector<std::pair<TMark, ui64>>& tsIds,
                        TVector<std::pair<TPortionInfo, ui64>>& toMove) const;

    std::vector<std::shared_ptr<arrow::RecordBatch>> PortionsToBatches(const TVector<TPortionInfo>& portions,
                                                                    const THashMap<TBlobRange, TString>& blobs,
                                                                    bool insertedOnly = false) const;
};

class TEvictionLogic: public TIndexLogicBase {
public:
    using TIndexLogicBase::TIndexLogicBase;

    TVector<TString> Apply(std::shared_ptr<TColumnEngineChanges> indexChanges) const override;

private:
    bool UpdateEvictedPortion(TPortionInfo& portionInfo,
                            TPortionEvictionFeatures& evictFeatures, const THashMap<TBlobRange, TString>& srcBlobs,
                            TVector<TColumnRecord>& evictedRecords, TVector<TString>& newBlobs) const;
};

}
