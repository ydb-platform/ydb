#pragma once
#include "compaction.h"

namespace NKikimr::NOlap {

class TGranuleMeta;

class TInGranuleCompactColumnEngineChanges: public TCompactColumnEngineChanges {
private:
    using TBase = TCompactColumnEngineChanges;
    std::pair<std::shared_ptr<arrow::RecordBatch>, TSnapshot> CompactInOneGranule(ui64 granule,
        const std::vector<TPortionInfo>& portions, const THashMap<TBlobRange, TString>& blobs,
        TConstructionContext& context) const;
protected:
    virtual TConclusion<std::vector<TString>> DoConstructBlobs(TConstructionContext& context) noexcept override;
public:
    using TBase::TBase;
};

}
