#pragma once
#include "abstract.h"

#include <unordered_set>

namespace NKikimr::NOlap::NReader::NSimple {

// Early-stop controller for DISTINCT+LIMIT pushdown.
// Counts number of rows produced after DISTINCT (i.e. number of distinct keys),
// and stops scanning once it reaches the requested limit (or more).
class TSyncPointDistinctLimitControl: public ISyncPoint {
private:
    using TBase = ISyncPoint;
    const ui64 Limit;
    ui64 FetchedDistinct = 0;
    /// Sources for which we already applied limit accounting using full batch row count (no ChunkToReply yet).
    std::unordered_set<ui32> SourcesWithFullBatchDistinctCount;

    virtual bool IsSourcePrepared(const std::shared_ptr<NCommon::IDataSource>& source) const override {
        return source->IsSyncSection() && source->HasStageResult();
    }

    virtual ESourceAction OnSourceReady(const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& /*reader*/) override;

    virtual void DoAbort() override {
        FetchedDistinct = 0;
        SourcesWithFullBatchDistinctCount.clear();
    }

    virtual bool IsFinished() const override {
        return FetchedDistinct >= Limit || TBase::IsFinished();
    }

public:
    TSyncPointDistinctLimitControl(const ui64 limit, const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context,
        const std::shared_ptr<ISourcesCollection>& collection)
        : TBase(pointIndex, "SYNC_DISTINCT_LIMIT", context, collection)
        , Limit(limit) {
        AFL_VERIFY(Limit);
    }
};

} // namespace NKikimr::NOlap::NReader::NSimple

