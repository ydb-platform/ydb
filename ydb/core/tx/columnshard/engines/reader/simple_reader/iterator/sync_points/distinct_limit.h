#pragma once
#include "abstract.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

#include <memory>
#include <unordered_set>

namespace NKikimr::NOlap::NReader::NSimple {

struct TDistinctScalarPtrEq {
    bool operator()(const std::shared_ptr<arrow::Scalar>& a, const std::shared_ptr<arrow::Scalar>& b) const {
        return a->Equals(*b);
    }
};

class TSyncPointDistinctLimitControl: public ISyncPoint {
private:
    using TBase = ISyncPoint;
    const ui64 Limit;
    const ui32 KeyColumnId;
    /// At most `Limit` distinct keys are tracked; each entry holds one `arrow::Scalar` (typed value).
    std::unordered_set<std::shared_ptr<arrow::Scalar>, arrow::Scalar::Hash, TDistinctScalarPtrEq> Seen;

    virtual bool IsSourcePrepared(const std::shared_ptr<NCommon::IDataSource>& source) const override {
        return source->IsSyncSection() && source->HasStageResult();
    }

    virtual ESourceAction OnSourceReady(const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& /*reader*/) override;

    virtual void DoAbort() override {
        Seen.clear();
    }

    virtual bool IsFinished() const override {
        return Seen.size() >= Limit || TBase::IsFinished();
    }

public:
    TSyncPointDistinctLimitControl(const ui64 limit, const ui32 keyColumnId, const ui32 pointIndex,
        const std::shared_ptr<TSpecialReadContext>& context, const std::shared_ptr<ISourcesCollection>& collection)
        : TBase(pointIndex, "SYNC_DISTINCT_LIMIT", context, collection)
        , Limit(limit)
        , KeyColumnId(keyColumnId)
    {
        AFL_VERIFY(Limit);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
