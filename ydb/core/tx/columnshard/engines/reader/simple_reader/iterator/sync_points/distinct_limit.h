#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NSimple {

class TSyncPointDistinctLimitControl: public ISyncPoint {
private:
    using TBase = ISyncPoint;
    const ui64 Limit;
    const ui32 KeyColumnId;
    THashSet<TString> Seen;

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
    TSyncPointDistinctLimitControl(const ui64 limit, const ui32 keyColumnId, const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context,
        const std::shared_ptr<ISourcesCollection>& collection)
        : TBase(pointIndex, "SYNC_DISTINCT_LIMIT", context, collection)
        , Limit(limit)
        , KeyColumnId(keyColumnId) {
        AFL_VERIFY(Limit);
        AFL_VERIFY(KeyColumnId);
    }
};

} // namespace NKikimr::NOlap::NReader::NSimple
