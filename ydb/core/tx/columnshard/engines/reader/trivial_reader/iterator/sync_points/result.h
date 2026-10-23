#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NTrivial {

class TSyncPointResult: public ISyncPoint {
private:
    using TBase = ISyncPoint;

    virtual void DoAbort() override {
    }

    virtual ESourceAction OnSourceReady(const NCommon::TDataSourceLease& lease, TPlainReadData& reader) override;
    virtual bool IsSourcePrepared(const NCommon::IDataSource& source) const override;

public:
    TSyncPointResult(
        const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context, const std::shared_ptr<ISourcesCollection>& collection)
        : TBase(pointIndex, "RESULT", context, collection)
    {
        AFL_VERIFY(Collection);
    }
};

}   // namespace NKikimr::NOlap::NReader::NTrivial
