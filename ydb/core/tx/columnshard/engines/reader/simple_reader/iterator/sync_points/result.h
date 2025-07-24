#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NSimple {

class TSyncPointResult: public ISyncPoint {
private:
    using TBase = ISyncPoint;
    virtual void DoAbort() override {
    }

    virtual ESourceAction OnSourceReady(const std::shared_ptr<NCommon::IDataSource>& source, TPlainReadData& reader) override;
    virtual bool IsSourcePrepared(const std::shared_ptr<NCommon::IDataSource>& source) const override;

public:
    TSyncPointResult(
        const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context, const std::shared_ptr<ISourcesCollection>& collection)
        : TBase(pointIndex, "RESULT", context, collection) {
        AFL_VERIFY(Collection);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
