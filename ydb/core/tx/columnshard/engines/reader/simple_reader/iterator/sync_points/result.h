#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NSimple {

class TSyncPointResult: public ISyncPoint {
private:
    using TBase = ISyncPoint;
    virtual void DoAbort() override {
    }

    virtual ESourceAction OnSourceReady(const std::shared_ptr<IDataSource>& source, TPlainReadData& reader) override;

public:
    TSyncPointResult(
        const ui32 pointIndex, const std::shared_ptr<TSpecialReadContext>& context, const std::shared_ptr<ISourcesCollection>& collection)
        : TBase(pointIndex, "RESULT", context, collection) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
