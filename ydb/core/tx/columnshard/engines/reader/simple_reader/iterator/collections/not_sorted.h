#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TNotSortedCollection: public ISourcesCollection {
private:
    using TBase = ISourcesCollection;
    std::optional<ui32> Limit;
    ui32 InFlightLimit = 1;

    TPositiveControlInteger InFlightCount;
    ui32 FetchedCount = 0;
    virtual bool DoHasData() const override {
        return !SourcesConstructor->IsFinished();
    }
    virtual void DoClear() override {
        SourcesConstructor->Clear();
    }
    virtual void DoAbort() override {
        SourcesConstructor->Abort();
    }

    virtual std::shared_ptr<IScanCursor> DoBuildCursor(const std::shared_ptr<NCommon::IDataSource>& source, const ui32 readyRecords) const override;
    virtual bool DoIsFinished() const override {
        return SourcesConstructor->IsFinished();
    }
    virtual std::shared_ptr<NCommon::IDataSource> DoExtractNext() override {
        auto result = SourcesConstructor->ExtractNext(Context, InFlightLimit);
        if (!result) {
            return result;
        }
        InFlightCount.Inc();
        return std::move(result);
    }
    virtual bool DoCheckInFlightLimits() const override {
        return InFlightCount < InFlightLimit;
    }
    virtual void DoOnSourceFinished(const std::shared_ptr<NCommon::IDataSource>& source) override {
        if (!source->GetAs<IDataSource>()->GetResultRecordsCount() && InFlightLimit * 2 < GetMaxInFlight()) {
            InFlightLimit *= 2;
        }
        FetchedCount += source->GetAs<IDataSource>()->GetResultRecordsCount();
        if (Limit && *Limit <= FetchedCount) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "limit_exhausted")("limit", Limit)("fetched", FetchedCount);
            SourcesConstructor->Clear();
        }
        InFlightCount.Dec();
    }

public:
    TNotSortedCollection(const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor,
        const std::optional<ui32> limit)
        : TBase(context, std::move(sourcesConstructor))
        , Limit(limit) {
        if (Limit) {
            InFlightLimit = 1;
        } else {
            InFlightLimit = GetMaxInFlight();
        }
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
