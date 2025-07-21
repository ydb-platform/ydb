#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TSortedFullScanCollection: public ISourcesCollection {
private:
    using TBase = ISourcesCollection;
    std::unique_ptr<NCommon::ISourcesConstructor> SourcesConstructor;
    TPositiveControlInteger InFlightCount;
    virtual void DoClear() override {
        SourcesConstructor->Clear();
    }
    virtual bool DoHasData() const override {
        return !SourcesConstructor->IsFinished();
    }
    virtual void DoAbort() override {
        SourcesConstructor->Abort();
    }
    virtual bool DoIsFinished() const override {
        return SourcesConstructor->IsFinished();
    }
    virtual std::shared_ptr<IScanCursor> DoBuildCursor(const std::shared_ptr<NCommon::IDataSource>& source, const ui32 readyRecords) const override {
        return std::make_shared<TSimpleScanCursor>(
            std::make_shared<NArrow::TSimpleRow>(source->GetAs<IDataSource>()->GetStartPKRecordBatch()), source->GetSourceId(), readyRecords);
    }
    virtual std::shared_ptr<NCommon::IDataSource> DoExtractNext() override {
        auto result = SourcesConstructor->ExtractNext(Context);
        InFlightCount.Inc();
        return std::move(result);
    }
    virtual bool DoCheckInFlightLimits() const override {
        return InFlightCount < GetMaxInFlight();
    }
    virtual void DoOnSourceFinished(const std::shared_ptr<NCommon::IDataSource>& /*source*/) override {
        InFlightCount.Dec();
    }

public:
    TSortedFullScanCollection(
        const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor)
        : TBase(context)
        , SourcesConstructor(std::move(sourcesConstructor)) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
