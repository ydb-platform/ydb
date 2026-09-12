#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TOrderedResultNoLimitCollection: public ISourcesCollection {
private:
    using TBase = ISourcesCollection;

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

    virtual std::shared_ptr<NArrow::TSimpleRow> DoGetSourceStartPK(const std::shared_ptr<NCommon::IDataSource>& source) const override {
        return std::make_shared<NArrow::TSimpleRow>(source->GetAs<IDataSource>()->GetStartPKRecordBatch());
    }

    virtual std::shared_ptr<NCommon::IDataSource> DoTryExtractNext() override {
        return SourcesConstructor->TryExtractNext(Context, GetMaxInFlight());
    }

    virtual bool DoCheckInFlightLimits() const override {
        return GetSourcesInFlightCount() < GetMaxInFlight();
    }

    virtual void DoOnSourceFinished(const std::shared_ptr<NCommon::IDataSource>& /*source*/) override {
    }

public:
    virtual TString GetClassName() const override {
        return "ORDERED_RESULT_NO_LIMIT";
    }

    TOrderedResultNoLimitCollection(
        const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor)
        : TBase(context, std::move(sourcesConstructor))
    {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
