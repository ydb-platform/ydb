#pragma once
#include "abstract.h"

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TSortedFullScanCollection: public ISourcesCollection {
private:
    using TBase = ISourcesCollection;
    std::deque<TSourceConstructor> HeapSources;
    TPositiveControlInteger InFlightCount;
    ui32 SourceIdx = 0;
    virtual void DoClear() override {
        HeapSources.clear();
    }
    virtual bool DoHasData() const override {
        return HeapSources.size();
    }
    virtual void DoAbort() override {
        HeapSources.clear();
    }
    virtual bool DoIsFinished() const override {
        return HeapSources.empty();
    }
    virtual std::shared_ptr<IScanCursor> DoBuildCursor(const std::shared_ptr<IDataSource>& source, const ui32 readyRecords) const override {
        return std::make_shared<TSimpleScanCursor>(std::make_shared<NArrow::TSimpleRow>(source->GetStartPKRecordBatch()), source->GetSourceId(), readyRecords);
    }
    virtual std::shared_ptr<IDataSource> DoExtractNext() override {
        AFL_VERIFY(HeapSources.size());
        auto result = HeapSources.front().Construct(SourceIdx++, Context);
        std::pop_heap(HeapSources.begin(), HeapSources.end());
        HeapSources.pop_back();
        InFlightCount.Inc();
        return result;
    }
    virtual bool DoCheckInFlightLimits() const override {
        return InFlightCount < GetMaxInFlight();
    }
    virtual void DoOnSourceFinished(const std::shared_ptr<IDataSource>& /*source*/) override {
        InFlightCount.Dec();
    }

public:
    TSortedFullScanCollection(const std::shared_ptr<TSpecialReadContext>& context, std::deque<TSourceConstructor>&& sources,
        const std::shared_ptr<IScanCursor>& cursor)
        : TBase(context) {
        if (cursor && cursor->IsInitialized()) {
            for (auto&& i : sources) {
                bool usage = false;
                if (!context->GetCommonContext()->GetScanCursor()->CheckEntityIsBorder(i, usage)) {
                    continue;
                }
                if (usage) {
                    i.SetIsStartedByCursor();
                }
                break;
            }
        }
        HeapSources = std::move(sources);
        std::make_heap(HeapSources.begin(), HeapSources.end());
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
