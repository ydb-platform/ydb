#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NReader::NSimple {

class TNotSortedCollection: public ISourcesCollection {
private:
    using TBase = ISourcesCollection;
    std::optional<ui32> Limit;
    ui32 InFlightLimit = 1;
    std::deque<TSourceConstructor> Sources;
    TPositiveControlInteger InFlightCount;
    ui32 FetchedCount = 0;
    ui32 SourceIdx = 0;
    virtual bool DoHasData() const override {
        return Sources.size();
    }
    virtual void DoClear() override {
        Sources.clear();
    }
    virtual void DoAbort() override {
        Sources.clear();
    }

    virtual std::shared_ptr<IScanCursor> DoBuildCursor(const std::shared_ptr<IDataSource>& source, const ui32 readyRecords) const override;
    virtual bool DoIsFinished() const override {
        return Sources.empty();
    }
    virtual std::shared_ptr<IDataSource> DoExtractNext() override {
        AFL_VERIFY(Sources.size());
        auto result = Sources.front().Construct(SourceIdx++, Context);
        Sources.pop_front();
        InFlightCount.Inc();
        return result;
    }
    virtual bool DoCheckInFlightLimits() const override {
        return InFlightCount < InFlightLimit;
    }
    virtual void DoOnSourceFinished(const std::shared_ptr<IDataSource>& source) override {
        if (!source->GetResultRecordsCount() && InFlightLimit * 2 < GetMaxInFlight()) {
            InFlightLimit *= 2;
        }
        FetchedCount += source->GetResultRecordsCount();
        if (Limit && *Limit <= FetchedCount && Sources.size()) {
            AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "limit_exhausted")("limit", Limit)("fetched", FetchedCount);
            Sources.clear();
        }
        InFlightCount.Dec();
    }

public:
    TNotSortedCollection(const std::shared_ptr<TSpecialReadContext>& context, std::deque<TSourceConstructor>&& sources,
        const std::shared_ptr<IScanCursor>& cursor, const std::optional<ui32> limit)
        : TBase(context)
        , Limit(limit) {
        if (Limit) {
            InFlightLimit = 1;
        } else {
            InFlightLimit = GetMaxInFlight();
        }
        if (cursor && cursor->IsInitialized()) {
            while (sources.size()) {
                bool usage = false;
                if (!context->GetCommonContext()->GetScanCursor()->CheckEntityIsBorder(sources.front(), usage)) {
                    sources.pop_front();
                    continue;
                }
                if (usage) {
                    sources.front().SetIsStartedByCursor();
                } else {
                    sources.pop_front();
                }
                break;
            }
        }
        Sources = std::move(sources);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
