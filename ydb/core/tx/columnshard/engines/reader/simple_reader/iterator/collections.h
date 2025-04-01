#pragma once
#include "context.h"
#include "source.h"

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NReader::NSimple {

class ISourcesCollection {
private:
    virtual bool DoIsFinished() const = 0;
    virtual std::shared_ptr<IDataSource> DoExtractNext() = 0;
    virtual bool DoCheckInFlightLimits() const = 0;
    virtual void DoOnSourceFinished(const std::shared_ptr<IDataSource>& source) = 0;
    virtual void DoClear() = 0;

    TPositiveControlInteger SourcesInFlightCount;
    YDB_READONLY(ui64, MaxInFlight, 1024);

    virtual TString DoDebugString() const {
        return "";
    }

protected:
    const std::shared_ptr<TSpecialReadContext> Context;

public:
    TString DebugString() const {
        return DoDebugString();
    }

    virtual ~ISourcesCollection() = default;

    std::shared_ptr<IDataSource> ExtractNext() {
        SourcesInFlightCount.Inc();
        return DoExtractNext();
    }

    bool IsFinished() const {
        return DoIsFinished();
    }

    void OnSourceFinished(const std::shared_ptr<IDataSource>& source) {
        SourcesInFlightCount.Dec();
        DoOnSourceFinished(source);
    }

    bool CheckInFlightLimits() const {
        return DoCheckInFlightLimits();
    }

    void Clear() {
        DoClear();
    }

    ISourcesCollection(const std::shared_ptr<TSpecialReadContext>& context);
};

class TFullScanCollection: public ISourcesCollection {
private:
    using TBase = ISourcesCollection;
    std::deque<TSourceConstructor> HeapSources;
    TPositiveControlInteger InFlightCount;
    virtual void DoClear() override {
        HeapSources.clear();
    }
    virtual bool DoIsFinished() const override {
        return HeapSources.empty();
    }
    virtual std::shared_ptr<IDataSource> DoExtractNext() override {
        AFL_VERIFY(HeapSources.size());
        auto result = HeapSources.front().Construct(Context);
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
    TFullScanCollection(const std::shared_ptr<TSpecialReadContext>& context, std::deque<TSourceConstructor>&& sources)
        : TBase(context) {
        HeapSources = std::move(sources);
        std::make_heap(HeapSources.begin(), HeapSources.end());
    }
};

class TScanWithLimitCollection: public ISourcesCollection {
private:
    using TBase = ISourcesCollection;
    class TFinishedDataSource {
    private:
        YDB_READONLY(ui32, RecordsCount, 0);
        YDB_READONLY(ui32, SourceId, 0);
        YDB_READONLY(ui32, SourceIdx, 0);

    public:
        TFinishedDataSource(const std::shared_ptr<IDataSource>& source)
            : RecordsCount(source->GetResultRecordsCount())
            , SourceId(source->GetSourceId())
            , SourceIdx(source->GetSourceIdx()) {
        }
    };

    std::deque<TSourceConstructor> HeapSources;
    TPositiveControlInteger FetchingInFlightCount;
    TPositiveControlInteger FullIntervalsFetchingCount;
    ui64 Limit = 0;
    ui64 InFlightLimit = 1;
    ui64 FetchedCount = 0;
    std::map<TCompareKeyForScanSequence, TFinishedDataSource> FinishedSources;
    std::set<TCompareKeyForScanSequence> FetchingInFlightSources;

    virtual void DoClear() override {
        HeapSources.clear();
    }
    virtual bool DoIsFinished() const override {
        return HeapSources.empty();
    }
    virtual std::shared_ptr<IDataSource> DoExtractNext() override;
    virtual bool DoCheckInFlightLimits() const override {
        return (FetchingInFlightCount < GetMaxInFlight()) && (FullIntervalsFetchingCount < InFlightLimit);
    }
    virtual void DoOnSourceFinished(const std::shared_ptr<IDataSource>& source) override;
    ui32 GetInFlightIntervalsCount(const TCompareKeyForScanSequence& from, const TCompareKeyForScanSequence& to) const;

public:
    TScanWithLimitCollection(const std::shared_ptr<TSpecialReadContext>& context, std::deque<TSourceConstructor>&& sources);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
