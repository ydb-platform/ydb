#pragma once
#include "abstract.h"

#include <ydb/library/accessor/positive_integer.h>

namespace NKikimr::NOlap::NReader::NSimple {

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

        TFinishedDataSource(const std::shared_ptr<IDataSource>& source, const ui32 partSize)
            : RecordsCount(partSize)
            , SourceId(source->GetSourceId())
            , SourceIdx(source->GetSourceIdx()) {
            AFL_VERIFY(partSize < source->GetResultRecordsCount());
        }
    };

    virtual bool DoHasData() const override {
        return HeapSources.size();
    }
    THashSet<ui32> WaitingToFinish;
    std::shared_ptr<IDataSource> NextSource;
    std::deque<TSourceConstructor> HeapSources;
    ui64 Limit = 0;
    ui64 InFlightLimit = 1;
    ui64 FetchedCount = 0;
    std::set<ui32> FetchingInFlightSources;
    std::optional<ui32> PKPrefixSize;

    virtual bool DoIsSourceReadyForResult(const std::shared_ptr<IDataSource>& source) const override {
        return source->HasStageResult() && (source->GetStageResult().IsEmpty() || source->IsStartedInLimit());
    }


    void DrainToLimit();

    virtual void DoOnSourceCheckLimit(const std::shared_ptr<IDataSource>& source) override;

    virtual std::shared_ptr<IScanCursor> DoBuildCursor(const std::shared_ptr<IDataSource>& source, const ui32 readyRecords) const override {
        return std::make_shared<TSimpleScanCursor>(nullptr, source->GetSourceId(), readyRecords);
    }
    virtual void DoClear() override {
        HeapSources.clear();
        FetchingInFlightSources.clear();
    }
    virtual void DoAbort() override {
        HeapSources.clear();
        FetchingInFlightSources.clear();
    }
    virtual bool DoIsFinished() const override {
        return HeapSources.empty() && FetchingInFlightSources.empty();
    }
    virtual std::shared_ptr<IDataSource> DoExtractNext() override;
    virtual bool DoCheckInFlightLimits() const override {
        return (FetchingInFlightSources.size() < InFlightLimit) || (HeapSources.size() && HeapSources.front().NeedForceToExtract());
    }
    virtual bool DoHasWaitingSources() const override {
        return WaitingToFinish.size() || HeapSources.size() || FetchingInFlightSources.size();
    }

    virtual void DoOnSourceFinished(const std::shared_ptr<IDataSource>& source) override;
    ui32 GetInFlightIntervalsCount(const TCompareKeyForScanSequence& from, const TCompareKeyForScanSequence& to) const;

public:
    TScanWithLimitCollection(const std::shared_ptr<TSpecialReadContext>& context, std::deque<TSourceConstructor>&& sources,
        const std::shared_ptr<IScanCursor>& cursor);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
