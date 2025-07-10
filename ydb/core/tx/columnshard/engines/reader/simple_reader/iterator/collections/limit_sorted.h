#pragma once
#include "abstract.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>

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
        return !SourcesConstructor->IsFinished();
    }
    std::shared_ptr<IDataSource> NextSource;
    ui64 Limit = 0;
    std::unique_ptr<NCommon::ISourcesConstructor> SourcesConstructor;
    ui64 InFlightLimit = 1;
    std::set<ui32> FetchingInFlightSources;
    bool Aborted = false;
    bool Cleared = false;

    void DrainToLimit();

    virtual std::shared_ptr<IScanCursor> DoBuildCursor(const std::shared_ptr<IDataSource>& source, const ui32 readyRecords) const override {
        return std::make_shared<TSimpleScanCursor>(nullptr, source->GetSourceId(), readyRecords);
    }
    virtual void DoClear() override {
        Cleared = true;
        SourcesConstructor->Clear();
        FetchingInFlightSources.clear();
    }
    virtual void DoAbort() override {
        Aborted = true;
        SourcesConstructor->Abort();
        FetchingInFlightSources.clear();
    }
    virtual bool DoIsFinished() const override {
        return !NextSource && SourcesConstructor->IsFinished() && FetchingInFlightSources.empty();
    }
    virtual std::shared_ptr<IDataSource> DoExtractNext() override;
    virtual bool DoCheckInFlightLimits() const override {
        return FetchingInFlightSources.size() < InFlightLimit;
    }

    virtual void DoOnSourceFinished(const std::shared_ptr<IDataSource>& source) override;
    ui32 GetInFlightIntervalsCount(const TCompareKeyForScanSequence& from, const TCompareKeyForScanSequence& to) const;

public:
    const std::shared_ptr<IDataSource>& GetNextSource() const {
        return NextSource;
    }

    TScanWithLimitCollection(
        const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
