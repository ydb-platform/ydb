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
        YDB_READONLY(ui32, SourceIdx, 0);

    public:
        TFinishedDataSource(const std::shared_ptr<IDataSource>& source)
            : RecordsCount(source->GetResultRecordsCount())
            , SourceIdx(source->GetSourceIdx())
        {
        }

        TFinishedDataSource(const std::shared_ptr<IDataSource>& source, const ui32 partSize)
            : RecordsCount(partSize)
            , SourceIdx(source->GetSourceIdx())
        {
            AFL_VERIFY(partSize < source->GetResultRecordsCount());
        }
    };

    virtual bool DoHasData() const override {
        return !SourcesConstructor->IsFinished() || !!NextSource;
    }
    std::shared_ptr<NCommon::IDataSource> NextSource;
    ui64 Limit = 0;

    ui64 InFlightLimit = 16;
    std::set<ui32> FetchingInFlightSources;
    bool Aborted = false;
    bool Cleared = false;

    void DrainToLimit();

    virtual std::shared_ptr<IScanCursor> DoBuildCursor(
        const std::shared_ptr<NCommon::IDataSource>& source, const ui32 readyRecords) const override {
        if (AppDataVerified().ColumnShardConfig.GetEnableCursorV1()) {
            return std::make_shared<TSimpleScanCursor>(nullptr, source->GetSourceIdx(), readyRecords, source->GetPortionIdOptional());
        } else {
            return std::make_shared<TDeprecatedSimpleScanCursor>(nullptr, source->GetDeprecatedPortionId(), readyRecords);
        }
    }
    virtual void DoClear() override {
        Cleared = true;
        SourcesConstructor->Clear();
        FetchingInFlightSources.clear();
        NextSource.reset();
    }
    virtual void DoAbort() override {
        Aborted = true;
        SourcesConstructor->Abort();
        FetchingInFlightSources.clear();
        NextSource.reset();
    }
    virtual TString DoDebugString() const override {
        TStringBuilder sb;
        sb << "{";
        sb << "N:" << (NextSource ? true : false) << ";";
        if (Cleared) {
            sb << "C:" << Cleared << ";";
        }
        if (Aborted) {
            sb << "A:" << Aborted << ";";
        }
        sb << "SCF:" << SourcesConstructor->IsFinished() << ";";
        sb << "FFS:" << FetchingInFlightSources.size() << ";";
        sb << "IN_FLY:" << GetSourcesInFlightCount() << ";";
        sb << "HAS_DATA:" << HasData() << ";";
        sb << "}";
        return sb;
    }
    virtual bool DoIsFinished() const override {
        return !NextSource && SourcesConstructor->IsFinished() && FetchingInFlightSources.empty();
    }
    virtual std::shared_ptr<NCommon::IDataSource> DoTryExtractNext() override;
    virtual bool DoCheckInFlightLimits() const override {
        return GetSourcesInFlightCount() < InFlightLimit;
    }

    virtual void DoOnSourceFinished(const std::shared_ptr<NCommon::IDataSource>& source) override;
    ui32 GetInFlightIntervalsCount(const TCompareKeyForScanSequence& from, const TCompareKeyForScanSequence& to) const;

public:
    virtual TString GetClassName() const override {
        return "SORT_LIMIT";
    }

    const std::shared_ptr<NCommon::IDataSource>& GetNextSource() const {
        return NextSource;
    }

    TScanWithLimitCollection(
        const std::shared_ptr<TSpecialReadContext>& context, std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
