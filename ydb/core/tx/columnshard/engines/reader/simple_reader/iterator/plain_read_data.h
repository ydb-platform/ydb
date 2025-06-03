#pragma once
#include "scanner.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/queue.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/columns_set.h>

namespace NKikimr::NOlap::NReader::NSimple {

class TPlainReadData: public IDataReader, TNonCopyable, NColumnShard::TMonitoringObjectsCounter<TPlainReadData> {
private:
    using TBase = IDataReader;
    std::shared_ptr<TScanHead> Scanner;
    std::shared_ptr<TSpecialReadContext> SpecialReadContext;
    std::vector<std::shared_ptr<TPartialReadResult>> PartialResults;
    ui32 ReadyResultsCount = 0;

protected:
    virtual TConclusionStatus DoStart() override {
        SpecialReadContext->RegisterActors();
        return Scanner->Start();
    }

    virtual TString DoDebugString(const bool verbose) const override {
        TStringBuilder sb;
        sb << SpecialReadContext->DebugString() << ";";
        if (verbose) {
            sb << "intervals_schema=" << Scanner->DebugString();
        }
        return sb;
    }

    virtual std::vector<std::shared_ptr<TPartialReadResult>> DoExtractReadyResults(const int64_t maxRowsInBatch) override;
    virtual TConclusion<bool> DoReadNextInterval() override;

    virtual void DoAbort() override {
        SpecialReadContext->Abort();
        Scanner->Abort();
        PartialResults.clear();
        Y_ABORT_UNLESS(IsFinished());
    }
    virtual bool DoIsFinished() const override {
        return (Scanner->IsFinished() && PartialResults.empty());
    }

public:
    const NCommon::TReadMetadata::TConstPtr& GetReadMetadata() const {
        return SpecialReadContext->GetReadMetadata();
    }

    const std::shared_ptr<TSpecialReadContext>& GetSpecialReadContext() const {
        return SpecialReadContext;
    }

    const TScanHead& GetScanner() const {
        return *Scanner;
    }

    TScanHead& MutableScanner() {
        return *Scanner;
    }
    virtual void OnSentDataFromInterval(const TPartialSourceAddress& sourceAddress) override;

    void OnIntervalResult(const std::shared_ptr<TPartialReadResult>& result);

    TPlainReadData(const std::shared_ptr<TReadContext>& context);
    ~TPlainReadData() {
        if (SpecialReadContext->IsActive()) {
            Abort("unexpected on destructor");
        }
        SpecialReadContext->UnregisterActors();
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
