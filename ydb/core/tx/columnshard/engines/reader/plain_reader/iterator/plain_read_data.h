#pragma once
#include "columns_set.h"
#include "source.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/queue.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TPlainReadData: public IDataReader, TNonCopyable, NColumnShard::TMonitoringObjectsCounter<TPlainReadData> {
private:
    using TBase = IDataReader;
    std::shared_ptr<TScanHead> Scanner;
    std::shared_ptr<TSpecialReadContext> SpecialReadContext;
    std::vector<TPartialReadResult> PartialResults;
    ui32 ReadyResultsCount = 0;
protected:
    virtual TConclusionStatus DoStart() override {
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

    virtual std::vector<TPartialReadResult> DoExtractReadyResults(const int64_t maxRowsInBatch) override;
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
    virtual void OnSentDataFromInterval(const ui32 intervalIdx) const override {
        Scanner->OnSentDataFromInterval(intervalIdx);
    }

    const TReadMetadata::TConstPtr& GetReadMetadata() const {
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

    void OnIntervalResult(const std::shared_ptr<TPartialReadResult>& result);

    TPlainReadData(const std::shared_ptr<TReadContext>& context);
    ~TPlainReadData() {
        if (!SpecialReadContext->IsAborted()) {
            Abort("unexpected on destructor");
        }
    }
};

}
