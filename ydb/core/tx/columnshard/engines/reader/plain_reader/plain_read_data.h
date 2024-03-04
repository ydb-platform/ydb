#pragma once
#include "columns_set.h"
#include "source.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/queue.h>

namespace NKikimr::NOlap::NPlainReader {

class TPlainReadData: public IDataReader, TNonCopyable {
private:
    using TBase = IDataReader;
    std::shared_ptr<TScanHead> Scanner;
    std::shared_ptr<TSpecialReadContext> SpecialReadContext;
    std::vector<TPartialReadResult> PartialResults;
    ui32 ReadyResultsCount = 0;
    bool AbortedFlag = false;
protected:
    virtual TString DoDebugString(const bool verbose) const override {
        TStringBuilder sb;
        sb << SpecialReadContext->DebugString() << ";";
        if (verbose) {
            sb << "intervals_schema=" << Scanner->DebugString();
        }
        return sb;
    }

    virtual std::vector<TPartialReadResult> DoExtractReadyResults(const int64_t /*maxRowsInBatch*/) override;
    virtual bool DoReadNextInterval() override;

    virtual void DoAbort() override {
        AbortedFlag = true;
        Scanner->Abort();
        PartialResults.clear();
        Y_ABORT_UNLESS(IsFinished());
    }
    virtual bool DoIsFinished() const override {
        return (Scanner->IsFinished() && PartialResults.empty());
    }
public:
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

    void OnIntervalResult(const std::shared_ptr<arrow::RecordBatch>& batch, const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& resourcesGuard);

    TPlainReadData(const std::shared_ptr<NOlap::TReadContext>& context);
    ~TPlainReadData() {
        if (!AbortedFlag) {
            Abort();
        }
    }
};

}
