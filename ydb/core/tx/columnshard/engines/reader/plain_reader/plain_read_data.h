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
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, EFColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, PKColumns);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FFColumns);
    std::shared_ptr<TColumnsSet> EmptyColumns = std::make_shared<TColumnsSet>();
    std::shared_ptr<TColumnsSet> PKFFColumns;
    std::shared_ptr<TColumnsSet> EFPKColumns;
    std::shared_ptr<TColumnsSet> FFMinusEFColumns;
    std::shared_ptr<TColumnsSet> FFMinusEFPKColumns;
    const bool TrivialEFFlag = false;
    std::vector<TPartialReadResult> PartialResults;
    ui32 ReadyResultsCount = 0;
    TFetchBlobsQueue Queue;
    THashMap<TBlobRange, std::shared_ptr<IDataSource>> Sources;
    bool AbortedFlag = false;
protected:
    virtual TString DoDebugString() const override {
        return TStringBuilder() <<
            "ef=" << EFColumns->DebugString() << ";" <<
            "pk=" << PKColumns->DebugString() << ";" <<
            "ff=" << FFColumns->DebugString() << ";"
            ;
    }

    virtual std::vector<TPartialReadResult> DoExtractReadyResults(const int64_t /*maxRowsInBatch*/) override;

    virtual void DoAbort() override {
        AbortedFlag = true;
        Scanner->Abort();
        PartialResults.clear();
        Y_VERIFY(IsFinished());
    }
    virtual bool DoIsFinished() const override {
        return (Scanner->IsFinished() && PartialResults.empty());
    }

    virtual void DoAddData(const TBlobRange& blobRange, const TString& data) override;
    virtual std::optional<TBlobRange> DoExtractNextBlob(const bool hasReadyResults) override;
public:
    TFetchingPlan GetColumnsFetchingPlan(const bool exclusiveSource) const;

    IDataSource& GetSourceByIdxVerified(const ui32 sourceIdx) {
        return *Scanner->GetSourceVerified(sourceIdx);
    }

    void AddBlobForFetch(const ui64 objectId, const TBlobRange& bRange) {
        Queue.emplace_back(objectId, bRange);
        Y_VERIFY(Sources.emplace(bRange, Scanner->GetSourceVerified(objectId)).second);
    }

    void OnIntervalResult(std::shared_ptr<arrow::RecordBatch> batch);

    TPlainReadData(TReadMetadata::TConstPtr readMetadata, const TReadContext& context);
    ~TPlainReadData() {
        if (!AbortedFlag) {
            Abort();
        }
    }
};

}
