#pragma once
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/queue.h>
#include "source.h"
#include "scanner.h"

namespace NKikimr::NOlap::NPlainReader {

class TPlainReadData: public IDataReader, TNonCopyable {
private:
    using TBase = IDataReader;
    std::shared_ptr<TScanHead> Scanner;
    YDB_READONLY_DEF(std::set<ui32>, EFColumnIds);
    YDB_READONLY_DEF(std::set<ui32>, PKColumnIds);
    YDB_READONLY_DEF(std::set<ui32>, FFColumnIds);
    YDB_READONLY_DEF(std::vector<TString>, EFColumnNames);
    YDB_READONLY_DEF(std::vector<TString>, PKColumnNames);
    YDB_READONLY_DEF(std::vector<TString>, FFColumnNames);
    std::vector<TPartialReadResult> PartialResults;
    ui32 ReadyResultsCount = 0;
    TFetchBlobsQueue Queue;
    THashMap<TBlobRange, std::shared_ptr<IDataSource>> Sources;
    bool AbortedFlag = false;
protected:
    virtual TString DoDebugString() const override {
        return TStringBuilder() <<
            "ef_columns=" << JoinSeq(",", EFColumnIds) << ";" <<
            "pk_columns=" << JoinSeq(",", PKColumnIds) << ";" <<
            "ff_columns=" << JoinSeq(",", FFColumnIds) << ";"
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
