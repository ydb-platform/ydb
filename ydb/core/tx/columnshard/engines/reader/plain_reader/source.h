#pragma once
#include "fetched_data.h"
#include <ydb/core/tx/columnshard/engines/reader/read_filter_merger.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/insert_table/data.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

namespace NKikimr::NOlap {
class IDataReader;
}

namespace NKikimr::NOlap::NPlainReader {

class TFetchingInterval;
class TPlainReadData;
class IFetchTaskConstructor;

class IDataSource {
private:
    YDB_READONLY(ui32, SourceIdx, 0);
    YDB_READONLY_DEF(NIndexedReader::TSortableBatchPosition, Start);
    YDB_READONLY_DEF(NIndexedReader::TSortableBatchPosition, Finish);
    virtual NJson::TJsonValue DoDebugJson() const = 0;
    bool MergingStartedFlag = false;
protected:
    TPlainReadData& ReadData;
    std::deque<TFetchingInterval*> Intervals;

    // EF (EarlyFilter)->PK (PrimaryKey)->FF (FullyFetched)
    std::shared_ptr<TEarlyFilterData> EFData;
    std::shared_ptr<TPrimaryKeyData> PKData;
    std::shared_ptr<TFullData> FFData;

    bool NeedEFFlag = false;
    bool NeedPKFlag = false;
    bool NeedFFFlag = false;

    virtual void DoFetchEF() = 0;
    virtual void DoFetchPK() = 0;
    virtual void DoFetchFF() = 0;
    virtual void DoAbort() = 0;

public:
    bool IsMergingStarted() const {
        return MergingStartedFlag;
    }

    void StartMerging() {
        Y_VERIFY(!MergingStartedFlag);
        MergingStartedFlag = true;
    }

    void Abort() {
        DoAbort();
    }

    bool IsScannersFinished() const {
        return Intervals.empty();
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("source_idx", SourceIdx);
        result.InsertValue("start", Start.DebugJson());
        result.InsertValue("finish", Finish.DebugJson());
        result.InsertValue("specific", DoDebugJson());
        return result;
    }

    bool OnIntervalFinished(const ui32 intervalIdx);

    std::shared_ptr<arrow::RecordBatch> GetBatch() const {
        if (!EFData || !PKData || !FFData) {
            return nullptr;
        }
        return NArrow::MergeColumns({EFData->GetBatch(), PKData->GetBatch(), FFData->GetBatch()});
    }

    bool HasFFData() const {
        return HasPKData() && !!FFData;
    }

    bool HasPKData() const {
        return HasEFData() && !!PKData;
    }

    bool HasEFData() const {
        return !!EFData;
    }

    const TEarlyFilterData& GetEFData() const {
        Y_VERIFY(EFData);
        return *EFData;
    }

    const TPrimaryKeyData& GetPKData() const {
        Y_VERIFY(PKData);
        return *PKData;
    }

    void NeedEF();
    void NeedPK();
    void NeedFF();

    void InitEF(const std::shared_ptr<NArrow::TColumnFilter>& appliedFilter, const std::shared_ptr<NArrow::TColumnFilter>& earlyFilter, const std::shared_ptr<arrow::RecordBatch>& batch);
    void InitFF(const std::shared_ptr<arrow::RecordBatch>& batch);
    void InitPK(const std::shared_ptr<arrow::RecordBatch>& batch);

    void RegisterInterval(TFetchingInterval* interval) {
        Intervals.emplace_back(interval);
    }

    IDataSource(const ui32 sourceIdx, TPlainReadData& readData, const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish)
        : SourceIdx(sourceIdx)
        , Start(start)
        , Finish(finish)
        , ReadData(readData)
    {
        if (Start.IsReverseSort()) {
            std::swap(Start, Finish);
        }
        Y_VERIFY(Start.Compare(Finish) != std::partial_ordering::greater);
    }

    virtual ~IDataSource() = default;
    virtual void AddData(const TBlobRange& range, TString&& data) = 0;
};

class TPortionDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    std::shared_ptr<TPortionInfo> Portion;
    THashMap<TBlobRange, std::shared_ptr<IFetchTaskConstructor>> BlobsWaiting;

    void NeedFetchColumns(const std::set<ui32>& columnIds, std::shared_ptr<IFetchTaskConstructor> constructor);

    virtual void DoFetchEF() override;
    virtual void DoFetchPK() override;
    virtual void DoFetchFF() override;
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "portion");
        result.InsertValue("info", Portion->DebugString());
        return result;
    }

    virtual void DoAbort() override;
public:
    const TPortionInfo& GetPortionInfo() const {
        return *Portion;
    }

    std::shared_ptr<TPortionInfo> GetPortionInfoPtr() const {
        return Portion;
    }

    TPortionDataSource(const ui32 sourceIdx, const std::shared_ptr<TPortionInfo>& portion, TPlainReadData& reader,
        const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish)
        : TBase(sourceIdx, reader, start, finish)
        , Portion(portion) {

    }

    virtual void AddData(const TBlobRange& range, TString&& data) override;
};

class TCommittedDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    TCommittedBlob CommittedBlob;
    bool ReadStarted = false;
    bool ResultReady = false;

    void DoFetch();
    virtual void DoAbort() override {

    }

    virtual void DoFetchEF() override {
        DoFetch();
    }

    virtual void DoFetchPK() override {
        DoFetch();
    }

    virtual void DoFetchFF() override {
        DoFetch();
    }
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "commit");
        result.InsertValue("info", CommittedBlob.DebugString());
        return result;
    }
public:
    const TCommittedBlob& GetCommitted() const {
        return CommittedBlob;
    }

    TCommittedDataSource(const ui32 sourceIdx, const TCommittedBlob& committed, TPlainReadData& reader,
        const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish)
        : TBase(sourceIdx, reader, start, finish)
        , CommittedBlob(committed) {

    }

    virtual void AddData(const TBlobRange& range, TString&& data) override;
};

}
