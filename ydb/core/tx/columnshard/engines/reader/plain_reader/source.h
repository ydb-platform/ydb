#pragma once
#include "context.h"
#include "columns_set.h"
#include "fetched_data.h"
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
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
    YDB_READONLY_DEF(std::shared_ptr<TSpecialReadContext>, Context);
    YDB_READONLY(ui32, IntervalsCount, 0);
    virtual NJson::TJsonValue DoDebugJson() const = 0;
    bool MergingStartedFlag = false;
    bool AbortedFlag = false;
protected:
    THashMap<ui32, TFetchingInterval*> Intervals;

    std::shared_ptr<TFilterStageData> FilterStageData;
    std::shared_ptr<TFetchStageData> FetchStageData;

    std::optional<TFetchingPlan> FetchingPlan;

    TAtomic FilterStageFlag = 0;

    bool IsAborted() const {
        return AbortedFlag;
    }

    virtual void DoStartFilterStage(const std::shared_ptr<IDataSource>& sourcePtr) = 0;
    virtual void DoStartFetchStage(const std::shared_ptr<IDataSource>& sourcePtr) = 0;
    virtual void DoAbort() = 0;
public:
    void IncIntervalsCount() {
        ++IntervalsCount;
    }

    virtual ui64 GetRawBytes(const std::set<ui32>& columnIds) const = 0;

    const TFetchingPlan& GetFetchingPlan() const {
        Y_ABORT_UNLESS(FetchingPlan);
        return *FetchingPlan;
    }

    bool IsMergingStarted() const {
        return MergingStartedFlag;
    }

    void StartMerging() {
        Y_ABORT_UNLESS(!MergingStartedFlag);
        MergingStartedFlag = true;
    }

    void Abort() {
        AbortedFlag = true;
        DoAbort();
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
        if (!FilterStageData || !FetchStageData) {
            return nullptr;
        }
        return NArrow::MergeColumns({FilterStageData->GetBatch(), FetchStageData->GetBatch()});
    }

    bool IsDataReady() const {
        return !!FilterStageData && !!FetchStageData;
    }

    const TFilterStageData& GetFilterStageData() const {
        Y_ABORT_UNLESS(FilterStageData);
        return *FilterStageData;
    }

    void InitFetchingPlan(const TFetchingPlan& fetchingPlan, const std::shared_ptr<IDataSource>& sourcePtr);

    void InitFilterStageData(const std::shared_ptr<NArrow::TColumnFilter>& appliedFilter, const std::shared_ptr<NArrow::TColumnFilter>& earlyFilter, const std::shared_ptr<arrow::RecordBatch>& batch
        , const std::shared_ptr<IDataSource>& sourcePtr);
    void InitFetchStageData(const std::shared_ptr<arrow::RecordBatch>& batch);

    void RegisterInterval(TFetchingInterval* interval);

    IDataSource(const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context, const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish)
        : SourceIdx(sourceIdx)
        , Start(start)
        , Finish(finish)
        , Context(context)
    {
        if (Start.IsReverseSort()) {
            std::swap(Start, Finish);
        }
        Y_ABORT_UNLESS(Start.Compare(Finish) != std::partial_ordering::greater);
    }

    virtual ~IDataSource() {
        Y_ABORT_UNLESS(AbortedFlag || Intervals.empty());
    }
};

class TPortionDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    std::shared_ptr<TPortionInfo> Portion;

    void NeedFetchColumns(const std::set<ui32>& columnIds,
        const std::shared_ptr<IBlobsReadingAction>& readingAction, THashMap<TBlobRange, ui32>& nullBlocks,
        const std::shared_ptr<NArrow::TColumnFilter>& filter);

    virtual void DoStartFilterStage(const std::shared_ptr<IDataSource>& sourcePtr) override;
    virtual void DoStartFetchStage(const std::shared_ptr<IDataSource>& sourcePtr) override;
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "portion");
        result.InsertValue("info", Portion->DebugString());
        return result;
    }

    virtual void DoAbort() override;
public:
    virtual ui64 GetRawBytes(const std::set<ui32>& columnIds) const override {
        return Portion->GetRawBytes(columnIds);
    }

    const TPortionInfo& GetPortionInfo() const {
        return *Portion;
    }

    std::shared_ptr<TPortionInfo> GetPortionInfoPtr() const {
        return Portion;
    }

    TPortionDataSource(const ui32 sourceIdx, const std::shared_ptr<TPortionInfo>& portion, const std::shared_ptr<TSpecialReadContext>& context,
        const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish)
        : TBase(sourceIdx, context, start, finish)
        , Portion(portion) {

    }
};

class TCommittedDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    TCommittedBlob CommittedBlob;
    bool ReadStarted = false;
    bool ResultReady = false;

    void DoFetch(const std::shared_ptr<IDataSource>& sourcePtr);
    virtual void DoAbort() override {

    }

    virtual void DoStartFilterStage(const std::shared_ptr<IDataSource>& sourcePtr) override {
        DoFetch(sourcePtr);
    }

    virtual void DoStartFetchStage(const std::shared_ptr<IDataSource>& sourcePtr) override {
        DoFetch(sourcePtr);
    }
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "commit");
        result.InsertValue("info", CommittedBlob.DebugString());
        return result;
    }
public:
    virtual ui64 GetRawBytes(const std::set<ui32>& /*columnIds*/) const override {
        return CommittedBlob.GetBlobRange().Size;
    }

    const TCommittedBlob& GetCommitted() const {
        return CommittedBlob;
    }

    TCommittedDataSource(const ui32 sourceIdx, const TCommittedBlob& committed, const std::shared_ptr<TSpecialReadContext>& context,
        const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish)
        : TBase(sourceIdx, context, start, finish)
        , CommittedBlob(committed) {

    }
};

}
