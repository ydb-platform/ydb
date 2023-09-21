#pragma once
#include "fetched_data.h"
#include "columns_set.h"
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

class TFetchingPlan {
private:
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FilterStage);
    YDB_READONLY_DEF(std::shared_ptr<TColumnsSet>, FetchingStage);
    bool CanUseEarlyFilterImmediatelyFlag = false;
public:
    TFetchingPlan(const std::shared_ptr<TColumnsSet>& filterStage, const std::shared_ptr<TColumnsSet>& fetchingStage, const bool canUseEarlyFilterImmediately)
        : FilterStage(filterStage)
        , FetchingStage(fetchingStage)
        , CanUseEarlyFilterImmediatelyFlag(canUseEarlyFilterImmediately)
    {

    }

    bool CanUseEarlyFilterImmediately() const {
        return CanUseEarlyFilterImmediatelyFlag;
    }
};

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

    std::shared_ptr<TFilterStageData> FilterStageData;
    std::shared_ptr<TFetchStageData> FetchStageData;

    std::optional<TFetchingPlan> FetchingPlan;

    bool FilterStageFlag = false;

    virtual void DoStartFilterStage() = 0;
    virtual void DoStartFetchStage() = 0;
    virtual void DoAbort() = 0;

public:
    const TFetchingPlan& GetFetchingPlan() const {
        Y_VERIFY(FetchingPlan);
        return *FetchingPlan;
    }

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
        Y_VERIFY(FilterStageData);
        return *FilterStageData;
    }

    void InitFetchingPlan(const TFetchingPlan& fetchingPlan);

    void InitFilterStageData(const std::shared_ptr<NArrow::TColumnFilter>& appliedFilter, const std::shared_ptr<NArrow::TColumnFilter>& earlyFilter, const std::shared_ptr<arrow::RecordBatch>& batch);
    void InitFetchStageData(const std::shared_ptr<arrow::RecordBatch>& batch);

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

    virtual ~IDataSource() {
        Y_VERIFY(Intervals.empty());
    }
};

class TPortionDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    std::shared_ptr<TPortionInfo> Portion;

    void NeedFetchColumns(const std::set<ui32>& columnIds,
        const std::shared_ptr<IBlobsReadingAction>& readingAction, THashMap<TBlobRange, ui32>& nullBlocks,
        const std::shared_ptr<NArrow::TColumnFilter>& filter);

    virtual void DoStartFilterStage() override;
    virtual void DoStartFetchStage() override;
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

    virtual void DoStartFilterStage() override {
        DoFetch();
    }

    virtual void DoStartFetchStage() override {
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
};

}
