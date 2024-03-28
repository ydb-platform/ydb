#pragma once
#include "context.h"
#include "columns_set.h"
#include "fetched_data.h"
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/insert_table/data.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

namespace NKikimr::NOlap {
class IDataReader;
}

namespace NKikimr::NOlap::NReader::NPlain {

class TFetchingInterval;
class TPlainReadData;
class IFetchTaskConstructor;
class IFetchingStep;

class IDataSource {
private:
    YDB_READONLY(ui32, SourceIdx, 0);
    YDB_READONLY_DEF(NIndexedReader::TSortableBatchPosition, Start);
    YDB_READONLY_DEF(NIndexedReader::TSortableBatchPosition, Finish);
    NArrow::TReplaceKey StartReplaceKey;
    NArrow::TReplaceKey FinishReplaceKey;
    YDB_READONLY_DEF(std::shared_ptr<TSpecialReadContext>, Context);
    YDB_READONLY(TSnapshot, RecordSnapshotMax, TSnapshot::Zero());
    std::optional<ui32> RecordsCount;
    YDB_READONLY(ui32, IntervalsCount, 0);
    virtual NJson::TJsonValue DoDebugJson() const = 0;
    bool MergingStartedFlag = false;
    bool AbortedFlag = false;
protected:
    THashMap<ui32, TFetchingInterval*> Intervals;

    std::unique_ptr<TFetchedData> StageData;
    std::unique_ptr<TFetchedResult> StageResult;

    TAtomic FilterStageFlag = 0;
    bool IsReadyFlag = false;

    bool IsAborted() const {
        return AbortedFlag;
    }

    virtual bool DoStartFetchingColumns(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TColumnsSet>& columns) = 0;
    virtual bool DoStartFetchingIndexes(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TIndexesSet>& indexes) = 0;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns) = 0;
    virtual void DoAbort() = 0;
    virtual void DoApplyIndex(const NIndexes::TIndexCheckerContainer& indexMeta) = 0;
public:
    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const = 0;

    virtual bool HasIndexes(const std::set<ui32>& indexIds) const = 0;

    const NArrow::TReplaceKey& GetStartReplaceKey() const {
        return StartReplaceKey;
    }
    const NArrow::TReplaceKey& GetFinishReplaceKey() const {
        return FinishReplaceKey;
    }

    const TFetchedResult& GetStageResult() const {
        AFL_VERIFY(!!StageResult);
        return *StageResult;
    }

    void SetIsReady();

    void Finalize() {
        TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT");
        StageResult = std::make_unique<TFetchedResult>(std::move(StageData));
    }

    bool IsEmptyData() const {
        return GetStageData().IsEmpty();
    }

    void ApplyIndex(const NIndexes::TIndexCheckerContainer& indexMeta) {
        return DoApplyIndex(indexMeta);
    }

    void AssembleColumns(const std::shared_ptr<TColumnsSet>& columns) {
        if (columns->IsEmpty()) {
            return;
        }
        DoAssembleColumns(columns);
    }

    bool StartFetchingColumns(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TColumnsSet>& columns) {
        AFL_VERIFY(columns);
        return DoStartFetchingColumns(sourcePtr, step, columns);
    }

    bool StartFetchingIndexes(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TIndexesSet>& indexes) {
        AFL_VERIFY(indexes);
        return DoStartFetchingIndexes(sourcePtr, step, indexes);
    }
    void InitFetchingPlan(const std::shared_ptr<IFetchingStep>& fetchingFirstStep, const std::shared_ptr<IDataSource>& sourcePtr, const bool isExclusive);

    std::shared_ptr<arrow::RecordBatch> GetLastPK() const {
        return Finish.ExtractSortingPosition();
    }
    void IncIntervalsCount() {
        ++IntervalsCount;
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& columnIds) const = 0;
    virtual ui64 GetIndexRawBytes(const std::set<ui32>& indexIds) const = 0;

    bool IsMergingStarted() const {
        return MergingStartedFlag;
    }

    void StartMerging() {
        Y_ABORT_UNLESS(!MergingStartedFlag);
        MergingStartedFlag = true;
    }

    void Abort() {
        AbortedFlag = true;
        Intervals.clear();
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

    bool IsDataReady() const {
        return IsReadyFlag;
    }

    const TFetchedData& GetStageData() const {
        AFL_VERIFY(StageData);
        return *StageData;
    }

    TFetchedData& MutableStageData() {
        AFL_VERIFY(StageData);
        return *StageData;
    }

    ui32 GetRecordsCount() const {
        AFL_VERIFY(RecordsCount);
        return *RecordsCount;
    }

    void RegisterInterval(TFetchingInterval& interval);

    IDataSource(const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context, 
        const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& finish,
        const TSnapshot& recordSnapshotMax, const std::optional<ui32> recordsCount)
        : SourceIdx(sourceIdx)
        , Start(context->GetReadMetadata()->BuildSortedPosition(start))
        , Finish(context->GetReadMetadata()->BuildSortedPosition(finish))
        , StartReplaceKey(start)
        , FinishReplaceKey(finish)
        , Context(context)
        , RecordSnapshotMax(recordSnapshotMax)
        , RecordsCount(recordsCount)
    {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portions_for_merge")("start", Start.DebugJson())("finish", Finish.DebugJson());
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
    std::shared_ptr<ISnapshotSchema> Schema;

    void NeedFetchColumns(const std::set<ui32>& columnIds,
        TBlobsAction& blobsAction, THashMap<TChunkAddress, ui32>& nullBlocks,
        const std::shared_ptr<NArrow::TColumnFilter>& filter);

    virtual void DoApplyIndex(const NIndexes::TIndexCheckerContainer& indexChecker) override;
    virtual bool DoStartFetchingColumns(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TColumnsSet>& columns) override;
    virtual bool DoStartFetchingIndexes(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TIndexesSet>& indexes) override;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns) override {
        auto blobSchema = GetContext()->GetReadMetadata()->GetLoadSchema(Portion->GetMinSnapshot());
        MutableStageData().AddBatch(Portion->PrepareForAssemble(*blobSchema, columns->GetFilteredSchemaVerified(), MutableStageData().MutableBlobs()).AssembleTable());
    }
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "portion");
        result.InsertValue("info", Portion->DebugString());
        return result;
    }

    virtual void DoAbort() override;
public:
    virtual bool HasIndexes(const std::set<ui32>& indexIds) const override {
        return Portion->HasIndexes(indexIds);
    }

    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const override {
        return Portion->DecodeBlobAddresses(std::move(blobsOriginal), Schema->GetIndexInfo());
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& columnsIds) const override {
        return Portion->GetColumnRawBytes(columnsIds);
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& indexIds) const override {
        return Portion->GetIndexRawBytes(indexIds);
    }

    const TPortionInfo& GetPortionInfo() const {
        return *Portion;
    }

    std::shared_ptr<TPortionInfo> GetPortionInfoPtr() const {
        return Portion;
    }

    TPortionDataSource(const ui32 sourceIdx, const std::shared_ptr<TPortionInfo>& portion, const std::shared_ptr<TSpecialReadContext>& context,
        const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& finish)
        : TBase(sourceIdx, context, start, finish, portion->RecordSnapshotMax(), portion->GetRecordsCount())
        , Portion(portion)
        , Schema(GetContext()->GetReadMetadata()->GetLoadSchema(Portion->GetMinSnapshot()))
    {
    }
};

class TCommittedDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    TCommittedBlob CommittedBlob;
    bool ReadStarted = false;

    virtual void DoAbort() override {

    }

    virtual bool DoStartFetchingColumns(const std::shared_ptr<IDataSource>& sourcePtr, const std::shared_ptr<IFetchingStep>& step, const std::shared_ptr<TColumnsSet>& columns) override;
    virtual bool DoStartFetchingIndexes(const std::shared_ptr<IDataSource>& /*sourcePtr*/, const std::shared_ptr<IFetchingStep>& /*step*/, const std::shared_ptr<TIndexesSet>& /*indexes*/) override {
        return false;
    }
    virtual void DoApplyIndex(const NIndexes::TIndexCheckerContainer& /*indexMeta*/) override {
        return;
    }

    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns) override;
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "commit");
        result.InsertValue("info", CommittedBlob.DebugString());
        return result;
    }
public:
    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const override {
        THashMap<TChunkAddress, TString> result;
        for (auto&& i : blobsOriginal) {
            for (auto&& b : i.second) {
                result.emplace(TChunkAddress(1, 1), std::move(b.second));
            }
        }
        return result;
    }

    virtual bool HasIndexes(const std::set<ui32>& /*indexIds*/) const override {
        return false;
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& /*columnIds*/) const override {
        return CommittedBlob.GetBlobRange().Size;
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& /*columnIds*/) const override {
        AFL_VERIFY(false);
        return 0;
    }

    const TCommittedBlob& GetCommitted() const {
        return CommittedBlob;
    }

    TCommittedDataSource(const ui32 sourceIdx, const TCommittedBlob& committed, const std::shared_ptr<TSpecialReadContext>& context,
        const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& finish)
        : TBase(sourceIdx, context, start, finish, committed.GetSnapshot(), {})
        , CommittedBlob(committed) {

    }
};

}
