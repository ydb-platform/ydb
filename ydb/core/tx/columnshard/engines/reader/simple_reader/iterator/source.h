#pragma once
#include "context.h"
#include "fetched_data.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/predicate/range.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/columns_set.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/filtered_scheme.h>
#include <ydb/core/tx/columnshard/resource_subscriber/task.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <util/string/join.h>

namespace NKikimr::NOlap {
class IDataReader;
}

namespace NKikimr::NOlap::NReader::NSimple {

class TFetchingInterval;
class TPlainReadData;
class IFetchTaskConstructor;
class IFetchingStep;
class TBuildFakeSpec;

class TPortionPage {
private:
    YDB_READONLY(ui32, StartIndex, 0);
    YDB_READONLY(ui32, RecordsCount, 0);
    YDB_READONLY(ui64, MemoryBytes, 0);
    YDB_ACCESSOR_DEF(std::shared_ptr<arrow::Table>, Result);

public:
    TPortionPage(const ui32 startIndex, const ui32 recordsCount, const ui64 memoryBytes)
        : StartIndex(startIndex)
        , RecordsCount(recordsCount)
        , MemoryBytes(memoryBytes) {
    }
};

class TReplaceKeyAdapter {
private:
    const bool Reverse = false;
    const NArrow::TReplaceKey Value;

public:
    TReplaceKeyAdapter(const NArrow::TReplaceKey& rk, const bool reverse)
        : Reverse(reverse)
        , Value(rk) {
    }

    std::partial_ordering Compare(const TReplaceKeyAdapter& item) const {
        AFL_VERIFY(Reverse == item.Reverse);
        const std::partial_ordering result = Value.CompareNotNull(item.Value);
        if (result == std::partial_ordering::equivalent) {
            return std::partial_ordering::equivalent;
        } else if (result == std::partial_ordering::less) {
            return Reverse ? std::partial_ordering::greater : std::partial_ordering::less;
        } else if (result == std::partial_ordering::greater) {
            return Reverse ? std::partial_ordering::less : std::partial_ordering::greater;
        } else {
            AFL_VERIFY(false);
            return std::partial_ordering::less;
        }
    }

    bool operator<(const TReplaceKeyAdapter& item) const {
        AFL_VERIFY(Reverse == item.Reverse);
        const std::partial_ordering result = Value.CompareNotNull(item.Value);
        if (result == std::partial_ordering::equivalent) {
            return false;
        } else if (result == std::partial_ordering::less) {
            return !Reverse;
        } else if (result == std::partial_ordering::greater) {
            return Reverse;
        } else {
            AFL_VERIFY(false);
            return false;
        }
    }

    TString DebugString() const {
        return TStringBuilder() << "point:{" << Value.DebugString() << "};reverse:" << Reverse << ";";
    }
};

class IDataSource: public NCommon::IDataSource {
private:
    using TBase = NCommon::IDataSource;
    const TReplaceKeyAdapter Start;
    const TReplaceKeyAdapter Finish;
    virtual NJson::TJsonValue DoDebugJson() const = 0;
    std::shared_ptr<TFetchingScript> FetchingPlan;
    YDB_READONLY(TPKRangeFilter::EUsageClass, UsageClass, TPKRangeFilter::EUsageClass::PartialUsage);
    YDB_ACCESSOR(ui32, ResultRecordsCount, 0);
    bool ProcessingStarted = false;
    bool IsStartedByCursor = false;
    friend class TBuildFakeSpec;

    std::optional<TFetchingScriptCursor> ScriptCursor;
    std::shared_ptr<NGroupedMemoryManager::TGroupGuard> SourceGroupGuard;

    virtual void DoOnSourceFetchingFinishedSafe(IDataReader& owner, const std::shared_ptr<NCommon::IDataSource>& sourcePtr) override;
    virtual void DoBuildStageResult(const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) override;
    virtual void DoOnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) override;

    void Finalize(const std::optional<ui64> memoryLimit);

protected:
    std::optional<ui64> UsedRawBytes;

    virtual void DoAbort() = 0;
    virtual NJson::TJsonValue DoDebugJsonForMemory() const {
        return NJson::JSON_MAP;
    }
    virtual bool DoStartFetchingAccessor(const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step) = 0;

public:
    virtual void InitUsedRawBytes() = 0;

    ui64 GetUsedRawBytes() const {
        AFL_VERIFY(UsedRawBytes);
        return *UsedRawBytes;
    }

    void SetUsedRawBytes(const ui64 value) {
        AFL_VERIFY(!UsedRawBytes);
        UsedRawBytes = value;
    }

    const TReplaceKeyAdapter& GetStart() const {
        return Start;
    }
    const TReplaceKeyAdapter GetFinish() const {
        return Finish;
    }

    bool GetIsStartedByCursor() const {
        return IsStartedByCursor;
    }

    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard>& GetGroupGuard() const {
        AFL_VERIFY(SourceGroupGuard);
        return SourceGroupGuard;
    }

    ui64 GetMemoryGroupId() const {
        AFL_VERIFY(SourceGroupGuard);
        return SourceGroupGuard->GetGroupId();
    }

    virtual void ClearResult() {
        StageData.reset();
        StageResult.reset();
        ResourceGuards.clear();
        SourceGroupGuard = nullptr;
    }

    void SetIsStartedByCursor() {
        IsStartedByCursor = true;
    }

    void SetCursor(const TFetchingScriptCursor& scriptCursor) {
        AFL_VERIFY(!ScriptCursor);
        ScriptCursor = scriptCursor;
    }

    void ContinueCursor(const std::shared_ptr<IDataSource>& sourcePtr);

    class TCompareStartForScanSequence {
    public:
        bool operator()(const std::shared_ptr<IDataSource>& l, const std::shared_ptr<IDataSource>& r) const {
            const std::partial_ordering compareResult = l->GetStart().Compare(r->GetStart());
            if (compareResult == std::partial_ordering::equivalent) {
                return l->GetSourceId() < r->GetSourceId();
            } else {
                return compareResult == std::partial_ordering::less;
            }
        };
    };

    class TCompareFinishForScanSequence {
    public:
        bool operator()(const std::shared_ptr<IDataSource>& l, const std::shared_ptr<IDataSource>& r) const {
            const std::partial_ordering compareResult = l->GetFinish().Compare(r->GetFinish());
            if (compareResult == std::partial_ordering::equivalent) {
                return l->GetSourceId() < r->GetSourceId();
            } else {
                return compareResult == std::partial_ordering::less;
            }
        };
    };

    virtual std::shared_ptr<arrow::RecordBatch> GetStartPKRecordBatch() const = 0;

    void StartProcessing(const std::shared_ptr<IDataSource>& sourcePtr);
    virtual ui64 PredictAccessorsSize(const std::set<ui32>& entityIds) const = 0;

    bool StartFetchingAccessor(const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step) {
        return DoStartFetchingAccessor(sourcePtr, step);
    }

    virtual ui64 GetPathId() const = 0;
    virtual bool HasIndexes(const std::set<ui32>& indexIds) const = 0;

    void InitFetchingPlan(const std::shared_ptr<TFetchingScript>& fetching);

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& indexIds) const = 0;

    void Abort() {
        DoAbort();
    }

    NJson::TJsonValue DebugJsonForMemory() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("details", DoDebugJsonForMemory());
        result.InsertValue("count", GetRecordsCount());
        return result;
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("source_id", GetSourceId());
        result.InsertValue("source_idx", GetSourceIdx());
        result.InsertValue("start", Start.DebugString());
        result.InsertValue("finish", Finish.DebugString());
        result.InsertValue("specific", DoDebugJson());
        return result;
    }

    bool OnIntervalFinished(const ui32 intervalIdx);

    IDataSource(const ui64 sourceId, const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context, const NArrow::TReplaceKey& start,
        const NArrow::TReplaceKey& finish, const TSnapshot& recordSnapshotMin, const TSnapshot& recordSnapshotMax, const ui32 recordsCount,
        const std::optional<ui64> shardingVersion, const bool hasDeletions)
        : TBase(sourceId, sourceIdx, context, recordSnapshotMin, recordSnapshotMax, recordsCount, shardingVersion, hasDeletions)
        , Start(context->GetReadMetadata()->IsDescSorted() ? finish : start, context->GetReadMetadata()->IsDescSorted())
        , Finish(context->GetReadMetadata()->IsDescSorted() ? start : finish, context->GetReadMetadata()->IsDescSorted()) {
        StageData = std::make_unique<TFetchedData>(true, recordsCount);
        UsageClass = GetContext()->GetReadMetadata()->GetPKRangesFilter().GetUsageClass(start, finish);
        AFL_VERIFY(UsageClass != TPKRangeFilter::EUsageClass::NoUsage);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portions_for_merge")("start", Start.DebugString())(
            "finish", Finish.DebugString());
        Y_ABORT_UNLESS(Start.Compare(Finish) != std::partial_ordering::greater);
    }

    virtual ~IDataSource() = default;
};

class TPortionDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    const TPortionInfo::TConstPtr Portion;
    std::shared_ptr<ISnapshotSchema> Schema;

    void NeedFetchColumns(const std::set<ui32>& columnIds, TBlobsAction& blobsAction,
        THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>& nullBlocks, const std::shared_ptr<NArrow::TColumnFilter>& filter);

    virtual void InitUsedRawBytes() override {
        AFL_VERIFY(!UsedRawBytes);
        UsedRawBytes = StageData->GetPortionAccessor().GetColumnRawBytes(GetContext()->GetAllUsageColumns()->GetColumnIds(), false);
    }

    virtual bool DoStartFetchingColumns(
        const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) override;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) override;

    std::shared_ptr<NIndexes::TSkipIndex> SelectOptimalIndex(
        const std::vector<std::shared_ptr<NIndexes::TSkipIndex>>& indexes, const NArrow::NSSA::EIndexCheckOperation op) const;

    virtual TConclusion<bool> DoStartFetchIndex(const NArrow::NSSA::TProcessorContext& context, const TFetchIndexContext& fetchContext) override;
    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(const NArrow::NSSA::TProcessorContext& context,
        const TFetchIndexContext& fetchContext,
        const std::shared_ptr<arrow::Scalar>& value) override;
    virtual TConclusion<bool> DoStartFetchHeader(const NArrow::NSSA::TProcessorContext& context, const TFetchHeaderContext& fetchContext) override;
    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(const NArrow::NSSA::TProcessorContext& context,
        const TFetchHeaderContext& fetchContext) override;
    virtual void DoAssembleAccessor(const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& subColumnName) override;
    virtual TConclusion<bool> DoStartFetchData(
        const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& subColumnName) override;

    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "portion");
        result.InsertValue("info", Portion->DebugString());
        result.InsertValue("commit", Portion->GetCommitSnapshotOptional().value_or(TSnapshot::Zero()).DebugString());
        result.InsertValue("insert", (ui64)Portion->GetInsertWriteIdOptional().value_or(TInsertWriteId(0)));
        return result;
    }

    virtual NJson::TJsonValue DoDebugJsonForMemory() const override {
        NJson::TJsonValue result = TBase::DoDebugJsonForMemory();
        if (GetStageData().HasPortionAccessor()) {
            auto columns = GetStageData().GetPortionAccessor().GetColumnIds();
            //        result.InsertValue("sequential_columns", JoinSeq(",", SequentialEntityIds));
            result.InsertValue("in_mem", GetStageData().GetPortionAccessor().GetColumnRawBytes(columns, false));
            result.InsertValue("columns_in_mem", JoinSeq(",", columns));
        }
        result.InsertValue("portion_id", Portion->GetPortionId());
        result.InsertValue("raw", Portion->GetTotalRawBytes());
        result.InsertValue("blob", Portion->GetTotalBlobBytes());
        result.InsertValue("read_memory", GetColumnRawBytes(GetStageData().GetPortionAccessor().GetColumnIds()));
        return result;
    }
    virtual void DoAbort() override;
    virtual ui64 GetPathId() const override {
        return Portion->GetPathId();
    }

    virtual bool DoStartFetchingAccessor(const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step) override;

public:
    virtual TString GetEntityStorageId(const ui32 entityId) const override {
        return Portion->GetEntityStorageId(entityId, Schema->GetIndexInfo());
    }

    virtual TString GetColumnStorageId(const ui32 columnId) const override {
        return Portion->GetColumnStorageId(columnId, Schema->GetIndexInfo());
    }

    virtual TBlobRange RestoreBlobRange(const TBlobRangeLink16& rangeLink) const override {
        return Portion->RestoreBlobRange(rangeLink);
    }

    virtual const std::shared_ptr<ISnapshotSchema>& GetSourceSchema() const override {
        return Schema;
    }

    virtual std::optional<TSnapshot> GetDataSnapshot() const override {
        if (Portion->HasInsertWriteId()) {
            if (Portion->HasCommitSnapshot()) {
                return Portion->GetCommitSnapshotVerified();
            } else if (GetContext()->GetReadMetadata()->IsMyUncommitted(Portion->GetInsertWriteIdVerified())) {
                return GetContext()->GetReadMetadata()->GetRequestSnapshot();
            }
        }
        return std::nullopt;
    }

    virtual ui64 PredictAccessorsSize(const std::set<ui32>& entityIds) const override {
        return Portion->GetApproxChunksCount(entityIds.size()) * sizeof(TColumnRecord);
    }

    virtual std::shared_ptr<arrow::RecordBatch> GetStartPKRecordBatch() const override {
        if (GetContext()->GetReadMetadata()->IsDescSorted()) {
            AFL_VERIFY(Portion->GetMeta().GetFirstLastPK().GetBatch()->num_rows());
            return Portion->GetMeta().GetFirstLastPK().GetBatch()->Slice(Portion->GetMeta().GetFirstLastPK().GetBatch()->num_rows() - 1, 1);
        } else {
            return Portion->GetMeta().GetFirstLastPK().GetBatch()->Slice(0, 1);
        }
    }

    virtual bool DoAddTxConflict() override {
        if (Portion->HasCommitSnapshot() || !Portion->HasInsertWriteId()) {
            GetContext()->GetReadMetadata()->SetBrokenWithCommitted();
            return true;
        } else if (!GetContext()->GetReadMetadata()->IsMyUncommitted(Portion->GetInsertWriteIdVerified())) {
            GetContext()->GetReadMetadata()->SetConflictedWriteId(Portion->GetInsertWriteIdVerified());
            return true;
        }
        return false;
    }

    virtual bool HasIndexes(const std::set<ui32>& indexIds) const override {
        return Schema->GetIndexInfo().HasIndexes(indexIds);
    }

    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(NBlobOperations::NRead::TCompositeReadBlobs&& blobsOriginal) const override {
        return GetStageData().GetPortionAccessor().DecodeBlobAddresses(std::move(blobsOriginal), Schema->GetIndexInfo());
    }

    virtual ui64 GetColumnsVolume(const std::set<ui32>& columnIds, const EMemType type) const override {
        AFL_VERIFY(columnIds.size());
        switch (type) {
            case EMemType::Raw:
                return GetStageData().GetPortionAccessor().GetColumnRawBytes(columnIds, false);
            case EMemType::Blob:
                return GetStageData().GetPortionAccessor().GetColumnBlobBytes(columnIds, false);
            case EMemType::RawSequential:
                return GetStageData().GetPortionAccessor().GetMinMemoryForReadColumns(columnIds);
        }
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& columnsIds) const override {
        AFL_VERIFY(columnsIds.size());
        return GetStageData().GetPortionAccessor().GetColumnRawBytes(columnsIds, false);
    }

    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& columnsIds) const override {
        return GetStageData().GetPortionAccessor().GetColumnBlobBytes(columnsIds, false);
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& indexIds) const override {
        return GetStageData().GetPortionAccessor().GetIndexRawBytes(indexIds, false);
    }

    const TPortionInfo& GetPortionInfo() const {
        return *Portion;
    }

    const TPortionInfo::TConstPtr& GetPortionInfoPtr() const {
        return Portion;
    }

    TPortionDataSource(const ui32 sourceIdx, const std::shared_ptr<TPortionInfo>& portion, const std::shared_ptr<TSpecialReadContext>& context);
};

}   // namespace NKikimr::NOlap::NReader::NSimple
