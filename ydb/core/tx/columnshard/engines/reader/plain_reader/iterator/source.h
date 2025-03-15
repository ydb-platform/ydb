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

namespace NKikimr::NOlap::NReader::NPlain {

class TFetchingInterval;
class TPlainReadData;
class IFetchTaskConstructor;
class IFetchingStep;

class IDataSource: public NCommon::IDataSource {
private:
    using TBase = NCommon::IDataSource;
    YDB_ACCESSOR(bool, ExclusiveIntervalOnly, true);
    YDB_READONLY_DEF(NArrow::NMerger::TSortableBatchPosition, Start);
    YDB_READONLY_DEF(NArrow::NMerger::TSortableBatchPosition, Finish);
    NArrow::TReplaceKey StartReplaceKey;
    NArrow::TReplaceKey FinishReplaceKey;
    YDB_READONLY(ui32, IntervalsCount, 0);
    virtual NJson::TJsonValue DoDebugJson() const = 0;
    bool MergingStartedFlag = false;
    TAtomic SourceStartedFlag = 0;
    std::shared_ptr<TFetchingScript> FetchingPlan;
    ui32 CurrentPlanStepIndex = 0;
    YDB_READONLY(TPKRangeFilter::EUsageClass, UsageClass, TPKRangeFilter::EUsageClass::PartialUsage);

    virtual void DoOnSourceFetchingFinishedSafe(IDataReader& owner, const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) override;
    virtual void DoBuildStageResult(const std::shared_ptr<NCommon::IDataSource>& sourcePtr) override;
    virtual void DoOnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& sourcePtr) override;
    virtual TConclusion<bool> DoStartFetchIndex(const NArrow::NSSA::TProcessorContext& /*context*/, const TFetchIndexContext& /*fetchContext*/) override {
        AFL_VERIFY(false);
        return false;
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(const NArrow::NSSA::TProcessorContext& /*context*/,
        const TFetchIndexContext& /*fetchContext*/,
        const std::shared_ptr<arrow::Scalar>& /*value*/) override {
        AFL_VERIFY(false);
        return NArrow::TColumnFilter::BuildAllowFilter();
    }
    virtual void DoAssembleAccessor(
        const NArrow::NSSA::TProcessorContext& /*context*/, const ui32 /*columnId*/, const TString& /*subColumnName*/) override {
        AFL_VERIFY(false);
    }
    virtual TConclusion<bool> DoStartFetchData(
        const NArrow::NSSA::TProcessorContext& /*context*/, const ui32 /*columnId*/, const TString& /*subColumnName*/) override {
        AFL_VERIFY(false);
        return false;
    }
    virtual TConclusion<bool> DoStartFetchHeader(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TFetchHeaderContext& /*fetchContext*/) override {
        AFL_VERIFY(false);
        return false;
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TFetchHeaderContext& /*fetchContext*/) override {
        AFL_VERIFY(false);
        return NArrow::TColumnFilter::BuildAllowFilter();
    }

protected:
    THashMap<ui32, TFetchingInterval*> Intervals;

    TAtomic FilterStageFlag = 0;
    bool IsReadyFlag = false;

    virtual void DoAbort() = 0;
    virtual NJson::TJsonValue DoDebugJsonForMemory() const {
        return NJson::JSON_MAP;
    }
    virtual bool DoStartFetchingAccessor(const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step) = 0;

public:
    virtual bool NeedAccessorsForRead() const = 0;
    virtual bool NeedAccessorsFetching() const = 0;
    virtual ui64 PredictAccessorsMemory() const = 0;
    bool StartFetchingAccessor(const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step) {
        return DoStartFetchingAccessor(sourcePtr, step);
    }

    virtual ui64 GetPathId() const = 0;
    virtual bool HasIndexes(const std::set<ui32>& indexIds) const = 0;

    const NArrow::TReplaceKey& GetStartReplaceKey() const {
        return StartReplaceKey;
    }
    const NArrow::TReplaceKey& GetFinishReplaceKey() const {
        return FinishReplaceKey;
    }

    void InitFetchingPlan(const std::shared_ptr<TFetchingScript>& fetching);

    std::shared_ptr<arrow::RecordBatch> GetLastPK() const {
        return Finish.BuildSortingCursor().ExtractSortingPosition(Finish.GetSortFields());
    }
    void IncIntervalsCount() {
        ++IntervalsCount;
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& indexIds) const = 0;

    bool IsMergingStarted() const {
        return MergingStartedFlag;
    }

    void StartMerging() {
        AFL_VERIFY(!MergingStartedFlag);
        MergingStartedFlag = true;
    }

    void Abort() {
        Intervals.clear();
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
        result.InsertValue("start", Start.DebugJson());
        result.InsertValue("finish", Finish.DebugJson());
        result.InsertValue("specific", DoDebugJson());
        return result;
    }

    bool OnIntervalFinished(const ui32 intervalIdx);

    bool IsDataReady() const {
        return IsReadyFlag;
    }

    void RegisterInterval(TFetchingInterval& interval, const std::shared_ptr<IDataSource>& sourcePtr);

    IDataSource(const ui64 sourceId, const ui32 sourceIdx, const std::shared_ptr<TSpecialReadContext>& context, const NArrow::TReplaceKey& start,
        const NArrow::TReplaceKey& finish, const TSnapshot& recordSnapshotMin, const TSnapshot& recordSnapshotMax, const ui32 recordsCount,
        const std::optional<ui64> shardingVersion, const bool hasDeletions)
        : TBase(sourceId, sourceIdx, context, recordSnapshotMin, recordSnapshotMax, recordsCount, shardingVersion, hasDeletions)
        , Start(context->GetReadMetadata()->BuildSortedPosition(start))
        , Finish(context->GetReadMetadata()->BuildSortedPosition(finish))
        , StartReplaceKey(start)
        , FinishReplaceKey(finish) {
        UsageClass = GetContext()->GetReadMetadata()->GetPKRangesFilter().GetUsageClass(GetStartReplaceKey(), GetFinishReplaceKey());
        AFL_VERIFY(UsageClass != TPKRangeFilter::EUsageClass::NoUsage);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portions_for_merge")("start", Start.DebugJson())("finish", Finish.DebugJson());
        if (Start.IsReverseSort()) {
            std::swap(Start, Finish);
        }
        Y_ABORT_UNLESS(Start.Compare(Finish) != std::partial_ordering::greater);
    }

    virtual ~IDataSource() {
        AFL_VERIFY(Intervals.empty());
    }
};

class TPortionDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    const TPortionInfo::TConstPtr Portion;
    std::shared_ptr<ISnapshotSchema> Schema;

    void NeedFetchColumns(const std::set<ui32>& columnIds, TBlobsAction& blobsAction,
        THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>& nullBlocks, const std::shared_ptr<NArrow::TColumnFilter>& filter);

    virtual bool DoStartFetchingColumns(
        const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) override;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) override;
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
    virtual ui64 PredictAccessorsMemory() const override {
        return Portion->GetApproxChunksCount(GetContext()->GetCommonContext()->GetReadMetadata()->GetResultSchema()->GetColumnsCount()) *
               sizeof(TColumnRecord);
    }

    virtual bool NeedAccessorsForRead() const override {
        return true;
    }

    virtual bool NeedAccessorsFetching() const override {
        return !StageData || !StageData->HasPortionAccessor();
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

    TPortionDataSource(const ui32 sourceIdx, const std::shared_ptr<TPortionInfo>& portion, const std::shared_ptr<TSpecialReadContext>& context)
        : TBase(portion->GetPortionId(), sourceIdx, context, portion->IndexKeyStart(), portion->IndexKeyEnd(),
              portion->RecordSnapshotMin(TSnapshot::Zero()), portion->RecordSnapshotMax(TSnapshot::Zero()), portion->GetRecordsCount(),
              portion->GetShardingVersionOptional(), portion->GetMeta().GetDeletionsCount())
        , Portion(portion)
        , Schema(GetContext()->GetReadMetadata()->GetLoadSchemaVerified(*portion)) {
    }
};

class TCommittedDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    TCommittedBlob CommittedBlob;
    bool ReadStarted = false;

    virtual void DoAbort() override {
    }

    virtual bool DoStartFetchingColumns(
        const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) override;

    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) override;
    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "commit");
        result.InsertValue("info", CommittedBlob.DebugString());
        return result;
    }
    virtual ui64 GetPathId() const override {
        return 0;
    }

    virtual bool DoAddTxConflict() override {
        if (CommittedBlob.IsCommitted()) {
            GetContext()->GetReadMetadata()->SetBrokenWithCommitted();
            return true;
        } else if (!GetContext()->GetReadMetadata()->IsMyUncommitted(CommittedBlob.GetInsertWriteId())) {
            GetContext()->GetReadMetadata()->SetConflictedWriteId(CommittedBlob.GetInsertWriteId());
            return true;
        }
        return false;
    }

public:
    virtual ui64 PredictAccessorsMemory() const override {
        return 0;
    }

    virtual bool NeedAccessorsForRead() const override {
        return false;
    }

    virtual bool NeedAccessorsFetching() const override {
        return false;
    }

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

    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& /*columnsIds*/) const override {
        return CommittedBlob.GetBlobRange().Size;
    }

    virtual bool DoStartFetchingAccessor(const std::shared_ptr<IDataSource>& /*sourcePtr*/, const TFetchingScriptCursor& /*step*/) override {
        return false;
    }

    virtual ui64 GetColumnsVolume(const std::set<ui32>& columnIds, const EMemType type) const override {
        AFL_VERIFY(columnIds.size());
        switch (type) {
            case EMemType::Raw:
                return GetColumnRawBytes(columnIds);
            case EMemType::Blob:
                return GetColumnBlobBytes(columnIds);
            case EMemType::RawSequential:
                return GetColumnRawBytes(columnIds);
        }
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& /*columnIds*/) const override {
        AFL_VERIFY(false);
        return 0;
    }

    const TCommittedBlob& GetCommitted() const {
        return CommittedBlob;
    }

    TCommittedDataSource(const ui32 sourceIdx, const TCommittedBlob& committed, const std::shared_ptr<TSpecialReadContext>& context)
        : TBase((ui64)committed.GetInsertWriteId(), sourceIdx, context, committed.GetFirst(), committed.GetLast(),
              committed.GetCommittedSnapshotDef(TSnapshot::Zero()), committed.GetCommittedSnapshotDef(TSnapshot::Zero()),
              committed.GetRecordsCount(), {}, committed.GetIsDelete())
        , CommittedBlob(committed) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NPlain
