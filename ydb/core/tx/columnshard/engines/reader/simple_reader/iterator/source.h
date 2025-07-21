#pragma once
#include "context.h"
#include "fetched_data.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/action.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/engines/predicate/range.h>
#include <ydb/core/tx/columnshard/engines/reader/common/comparable.h>
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

using TCompareKeyForScanSequence = NCommon::TCompareKeyForScanSequence;
using TReplaceKeyAdapter = NCommon::TReplaceKeyAdapter;

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

class IDataSource: public NCommon::IDataSource {
public:
    enum class EType {
        SysInfo,
        Portion,
        Aggregation
    };

private:
    using TBase = NCommon::IDataSource;
    YDB_READONLY(EType, Type, EType::Portion);
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
    virtual void DoBuildStageResult(const std::shared_ptr<NCommon::IDataSource>& sourcePtr) override;
    virtual void DoOnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& sourcePtr) override;

    void Finalize(const std::optional<ui64> memoryLimit);
    bool NeedFullAnswerFlag = true;
    std::optional<ui32> PurposeSyncPointIndex;

protected:
    std::optional<ui64> UsedRawBytes;

    virtual void DoAbort() = 0;
    virtual NJson::TJsonValue DoDebugJsonForMemory() const {
        return NJson::JSON_MAP;
    }
    virtual bool DoStartFetchingAccessor(const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step) = 0;

public:
    bool NeedFullAnswer() const {
        return NeedFullAnswerFlag;
    }

    void SetNeedFullAnswer(const bool value) {
        NeedFullAnswerFlag = value;
    }

    ui32 GetPurposeSyncPointIndex() const {
        AFL_VERIFY(PurposeSyncPointIndex);
        return *PurposeSyncPointIndex;
    }

    void ResetPurposeSyncPointIndex() {
        AFL_VERIFY(PurposeSyncPointIndex);
        PurposeSyncPointIndex.reset();
    }

    void SetPurposeSyncPointIndex(const ui32 value) {
        if (!PurposeSyncPointIndex) {
            AFL_VERIFY(value == 0);
        } else {
            AFL_VERIFY(*PurposeSyncPointIndex < value);
        }
        PurposeSyncPointIndex = value;
    }

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
    const TReplaceKeyAdapter& GetFinish() const {
        return Finish;
    }

    bool GetIsStartedByCursor() const {
        return IsStartedByCursor;
    }

    const std::shared_ptr<NGroupedMemoryManager::TGroupGuard>& GetGroupGuard() const {
        AFL_VERIFY(SourceGroupGuard);
        return SourceGroupGuard;
    }

    std::shared_ptr<NGroupedMemoryManager::TGroupGuard> ExtractGroupGuard() {
        AFL_VERIFY(SourceGroupGuard);
        auto result = std::move(SourceGroupGuard);
        SourceGroupGuard = nullptr;
        return std::move(result);
    }

    ui64 GetMemoryGroupId() const {
        AFL_VERIFY(SourceGroupGuard);
        return SourceGroupGuard->GetGroupId();
    }

    virtual void ClearResult() {
        ClearStageData();
        MutableExecutionContext().Stop();
        StageResult.reset();
        ResourceGuards.clear();
        SourceGroupGuard = nullptr;
    }

    void SetIsStartedByCursor() {
        IsStartedByCursor = true;
    }

    void SetCursor(TFetchingScriptCursor&& scriptCursor) {
        AFL_VERIFY(!ScriptCursor);
        ScriptCursor = std::move(scriptCursor);
    }

    void ContinueCursor(const std::shared_ptr<NCommon::IDataSource>& sourcePtr);

    template <bool Reverse>
    class TCompareStartForScanSequence {
    public:
        bool operator()(const std::shared_ptr<IDataSource>& l, const std::shared_ptr<IDataSource>& r) const {
            const std::partial_ordering compareResult = l->GetStart().Compare(r->GetStart());
            if (compareResult == std::partial_ordering::equivalent) {
                return l->GetSourceId() < r->GetSourceId();
            } else {
                return Reverse ? compareResult == std::partial_ordering::greater : compareResult == std::partial_ordering::less;
            }
        };
    };

    virtual NArrow::TSimpleRow GetStartPKRecordBatch() const = 0;

    void StartProcessing(const std::shared_ptr<NCommon::IDataSource>& sourcePtr);
    virtual ui64 PredictAccessorsSize(const std::set<ui32>& entityIds) const = 0;

    bool StartFetchingAccessor(const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step) {
        return DoStartFetchingAccessor(sourcePtr, step);
    }

    void StartFetchingDuplicateFilter(std::shared_ptr<NDuplicateFiltering::IFilterSubscriber>&& subscriber) {
        NActors::TActivationContext::AsActorContext().Send(
            std::static_pointer_cast<TSpecialReadContext>(GetContext())->GetDuplicatesManagerVerified(),
            new NDuplicateFiltering::TEvRequestFilter(*this, std::move(subscriber)));
    }

    virtual TInternalPathId GetPathId() const = 0;
    virtual bool HasIndexes(const std::set<ui32>& indexIds) const = 0;

    void InitFetchingPlan(const std::shared_ptr<TFetchingScript>& fetching);
    bool HasFetchingPlan() const {
        return !!FetchingPlan;
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& indexIds) const = 0;

    virtual NArrow::TSimpleRow GetMinPK() const = 0;
    virtual NArrow::TSimpleRow GetMaxPK() const = 0;

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

    IDataSource(const EType type, const ui64 sourceId, const ui32 sourceIdx, const std::shared_ptr<NCommon::TSpecialReadContext>& context,
        NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish, const TSnapshot& recordSnapshotMin, const TSnapshot& recordSnapshotMax,
        const std::optional<ui32> recordsCount, const std::optional<ui64> shardingVersion, const bool hasDeletions)
        : TBase(sourceId, sourceIdx, context, recordSnapshotMin, recordSnapshotMax, recordsCount, shardingVersion, hasDeletions)
        , Type(type)
        , Start(context->GetReadMetadata()->IsDescSorted() ? std::move(finish) : std::move(start), context->GetReadMetadata()->IsDescSorted())
        , Finish(context->GetReadMetadata()->IsDescSorted() ? std::move(start) : std::move(finish), context->GetReadMetadata()->IsDescSorted()) {
        if (context->GetReadMetadata()->IsDescSorted()) {
            UsageClass = GetContext()->GetReadMetadata()->GetPKRangesFilter().GetUsageClass(Finish.GetValue(), Start.GetValue());
        } else {
            UsageClass = GetContext()->GetReadMetadata()->GetPKRangesFilter().GetUsageClass(Start.GetValue(), Finish.GetValue());
        }
        AFL_VERIFY(UsageClass != TPKRangeFilter::EUsageClass::NoUsage);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "portions_for_merge")("start", Start.DebugString())(
            "finish", Finish.DebugString());
        AFL_VERIFY_DEBUG(Start.Compare(Finish) != std::partial_ordering::greater);
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
        UsedRawBytes = GetStageData().GetPortionAccessor().GetColumnRawBytes(GetContext()->GetAllUsageColumns()->GetColumnIds(), false);
    }

    virtual bool DoStartFetchingColumns(
        const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) override;
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) override;

    std::shared_ptr<NIndexes::TSkipIndex> SelectOptimalIndex(
        const std::vector<std::shared_ptr<NIndexes::TSkipIndex>>& indexes, const NArrow::NSSA::TIndexCheckOperation& op) const;

    virtual TConclusion<bool> DoStartFetchImpl(
        const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<NCommon::IKernelFetchLogic>>& fetchersExt) override;

    virtual TConclusion<bool> DoStartReserveMemory(const NArrow::NSSA::TProcessorContext& context,
        const THashMap<ui32, IDataSource::TDataAddress>& columns, const THashMap<ui32, IDataSource::TFetchIndexContext>& indexes,
        const THashMap<ui32, IDataSource::TFetchHeaderContext>& headers,
        const std::shared_ptr<NArrow::NSSA::IMemoryCalculationPolicy>& policy) override;
    virtual TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> DoStartFetchIndex(
        const NArrow::NSSA::TProcessorContext& context, const TFetchIndexContext& fetchContext) override;
    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(const NArrow::NSSA::TProcessorContext& context,
        const TCheckIndexContext& fetchContext, const std::shared_ptr<arrow::Scalar>& value) override;
    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchHeader(
        const NArrow::NSSA::TProcessorContext& context, const TFetchHeaderContext& fetchContext) override;
    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(
        const NArrow::NSSA::TProcessorContext& context, const TCheckHeaderContext& fetchContext) override;
    virtual void DoAssembleAccessor(const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& subColumnName) override;
    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const NArrow::NSSA::TProcessorContext& context, const TDataAddress& addr) override;

    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "portion");
        result.InsertValue("info", Portion->DebugString());
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
    virtual TInternalPathId GetPathId() const override {
        return Portion->GetPathId();
    }

    virtual bool DoStartFetchingAccessor(const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step) override;

public:
    virtual TString GetEntityStorageId(const ui32 entityId) const override {
        return Portion->GetEntityStorageId(entityId, Schema->GetIndexInfo());
    }

    virtual TString GetColumnStorageId(const ui32 columnId) const override {
        return Portion->GetColumnStorageId(columnId, Schema->GetIndexInfo());
    }

    virtual TBlobRange RestoreBlobRange(const TBlobRangeLink16& rangeLink) const override {
        return GetStageData().GetPortionAccessor().RestoreBlobRange(rangeLink);
    }

    virtual const std::shared_ptr<ISnapshotSchema>& GetSourceSchema() const override {
        return Schema;
    }

    virtual ui64 PredictAccessorsSize(const std::set<ui32>& entityIds) const override {
        return Portion->GetApproxChunksCount(entityIds.size()) * sizeof(TColumnRecord);
    }

    virtual NArrow::TSimpleRow GetStartPKRecordBatch() const override {
        if (GetContext()->GetReadMetadata()->IsDescSorted()) {
            return Portion->IndexKeyEnd();
        } else {
            return Portion->IndexKeyStart();
        }
    }

    virtual bool DoAddTxConflict() override;

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

    virtual NArrow::TSimpleRow GetMinPK() const override {
        return Portion->GetMeta().IndexKeyStart();
    }

    virtual NArrow::TSimpleRow GetMaxPK() const override {
        return Portion->GetMeta().IndexKeyEnd();
    }

    const TPortionInfo& GetPortionInfo() const {
        return *Portion;
    }

    const TPortionInfo::TConstPtr& GetPortionInfoPtr() const {
        return Portion;
    }

    TPortionDataSource(
        const ui32 sourceIdx, const std::shared_ptr<TPortionInfo>& portion, const std::shared_ptr<NCommon::TSpecialReadContext>& context);
};

class TAggregationDataSource: public IDataSource {
private:
    using TBase = IDataSource;
    YDB_READONLY_DEF(std::vector<std::shared_ptr<NCommon::IDataSource>>, Sources);
    const ui64 LastSourceId;
    const ui64 LastSourceRecordsCount;

    void DoBuildStageResult(const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) override {
        const ui32 recordsCount = GetStageData().GetTable()->GetRecordsCountActualVerified();
        StageResult = std::make_unique<TFetchedResult>(ExtractStageData(), *GetContext()->GetCommonContext()->GetResolver());
        StageResult->SetPages({ TPortionDataAccessor::TReadPage(0, recordsCount, 0) });
        StageResult->SetResultChunk(StageResult->GetBatch()->BuildTableVerified(), 0, recordsCount);
    }

    virtual void InitUsedRawBytes() override {
        AFL_VERIFY(false);
    }

    virtual bool DoStartFetchingColumns(const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/, const TFetchingScriptCursor& /*step*/,
        const TColumnsSetIds& /*columns*/) override {
        AFL_VERIFY(false);
        return true;
    }
    virtual void DoAssembleColumns(const std::shared_ptr<TColumnsSet>& /*columns*/, const bool /*sequential*/) override {
        AFL_VERIFY(false);
    }

    virtual TConclusion<bool> DoStartFetchImpl(const NArrow::NSSA::TProcessorContext& /*context*/,
        const std::vector<std::shared_ptr<NCommon::IKernelFetchLogic>>& /*fetchersExt*/) override {
        return TConclusionStatus::Fail("not implemented DoStartFetchImpl for TAggregationDataSource");
    }

    virtual TConclusion<bool> DoStartReserveMemory(const NArrow::NSSA::TProcessorContext& /*context*/,
        const THashMap<ui32, IDataSource::TDataAddress>& /*columns*/, const THashMap<ui32, IDataSource::TFetchIndexContext>& /*indexes*/,
        const THashMap<ui32, IDataSource::TFetchHeaderContext>& /*headers*/,
        const std::shared_ptr<NArrow::NSSA::IMemoryCalculationPolicy>& /*policy*/) override {
        return TConclusionStatus::Fail("not implemented DoStartReserveMemory for TAggregationDataSource");
    }
    virtual TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> DoStartFetchIndex(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TFetchIndexContext& /*fetchContext*/) override {
        return TConclusionStatus::Fail("not implemented DoStartFetchIndex for TAggregationDataSource");
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(const NArrow::NSSA::TProcessorContext& /*context*/,
        const TCheckIndexContext& /*fetchContext*/, const std::shared_ptr<arrow::Scalar>& /*value*/) override {
        return TConclusionStatus::Fail("not implemented DoCheckIndex for TAggregationDataSource");
    }
    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchHeader(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TFetchHeaderContext& /*fetchContext*/) override {
        return TConclusionStatus::Fail("not implemented DoStartFetchHeader for TAggregationDataSource");
    }
    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TCheckHeaderContext& /*fetchContext*/) override {
        return TConclusionStatus::Fail("not implemented DoCheckHeader for TAggregationDataSource");
    }
    virtual void DoAssembleAccessor(
        const NArrow::NSSA::TProcessorContext& /*context*/, const ui32 /*columnId*/, const TString& /*subColumnName*/) override {
        AFL_VERIFY(false);
    }
    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TDataAddress& /*addr*/) override {
        return TConclusionStatus::Fail("not implemented DoStartFetchData for TAggregationDataSource");
    }

    virtual NJson::TJsonValue DoDebugJson() const override {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("type", "aggregation");
        return result;
    }

    virtual void DoAbort() override {
    }

    virtual TInternalPathId GetPathId() const override {
        return Sources.front()->GetAs<IDataSource>()->GetPathId();
    }

    virtual bool DoStartFetchingAccessor(
        const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/, const TFetchingScriptCursor& /*step*/) override {
        AFL_VERIFY(false);
        return false;
    }

public:
    ui64 GetLastSourceId() const {
        return LastSourceId;
    }

    ui64 GetLastSourceRecordsCount() const {
        return LastSourceRecordsCount;
    }

    virtual TString GetEntityStorageId(const ui32 /*entityId*/) const override {
        AFL_VERIFY(false);
        return "";
    }

    virtual TString GetColumnStorageId(const ui32 /*columnId*/) const override {
        AFL_VERIFY(false);
        return "";
    }

    virtual TBlobRange RestoreBlobRange(const TBlobRangeLink16& /*rangeLink*/) const override {
        AFL_VERIFY(false);
        return TBlobRange();
    }

    virtual const std::shared_ptr<ISnapshotSchema>& GetSourceSchema() const override {
        AFL_VERIFY(false);
        return Default<std::shared_ptr<ISnapshotSchema>>();
    }

    virtual ui64 PredictAccessorsSize(const std::set<ui32>& /*entityIds*/) const override {
        AFL_VERIFY(false);
        return 0;
    }

    virtual NArrow::TSimpleRow GetStartPKRecordBatch() const override {
        AFL_VERIFY(false);
        return NArrow::TSimpleRow(nullptr, 0);
    }

    virtual bool DoAddTxConflict() override {
        AFL_VERIFY(false);
        return false;
    }

    virtual bool HasIndexes(const std::set<ui32>& /*indexIds*/) const override {
        AFL_VERIFY(false);
        return false;
    }

    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(
        NBlobOperations::NRead::TCompositeReadBlobs&& /*blobsOriginal*/) const override {
        AFL_VERIFY(false);
        return {};
    }

    virtual ui64 GetColumnsVolume(const std::set<ui32>& /*columnIds*/, const EMemType /*type*/) const override {
        AFL_VERIFY(false);
        return 0;
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& /*columnsIds*/) const override {
        AFL_VERIFY(false);
        return 0;
    }

    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& /*columnsIds*/) const override {
        AFL_VERIFY(false);
        return 0;
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& /*indexIds*/) const override {
        AFL_VERIFY(false);
        return 0;
    }

    virtual NArrow::TSimpleRow GetMinPK() const override {
        AFL_VERIFY(false);
        return NArrow::TSimpleRow(nullptr, 0);
    }

    virtual NArrow::TSimpleRow GetMaxPK() const override {
        AFL_VERIFY(false);
        return NArrow::TSimpleRow(nullptr, 0);
    }

    TAggregationDataSource(
        std::vector<std::shared_ptr<NCommon::IDataSource>>&& sources, const std::shared_ptr<NCommon::TSpecialReadContext>& context)
        : TBase(EType::Aggregation, sources.back()->GetSourceId(), sources.back()->GetSourceIdx(), context,
              sources.front()->GetAs<IDataSource>()->GetStart().CopyValue(), sources.back()->GetAs<IDataSource>()->GetFinish().CopyValue(),
              TSnapshot::Zero(), TSnapshot::Zero(), std::nullopt, std::nullopt, false)
        , Sources(std::move(sources))
        , LastSourceId(Sources.back()->GetSourceId())
        , LastSourceRecordsCount(Sources.back()->GetRecordsCount()) {
        AFL_VERIFY(Sources.size());
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
