#include "metadata_accessor.h"

#include "reader/common/description.h"
#include "reader/common_reader/constructor/read_metadata.h"
#include "reader/plain_reader/iterator/constructors.h"
#include "reader/simple_reader/iterator/collections/constructors.h"
#include "reader/sys_view/chunks/chunks.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <util/folder/path.h>

namespace NKikimr::NOlap {
ITableMetadataAccessor::ITableMetadataAccessor(const TString& tablePath)
    : TablePath(tablePath) {
    AFL_VERIFY(!!TablePath);
}

TString ITableMetadataAccessor::GetTableName() const {
    return TFsPath(TablePath).Fix().GetName();
}

TSysViewTableAccessor::TSysViewTableAccessor(const TString& tableName, const NColumnShard::TSchemeShardLocalPathId externalPathId,
    const std::optional<NColumnShard::TInternalPathId> internalPathId)
    : TBase(tableName)
    , PathId(NColumnShard::TUnifiedPathId::BuildNoCheck(internalPathId, externalPathId)) {
    AFL_VERIFY(GetTablePath().find(".sys") != TString::npos);
}

namespace {
class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    NReader::NCommon::TFetchingScriptCursor Step;
    std::shared_ptr<NReader::NSimple::IDataSource> Source;
    const NColumnShard::TCounterGuard Guard;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Source->GetContext()->GetCommonContext()->GetAbortionFlag();
    }

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("facc"));
        if (result.HasErrors()) {
            Source->GetContext()->GetCommonContext()->AbortWithError("has errors on portion accessors restore");
            return;
        }
        AFL_VERIFY(result.GetPortions().size() == 1)("count", result.GetPortions().size());
        Source->MutableStageData().SetPortionAccessor(std::move(result.ExtractPortionsVector().front()));
        Source->InitUsedRawBytes();
        AFL_VERIFY(Step.Next());
        const auto& commonContext = *Source->GetContext()->GetCommonContext();
        auto task = std::make_shared<NReader::NCommon::TStepAction>(Source, std::move(Step), commonContext.GetScanActorId(), false);
        NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, commonContext.GetConveyorProcessId());
    }

public:
    TPortionAccessorFetchingSubscriber(
        const NReader::NCommon::TFetchingScriptCursor& step, const std::shared_ptr<NReader::NSimple::IDataSource>& source)
        : Step(step)
        , Source(source)
        , Guard(Source->GetContext()->GetCommonContext()->GetCounters().GetFetcherAcessorsGuard()) {
    }
};

}   // namespace

class TSysViewPortionInfo: public NReader::NSimple::IDataSource {
private:
    using TBase = NReader::NSimple::IDataSource;
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, UnifiedPathId);
    YDB_READONLY(ui64, TabletId, 0);
    YDB_READONLY_DEF(TPortionInfo::TConstPtr, Portion);

    virtual TConclusion<bool> DoStartFetchImpl(const NArrow::NSSA::TProcessorContext& /*context*/,
        const std::vector<std::shared_ptr<NReader::NCommon::IKernelFetchLogic>>& /*fetchersExt*/) override {
        return false;
    }

    virtual bool DoStartFetchingAccessor(
        const std::shared_ptr<IDataSource>& sourcePtr, const NReader::NCommon::TFetchingScriptCursor& step) override {
        AFL_VERIFY(!GetStageData().HasPortionAccessor());
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

        std::shared_ptr<TDataAccessorsRequest> request =
            std::make_shared<TDataAccessorsRequest>(NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
        request->AddPortion(Portion);
        request->SetColumnIds(GetContext()->GetAllUsageColumns()->GetColumnIds());
        request->RegisterSubscriber(std::make_shared<TPortionAccessorFetchingSubscriber>(step, sourcePtr));
        GetContext()->GetCommonContext()->GetDataAccessorsManager()->AskData(request);
        return true;
    }

    std::shared_ptr<arrow::Array> BuildArrayAccessor(const ui64 columnId, const ui32 recordsCount) const {
        if (columnId == 1) {
            return NArrow::TStatusValidator::GetValid(
                arrow::MakeArrayFromScalar(arrow::UInt64Scalar(UnifiedPathId.GetSchemeShardLocalPathId().GetRawValue()), recordsCount));
        }
        if (columnId == 2) {
            return NArrow::TStatusValidator::GetValid(
                arrow::MakeArrayFromScalar(arrow::StringScalar(::ToString(Portion->GetProduced())), recordsCount));
        }
        if (columnId == 3) {
            return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(TabletId), recordsCount));
        }
        if (columnId == 6) {
            return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt64Scalar(Portion->GetPortionId()), recordsCount));
        }
        if (columnId == 13) {
            if (Portion->IsRemovedFor(GetContext()->GetCommonContext()->GetReadMetadata()->GetRequestSnapshot())) {
                return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(0), recordsCount));
            } else {
                return NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(arrow::UInt8Scalar(1), recordsCount));
            }
        }
        AFL_VERIFY(false);
        return nullptr;
    }

    virtual void DoAssembleColumns(const std::shared_ptr<NReader::NCommon::TColumnsSet>& columns, const bool /*sequential*/) override {
        const ui32 recordsCount = GetRecordsCount();
        for (auto&& i : columns->GetColumnIds()) {
            if (i == (ui64)IIndexInfo::ESpecialColumn::PLAN_STEP || i == (ui64)IIndexInfo::ESpecialColumn::TX_ID ||
                i == (ui64)IIndexInfo::ESpecialColumn::WRITE_ID) {
                MutableStageData().GetTable()->AddVerified(i,
                    std::make_shared<NArrow::NAccessor::TTrivialArray>(
                        NArrow::TThreadSimpleArraysCache::GetConst(arrow::uint64(), std::make_shared<arrow::UInt64Scalar>(0), recordsCount)),
                    true);
            } else {
                MutableStageData().GetTable()->AddVerified(
                    i, std::make_shared<NArrow::NAccessor::TTrivialArray>(BuildArrayAccessor(i, recordsCount)), true);
            }
        }
    }

    virtual bool DoStartFetchingColumns(const std::shared_ptr<NReader::NCommon::IDataSource>& /*sourcePtr*/,
        const NReader::NCommon::TFetchingScriptCursor& /*step*/, const NReader::NCommon::TColumnsSetIds& /*columns*/) override {
        return false;
    }

    virtual void DoAssembleAccessor(
        const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& /*subColumnName*/) override {
        const ui32 recordsCount = GetRecordsCount();
        if (columnId == (ui64)IIndexInfo::ESpecialColumn::PLAN_STEP || columnId == (ui64)IIndexInfo::ESpecialColumn::TX_ID ||
            columnId == (ui64)IIndexInfo::ESpecialColumn::WRITE_ID) {
            context.GetResources()->AddVerified(columnId,
                std::make_shared<NArrow::NAccessor::TTrivialArray>(
                    NArrow::TThreadSimpleArraysCache::GetConst(arrow::uint64(), std::make_shared<arrow::UInt64Scalar>(0), recordsCount)),
                true);
        } else {
            context.GetResources()->AddVerified(
                columnId, std::make_shared<NArrow::NAccessor::TTrivialArray>(BuildArrayAccessor(columnId, recordsCount)), true);
        }
    }

    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchData(
        const NArrow::NSSA::TProcessorContext& /*context*/, const NArrow::NSSA::IDataSource::TDataAddress& /*addr*/) override {
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }

    virtual TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> DoStartFetchHeader(
        const NArrow::NSSA::TProcessorContext& /*context*/, const NArrow::NSSA::IDataSource::TFetchHeaderContext& /*fetchContext*/) override {
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }

    virtual NArrow::TSimpleRow GetStartPKRecordBatch() const override {
        if (GetContext()->GetReadMetadata()->IsDescSorted()) {
            return GetFinish().GetValue();
        } else {
            return GetStart().GetValue();
        }
    }

    virtual NArrow::TSimpleRow GetMinPK() const override {
        return GetStart().GetValue();
    }

    virtual NArrow::TSimpleRow GetMaxPK() const override {
        return GetFinish().GetValue();
    }

    virtual THashMap<TChunkAddress, TString> DecodeBlobAddresses(
        NBlobOperations::NRead::TCompositeReadBlobs&& /*blobsOriginal*/) const override {
        AFL_VERIFY(false);
        return THashMap<TChunkAddress, TString>();
    }

    virtual ui64 GetColumnsVolume(const std::set<ui32>& /*columnIds*/, const NReader::NCommon::EMemType /*type*/) const override {
        return 0;
    }

    virtual ui64 PredictAccessorsSize(const std::set<ui32>& /*entityIds*/) const override {
        return 0;
    }

    virtual ui64 GetColumnRawBytes(const std::set<ui32>& /*columnsIds*/) const override {
        return 0;
    }

    virtual ui64 GetColumnBlobBytes(const std::set<ui32>& /*columnsIds*/) const override {
        return 0;
    }

    virtual ui64 GetIndexRawBytes(const std::set<ui32>& /*indexIds*/) const override {
        return 0;
    }

    virtual NColumnShard::TInternalPathId GetPathId() const override {
        return UnifiedPathId.GetInternalPathId();
    }

    virtual TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> DoStartFetchIndex(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TFetchIndexContext& /*fetchContext*/) override {
        return std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>();
    }

    virtual TConclusion<NArrow::TColumnFilter> DoCheckIndex(const NArrow::NSSA::TProcessorContext& /*context*/,
        const TCheckIndexContext& /*fetchContext*/, const std::shared_ptr<arrow::Scalar>& /*value*/) override {
        return NArrow::TColumnFilter::BuildAllowFilter();
    }

    virtual NJson::TJsonValue DoDebugJson() const override {
        return NJson::JSON_NULL;
    }

    virtual void InitUsedRawBytes() override {
        AFL_VERIFY(!UsedRawBytes);
        UsedRawBytes = GetStageData().GetPortionAccessor().GetColumnRawBytes(GetContext()->GetAllUsageColumns()->GetColumnIds(), false);
        MutableStageData().InitRecordsCount(GetRecordsCount());
    }

    virtual void DoAbort() override {
        return;
    }

    virtual bool HasIndexes(const std::set<ui32>& /*indexIds*/) const override {
        return false;
    }

    virtual bool DoAddTxConflict() override {
        AFL_VERIFY(false);
        return false;
    }

    virtual TConclusion<NArrow::TColumnFilter> DoCheckHeader(
        const NArrow::NSSA::TProcessorContext& /*context*/, const TCheckHeaderContext& /*fetchContext*/) override {
        return TConclusionStatus::Fail("incorrect method usage DoCheckHeader");
    }

    virtual ui32 GetRecordsCountVirtual() const override {
        if (HasStageData()) {
            return GetStageData().GetPortionAccessor().GetRecordsVerified().size() +
                   GetStageData().GetPortionAccessor().GetIndexesVerified().size();
        } else {
            return GetStageResult().GetBatch()->GetRecordsCount();
        }
    }

public:
    TSysViewPortionInfo(const ui32 sourceId, const ui32 sourceIdx, const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId,
        const TPortionInfo::TConstPtr& portion, NArrow::TSimpleRow&& start, NArrow::TSimpleRow&& finish,
        const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context)
        : TBase(sourceId, sourceIdx, context, std::move(start), std::move(finish), portion->RecordSnapshotMin(), portion->RecordSnapshotMin(),
              std::nullopt, std::nullopt, false)
        , UnifiedPathId(pathId)
        , TabletId(tabletId)
        , Portion(portion) {
    }
};

class TPortionDataConstructor {
private:
    NColumnShard::TUnifiedPathId PathId;
    ui64 TabletId;
    YDB_READONLY_DEF(TPortionInfo::TConstPtr, Portion);
    NArrow::TSimpleRow Start;
    NArrow::TSimpleRow Finish;
    ui32 SourceId = 0;
    ui32 SourceIdx = 0;

public:
    void SetIndex(const ui32 index) {
        AFL_VERIFY(!SourceId);
        SourceIdx = index;
        SourceId = index + 1;
    }

    TPortionDataConstructor(const NColumnShard::TUnifiedPathId& pathId, const ui64 tabletId,
        const TPortionInfo::TConstPtr& portion)
        : PathId(pathId)
        , TabletId(tabletId)
        , Portion(portion)
        , Start(NReader::NSysView::NChunks::TSchemaAdapter<NKikimr::NSysView::Schema::PrimaryIndexStats>::GetPKSimpleRow(
              PathId, TabletId, Portion->GetPortionId(), 0, 0))
        , Finish(NReader::NSysView::NChunks::TSchemaAdapter<NKikimr::NSysView::Schema::PrimaryIndexStats>::GetPKSimpleRow(
              PathId, TabletId, Portion->GetPortionId(), Max<ui32>(), Max<ui32>())) {
    }

    const NArrow::TSimpleRow& GetStart() const {
        return Start;
    }
    const NArrow::TSimpleRow& GetFinish() const {
        return Finish;
    }

    struct TComparator {
    private:
        const bool IsReverse;
    public:
        TComparator(const bool isReverse)
            : IsReverse(isReverse)
        {

        }

        bool operator()(const TPortionDataConstructor& l, const TPortionDataConstructor& r) const {
            if (IsReverse) {
                return r.Finish < l.Finish;
            } else {
                return l.Start < r.Start;
            }
        }
    };

    std::shared_ptr<TSysViewPortionInfo> Construct(const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context) {
        AFL_VERIFY(SourceId);
        return std::make_shared<TSysViewPortionInfo>(
            SourceId, SourceIdx, PathId, TabletId, Portion, std::move(Start), std::move(Finish), context);
    }
};

class TSysViewPortionChunksConstructor: public NReader::NCommon::ISourcesConstructor {
private:
    std::deque<TPortionDataConstructor> Constructors;

    virtual void DoClear() override {
        Constructors.clear();
    }
    virtual void DoAbort() override {
        Constructors.clear();
    }
    virtual bool DoIsFinished() const override {
        return Constructors.empty();
    }
    virtual std::shared_ptr<NReader::NCommon::IDataSource> DoExtractNext(
        const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) override {
        AFL_VERIFY(Constructors.size());
        std::shared_ptr<NReader::NCommon::IDataSource> result =
            Constructors.front().Construct(std::static_pointer_cast<NReader::NSimple::TSpecialReadContext>(context));
        Constructors.pop_front();
        return result;
    }
    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& /*cursor*/) override {
        AFL_VERIFY(false);
    }
    virtual TString DoDebugString() const override {
        return Default<TString>();
    }

public:
    TSysViewPortionChunksConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
        const std::optional<NOlap::TInternalPathId> internalPathId, const TSnapshot reqSnapshot,
        const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const bool isReverseSort) {
        const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
        for (auto&& i : engineImpl->GetTables()) {
            if (internalPathId && *internalPathId != i.first) {
                continue;
            }
            for (auto&& [_, p] : i.second->GetPortions()) {
                if (reqSnapshot < p->RecordSnapshotMin()) {
                    continue;
                }
                Constructors.emplace_back(pathIdTranslator.GetUnifiedByInternalVerified(p->GetPathId()), tabletId, p);
                if (!pkFilter->IsUsed(Constructors.back().GetStart(), Constructors.back().GetFinish())) {
                    Constructors.pop_back();
                }
            }
        }
        std::sort(Constructors.begin(), Constructors.end(), TPortionDataConstructor::TComparator(isReverseSort));
        for (ui32 idx = 0; idx < Constructors.size(); ++idx) {
            Constructors[idx].SetIndex(idx);
        }
    }
};

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TSysViewTableAccessor::SelectMetadata(const TSelectMetadataContext& context,
    const NReader::TReadDescription& readDescription, const bool /*withUncommitted*/, const bool isPlain) const {
    AFL_VERIFY(!isPlain);
    if (GetTableName().EndsWith("primary_index_stats")) {
        return std::make_unique<TSysViewPortionChunksConstructor>(context.GetPathIdTranslator(), context.GetEngine(),
            readDescription.GetTabletId(),
            PathId.GetInternalPathId().GetRawValue() ? PathId.GetInternalPathId() : std::optional<NColumnShard::TInternalPathId>(),
            readDescription.GetSnapshot(), readDescription.PKRangesFilter, readDescription.IsReverseSort());
    }
    AFL_VERIFY(false);
    return nullptr;
}

std::shared_ptr<ISnapshotSchema> TSysViewTableAccessor::GetSnapshotSchemaOptional(
    const TVersionedPresetSchemas& vSchemas, const TSnapshot& /*snapshot*/) const {
    if (GetTableName().EndsWith("primary_index_stats")) {
        return vSchemas
            .GetVersionedIndex(NReader::NSysView::NChunks::TSchemaAdapter<NKikimr::NSysView::Schema::PrimaryIndexStats>::GetPresetId())
            .GetLastSchema();
    }
    AFL_VERIFY(false);
    return nullptr;
}

std::shared_ptr<const TVersionedIndex> TSysViewTableAccessor::GetVersionedIndexCopyOptional(TVersionedPresetSchemas& vSchemas) const {
    if (GetTableName().EndsWith("primary_index_stats")) {
        return vSchemas.GetVersionedIndexCopy(
            NReader::NSysView::NChunks::TSchemaAdapter<NKikimr::NSysView::Schema::PrimaryIndexStats>::GetPresetId());
    }
    AFL_VERIFY(false);
    return nullptr;
}

TUserTableAccessor::TUserTableAccessor(const TString& tableName, const NColumnShard::TUnifiedPathId& pathId)
    : TBase(tableName)
    , PathId(pathId) {
    AFL_VERIFY(GetTablePath().find(".sys") == TString::npos);
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TUserTableAccessor::SelectMetadata(const TSelectMetadataContext& context,
    const NReader::TReadDescription& readDescription, const bool withUncommitted, const bool isPlain) const {
    AFL_VERIFY(readDescription.PKRangesFilter);
    std::vector<std::shared_ptr<TPortionInfo>> portions =
        context.GetEngine().Select(PathId.InternalPathId, readDescription.GetSnapshot(), *readDescription.PKRangesFilter, withUncommitted);
    if (!isPlain) {
        std::deque<NReader::NSimple::TSourceConstructor> sources;
        for (auto&& i : portions) {
            sources.emplace_back(NReader::NSimple::TSourceConstructor(std::move(i), readDescription.GetSorting()));
        }
        if (readDescription.GetSorting() != NReader::ERequestSorting::NONE) {
            return std::make_unique<NReader::NSimple::TSortedPortionsSources>(std::move(sources));
        } else {
            return std::make_unique<NReader::NSimple::TNotSortedPortionsSources>(std::move(sources));
        }
    } else {
        return std::make_unique<NReader::NPlain::TPortionSources>(std::move(portions));
    }
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TAbsentTableAccessor::SelectMetadata(const TSelectMetadataContext& /*context*/,
    const NReader::TReadDescription& /*readDescription*/, const bool /*withUncommitted*/, const bool /*isPlain*/) const {
    return std::make_unique<NReader::NSimple::TNotSortedPortionsSources>();
}

}   // namespace NKikimr::NOlap
