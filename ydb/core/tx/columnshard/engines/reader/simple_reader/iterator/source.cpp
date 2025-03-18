#include "fetched_data.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/blobs_reader/events.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/default_fetching.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/sub_columns_fetching.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NOlap::NReader::NSimple {

void IDataSource::InitFetchingPlan(const std::shared_ptr<TFetchingScript>& fetching) {
    AFL_VERIFY(fetching);
    //    AFL_VERIFY(!FetchingPlan);
    FetchingPlan = fetching;
}

void IDataSource::StartProcessing(const std::shared_ptr<IDataSource>& sourcePtr) {
    AFL_VERIFY(!ProcessingStarted);
    AFL_VERIFY(FetchingPlan);
    ProcessingStarted = true;
    SourceGroupGuard = NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildGroupGuard(
        GetContext()->GetProcessMemoryControlId(), GetContext()->GetCommonContext()->GetScanId());
    SetMemoryGroupId(SourceGroupGuard->GetGroupId());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("InitFetchingPlan", FetchingPlan->DebugString())("source_idx", GetSourceIdx());
    //    NActors::TLogContextGuard logGuard(NActors::TLogContextBuilder::Build()("source", SourceIdx)("method", "InitFetchingPlan"));
    TFetchingScriptCursor cursor(FetchingPlan, 0);
    auto task = std::make_shared<TStepAction>(sourcePtr, std::move(cursor), GetContext()->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
}

void IDataSource::ContinueCursor(const std::shared_ptr<IDataSource>& sourcePtr) {
    AFL_VERIFY(!!ScriptCursor);
    if (ScriptCursor->Next()) {
        auto task = std::make_shared<TStepAction>(sourcePtr, std::move(*ScriptCursor), GetContext()->GetCommonContext()->GetScanActorId());
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
        ScriptCursor.reset();
    }
}

void IDataSource::DoOnSourceFetchingFinishedSafe(IDataReader& owner, const std::shared_ptr<NCommon::IDataSource>& sourcePtr) {
    auto* plainReader = static_cast<TPlainReadData*>(&owner);
    plainReader->MutableScanner().OnSourceReady(std::static_pointer_cast<IDataSource>(sourcePtr), nullptr, 0, GetRecordsCount(), *plainReader);
}

void IDataSource::DoOnEmptyStageData(const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) {
    TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT_EMPTY", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    ResourceGuards.clear();
    StageResult = TFetchedResult::BuildEmpty();
    StageResult->SetPages({ TPortionDataAccessor::TReadPage(0, GetRecordsCount(), 0) });
    StageData.reset();
}

void IDataSource::DoBuildStageResult(const std::shared_ptr<NCommon::IDataSource>& /*sourcePtr*/) {
    Finalize(NYDBTest::TControllers::GetColumnShardController()->GetMemoryLimitScanPortion());
}

void IDataSource::Finalize(const std::optional<ui64> memoryLimit) {
    TMemoryProfileGuard mpg("SCAN_PROFILE::STAGE_RESULT", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    if (memoryLimit && !IsSourceInMemory()) {
        const auto accessor = StageData->GetPortionAccessor();
        StageResult = std::make_unique<TFetchedResult>(std::move(StageData), *GetContext()->GetCommonContext()->GetResolver());
        StageResult->SetPages(accessor.BuildReadPages(*memoryLimit, GetContext()->GetProgramInputColumns()->GetColumnIds()));
    } else {
        StageResult = std::make_unique<TFetchedResult>(std::move(StageData), *GetContext()->GetCommonContext()->GetResolver());
        StageResult->SetPages({ TPortionDataAccessor::TReadPage(0, GetRecordsCount(), 0) });
    }
    StageData.reset();
}

void TPortionDataSource::NeedFetchColumns(const std::set<ui32>& columnIds, TBlobsAction& blobsAction,
    THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>& defaultBlocks, const std::shared_ptr<NArrow::TColumnFilter>& filter) {
    const NArrow::TColumnFilter& cFilter = filter ? *filter : NArrow::TColumnFilter::BuildAllowFilter();
    ui32 fetchedChunks = 0;
    ui32 nullChunks = 0;
    for (auto&& i : columnIds) {
        auto columnChunks = GetStageData().GetPortionAccessor().GetColumnChunksPointers(i);
        if (columnChunks.empty()) {
            continue;
        }
        auto itFilter = cFilter.GetIterator(false, Portion->GetRecordsCount());
        bool itFinished = false;
        for (auto&& c : columnChunks) {
            AFL_VERIFY(!itFinished);
            if (!itFilter.IsBatchForSkip(c->GetMeta().GetRecordsCount())) {
                auto reading = blobsAction.GetReading(Portion->GetColumnStorageId(c->GetColumnId(), Schema->GetIndexInfo()));
                reading->SetIsBackgroundProcess(false);
                reading->AddRange(Portion->RestoreBlobRange(c->BlobRange));
                ++fetchedChunks;
            } else {
                defaultBlocks.emplace(c->GetAddress(), TPortionDataAccessor::TAssembleBlobInfo(c->GetMeta().GetRecordsCount(),
                                                           Schema->GetExternalDefaultValueVerified(c->GetColumnId())));
                ++nullChunks;
            }
            itFinished = !itFilter.Next(c->GetMeta().GetRecordsCount());
        }
        AFL_VERIFY(itFinished)("filter", itFilter.DebugString())("count", Portion->GetRecordsCount());
    }
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "chunks_stats")("fetch", fetchedChunks)("null", nullChunks)(
        "reading_actions", blobsAction.GetStorageIds())("columns", columnIds.size());
}

bool TPortionDataSource::DoStartFetchingColumns(
    const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step, const TColumnsSetIds& columns) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName());
    AFL_VERIFY(columns.GetColumnsCount());
    AFL_VERIFY(!StageData->GetAppliedFilter() || !StageData->GetAppliedFilter()->IsTotalDenyFilter());
    auto& columnIds = columns.GetColumnIds();
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    TBlobsAction action(GetContext()->GetCommonContext()->GetStoragesManager(), NBlobOperations::EConsumer::SCAN);
    {
        THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo> nullBlocks;
        NeedFetchColumns(columnIds, action, nullBlocks, StageData->GetAppliedFilter());
        StageData->AddDefaults(std::move(nullBlocks));
    }

    auto readActions = action.GetReadingActions();
    if (!readActions.size()) {
        return false;
    }

    auto constructor =
        std::make_shared<NCommon::TBlobsFetcherTask>(readActions, sourcePtr, step, GetContext(), "CS::READ::" + step.GetName(), "");
    NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(constructor));
    return true;
}

std::shared_ptr<NIndexes::TSkipIndex> TPortionDataSource::SelectOptimalIndex(
    const std::vector<std::shared_ptr<NIndexes::TSkipIndex>>& indexes, const NArrow::NSSA::EIndexCheckOperation /*op*/) const {
    if (indexes.size() == 0) {
        return nullptr;
    }
    if (indexes.size() == 1) {
        return indexes.front();
    }
    return indexes.front();
}

TConclusion<bool> TPortionDataSource::DoStartFetchImpl(
    const NArrow::NSSA::TProcessorContext& context, const std::vector<std::shared_ptr<NCommon::IKernelFetchLogic>>& fetchersExt) {
    TReadActionsCollection readActions;
    auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
    NCommon::TFetchingResultContext contextFetch(*GetStageData().GetTable(), *GetStageData().GetIndexes(), source);
    for (auto&& i : fetchersExt) {
        i->Start(readActions, contextFetch);
    }
    if (readActions.IsEmpty()) {
        for (auto&& i : fetchersExt) {
            NBlobOperations::NRead::TCompositeReadBlobs blobs;
            i->OnDataReceived(readActions, blobs);
            MutableStageData().AddFetcher(i);
            AFL_VERIFY(readActions.IsEmpty());
        }
        return false;
    }
    THashMap<ui32, std::shared_ptr<NCommon::IKernelFetchLogic>> fetchers;
    for (auto&& i : fetchersExt) {
        AFL_VERIFY(fetchers.emplace(i->GetEntityId(), i).second);
    }
    NActors::TActivationContext::AsActorContext().Register(
        new NOlap::NBlobOperations::NRead::TActor(std::make_shared<NCommon::TColumnsFetcherTask>(
            std::move(readActions), fetchers, source, GetExecutionContext().GetCursorStep(), "fetcher", "")));
    return true;
}

TConclusion<std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>>> TPortionDataSource::DoStartFetchIndex(
    const NArrow::NSSA::TProcessorContext& /*context*/, const TFetchIndexContext& indexContext) {
    THashMap<TCheckIndexContext, std::shared_ptr<NIndexes::IIndexMeta>> indexInfo;
    for (auto&& i : indexContext.GetOperationsBySubColumn().GetData()) {
        NIndexes::NRequest::TOriginalDataAddress addr(indexContext.GetColumnId(), i.first);
        for (auto&& op : i.second) {
            auto indexMeta = MutableStageData().GetIndexes()->FindIndexFor(addr, op);
            TCheckIndexContext checkAddr(indexContext.GetColumnId(), i.first, op);
            if (!indexMeta) {
                const auto indexesMeta = GetSourceSchema()->GetIndexInfo().FindSkipIndexes(addr, op);
                if (indexesMeta.empty()) {
                    MutableStageData().AddRemapDataToIndex(checkAddr, nullptr);
                    continue;
                }
                indexMeta = SelectOptimalIndex(indexesMeta, op);
                if (!indexMeta) {
                    MutableStageData().AddRemapDataToIndex(checkAddr, nullptr);
                    continue;
                }
            }
            AFL_VERIFY(indexInfo.emplace(checkAddr, indexMeta).second);
            MutableStageData().AddRemapDataToIndex(checkAddr, indexMeta);
        }
    }
    THashMap<ui32, THashSet<NIndexes::NRequest::TOriginalDataAddress>> addresses;
    for (auto&& [check, index] : indexInfo) {
        const NIndexes::NRequest::TOriginalDataAddress addr(check.GetColumnId(), check.GetSubColumnName());
        addresses[index->GetIndexId()].emplace(addr);
    }
    std::vector<std::shared_ptr<NArrow::NSSA::IFetchLogic>> result;
    for (auto&& i : addresses) {
        auto indexMeta = GetSourceSchema()->GetIndexInfo().GetIndexVerified(i.first);
        result.emplace_back(
            indexMeta->BuildFetchTask(i.second, indexMeta.GetObjectPtrVerified(), GetContext()->GetCommonContext()->GetStoragesManager()));
    }
    return result;
}

TConclusion<NArrow::TColumnFilter> TPortionDataSource::DoCheckIndex(
    const NArrow::NSSA::TProcessorContext& context, const TCheckIndexContext& fetchContext, const std::shared_ptr<arrow::Scalar>& value) {
    auto meta = MutableStageData().GetRemapDataToIndex(fetchContext);
    if (!meta) {
        NYDBTest::TControllers::GetColumnShardController()->OnIndexSelectProcessed({});
        return NArrow::TColumnFilter::BuildAllowFilter();
    }
    AFL_VERIFY(meta->IsSkipIndex());

    if (auto fetcher = MutableStageData().ExtractFetcherOptional(meta->GetIndexId())) {
        auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
        NCommon::TFetchingResultContext fetchContext(*GetStageData().GetTable(), *GetStageData().GetIndexes(), source);
        fetcher->OnDataCollected(fetchContext);
    }

    NArrow::TColumnFilter filter = NArrow::TColumnFilter::BuildAllowFilter();

    const std::optional<ui64> cat = meta->CalcCategory(fetchContext.GetSubColumnName());
    const NIndexes::TIndexColumnChunked* infoPointer = GetStageData().GetIndexes()->GetIndexDataOptional(meta->GetIndexId());
    if (!infoPointer) {
        return filter;
    }
    const auto info = *infoPointer;
    for (auto&& i : info.GetChunks()) {
        const TString data = i.GetData(cat);
        if (std::static_pointer_cast<NIndexes::TSkipIndex>(meta)->CheckValue(data, cat, value, fetchContext.GetOperation())) {
            filter.Add(true, i.GetRecordsCount());
            NYDBTest::TControllers::GetColumnShardController()->OnIndexSelectProcessed(true);
            GetContext()->GetCommonContext()->GetCounters().OnAcceptedByIndex(i.GetRecordsCount());
        } else {
            filter.Add(false, i.GetRecordsCount());
            NYDBTest::TControllers::GetColumnShardController()->OnIndexSelectProcessed(false);
            GetContext()->GetCommonContext()->GetCounters().OnDeniedByIndex(i.GetRecordsCount());
        }
    }
    return filter.And(GetStageData().GetTable()->GetFilter());
}

void TPortionDataSource::DoAbort() {
}

TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> TPortionDataSource::DoStartFetchHeader(
    const NArrow::NSSA::TProcessorContext& context, const TFetchHeaderContext& fetchContext) {
    if (context.GetResources()->GetAccessorOptional(fetchContext.GetColumnId())) {
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }
    std::shared_ptr<NCommon::IKernelFetchLogic> fetcher;
    const ui32 columnId = fetchContext.GetColumnId();
    auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
    if (GetStageData().GetPortionAccessor().GetColumnChunksPointers(columnId).size() &&
        GetSourceSchema()->GetColumnLoaderVerified(columnId)->GetAccessorConstructor()->GetType() ==
            NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
        return std::make_shared<NCommon::TSubColumnsFetchLogic>(columnId, source, std::vector<TString>());
    } else {
        return std::shared_ptr<NArrow::NSSA::IFetchLogic>();
    }
}

TConclusion<NArrow::TColumnFilter> TPortionDataSource::DoCheckHeader(
    const NArrow::NSSA::TProcessorContext& context, const TCheckHeaderContext& fetchContext) {
    auto result = NArrow::TColumnFilter::BuildAllowFilter();
    auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
    {
        if (auto fetcher = MutableStageData().ExtractFetcherOptional(fetchContext.GetColumnId())) {
            NCommon::TFetchingResultContext fetchContext(*GetStageData().GetTable(), *GetStageData().GetIndexes(), source);
            fetcher->OnDataCollected(fetchContext);
        } else {
            NYDBTest::TControllers::GetColumnShardController()->OnHeaderSelectProcessed({});
            return result;
        }
    }

    auto acc = context.GetResources()->GetAccessorVerified(fetchContext.GetColumnId());
    NArrow::NAccessor::IChunkedArray::VisitDataOwners<bool>(acc, [&](const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& arrData) {
        bool isAllowed = false;
        if (arrData->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsPartialArray) {
            const auto* data = static_cast<const NArrow::NAccessor::TSubColumnsPartialArray*>(arrData.get());
            isAllowed = data->GetHeader().HasSubColumn(fetchContext.GetSubColumnName());
        } else if (arrData->GetType() == NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
            const auto* data = static_cast<const NArrow::NAccessor::TSubColumnsArray*>(arrData.get());
            isAllowed = data->HasSubColumn(fetchContext.GetSubColumnName());
        } else {
            AFL_VERIFY(false);
        }
        result.Add(isAllowed, arrData->GetRecordsCount());
        NYDBTest::TControllers::GetColumnShardController()->OnHeaderSelectProcessed(isAllowed);
        if (isAllowed) {
            GetContext()->GetCommonContext()->GetCounters().OnAcceptedByHeader(source->GetRecordsCount());
        } else {
            GetContext()->GetCommonContext()->GetCounters().OnDeniedByHeader(source->GetRecordsCount());
        }

        return false;
    });
    return result;
}

TConclusion<std::shared_ptr<NArrow::NSSA::IFetchLogic>> TPortionDataSource::DoStartFetchData(
    const NArrow::NSSA::TProcessorContext& context, const TDataAddress& addr) {
    std::shared_ptr<NCommon::IKernelFetchLogic> fetcher;
    auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
    if (addr.HasSubColumns() && GetStageData().GetPortionAccessor().GetColumnChunksPointers(addr.GetColumnId()).size() &&
        GetSourceSchema()->GetColumnLoaderVerified(addr.GetColumnId())->GetAccessorConstructor()->GetType() ==
            NArrow::NAccessor::IChunkedArray::EType::SubColumnsArray) {
        return std::make_shared<NCommon::TSubColumnsFetchLogic>(
            addr.GetColumnId(), source, std::vector<TString>(addr.GetSubColumnNames(false).begin(), addr.GetSubColumnNames(false).end()));
    } else {
        return std::make_shared<NCommon::TDefaultFetchLogic>(addr.GetColumnId(), GetContext()->GetCommonContext()->GetStoragesManager());
    }
}

void TPortionDataSource::DoAssembleAccessor(
    const NArrow::NSSA::TProcessorContext& context, const ui32 columnId, const TString& /*subColumnName*/) {
    auto source = context.GetDataSourceVerifiedAs<NCommon::IDataSource>();
    NCommon::TFetchingResultContext fetchContext(*GetStageData().GetTable(), *GetStageData().GetIndexes(), source);
    if (auto fetcher = MutableStageData().ExtractFetcherOptional(columnId)) {
        fetcher->OnDataCollected(fetchContext);
    }
}

void TPortionDataSource::DoAssembleColumns(const std::shared_ptr<TColumnsSet>& columns, const bool sequential) {
    auto blobSchema = GetContext()->GetReadMetadata()->GetLoadSchemaVerified(*Portion);

    std::optional<TSnapshot> ss;
    if (Portion->HasInsertWriteId()) {
        if (Portion->HasCommitSnapshot()) {
            ss = Portion->GetCommitSnapshotVerified();
        } else if (GetContext()->GetReadMetadata()->IsMyUncommitted(Portion->GetInsertWriteIdVerified())) {
            ss = GetContext()->GetReadMetadata()->GetRequestSnapshot();
        }
    }

    auto batch = GetStageData()
                     .GetPortionAccessor()
                     .PrepareForAssemble(*blobSchema, columns->GetFilteredSchemaVerified(), MutableStageData().MutableBlobs(), ss)
                     .AssembleToGeneralContainer(sequential ? columns->GetColumnIds() : std::set<ui32>())
                     .DetachResult();

    MutableStageData().AddBatch(batch, *GetContext()->GetCommonContext()->GetResolver(), true);
}

namespace {
class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    TFetchingScriptCursor Step;
    std::shared_ptr<IDataSource> Source;
    const NColumnShard::TCounterGuard Guard;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override {
        return Source->GetContext()->GetCommonContext()->GetAbortionFlag();
    }

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override {
        AFL_VERIFY(!result.HasErrors());
        AFL_VERIFY(result.GetPortions().size() == 1)("count", result.GetPortions().size());
        Source->MutableStageData().SetPortionAccessor(std::move(result.ExtractPortionsVector().front()));
        Source->InitUsedRawBytes();
        AFL_VERIFY(Step.Next());
        auto task = std::make_shared<TStepAction>(Source, std::move(Step), Source->GetContext()->GetCommonContext()->GetScanActorId());
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    }

public:
    TPortionAccessorFetchingSubscriber(const TFetchingScriptCursor& step, const std::shared_ptr<IDataSource>& source)
        : Step(step)
        , Source(source)
        , Guard(Source->GetContext()->GetCommonContext()->GetCounters().GetFetcherAcessorsGuard()) {
    }
};

}   // namespace

bool TPortionDataSource::DoStartFetchingAccessor(const std::shared_ptr<IDataSource>& sourcePtr, const TFetchingScriptCursor& step) {
    AFL_VERIFY(!StageData->HasPortionAccessor());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", step.GetName())("fetching_info", step.DebugString());

    std::shared_ptr<TDataAccessorsRequest> request = std::make_shared<TDataAccessorsRequest>("SIMPLE::" + step.GetName());
    request->AddPortion(Portion);
    request->SetColumnIds(GetContext()->GetAllUsageColumns()->GetColumnIds());
    request->RegisterSubscriber(std::make_shared<TPortionAccessorFetchingSubscriber>(step, sourcePtr));
    GetContext()->GetCommonContext()->GetDataAccessorsManager()->AskData(request);
    return true;
}

TPortionDataSource::TPortionDataSource(
    const ui32 sourceIdx, const std::shared_ptr<TPortionInfo>& portion, const std::shared_ptr<TSpecialReadContext>& context)
    : TBase(portion->GetPortionId(), sourceIdx, context, portion->IndexKeyStart(), portion->IndexKeyEnd(),
          portion->RecordSnapshotMin(TSnapshot::Zero()), portion->RecordSnapshotMax(TSnapshot::Zero()), portion->GetRecordsCount(),
          portion->GetShardingVersionOptional(), portion->GetMeta().GetDeletionsCount())
    , Portion(portion)
    , Schema(GetContext()->GetReadMetadata()->GetLoadSchemaVerified(*portion)) {
}

}   // namespace NKikimr::NOlap::NReader::NSimple
