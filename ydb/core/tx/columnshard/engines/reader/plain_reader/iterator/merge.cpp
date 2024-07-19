#include "merge.h"
#include "plain_read_data.h"
#include "source.h"

namespace NKikimr::NOlap::NReader::NPlain {

std::optional<NArrow::NMerger::TCursor> TBaseMergeTask::DrainMergerLinearScan(const std::optional<ui32> resultBufferLimit) {
    std::optional<NArrow::NMerger::TCursor> lastResultPosition;
    AFL_VERIFY(!ResultBatch);
    auto rbBuilder = std::make_shared<NArrow::NMerger::TRecordBatchBuilder>(Context->GetProgramInputColumns()->GetSchema()->fields());
    rbBuilder->SetMemoryBufferLimit(resultBufferLimit);
    if (!Merger->DrainToControlPoint(*rbBuilder, MergingContext->GetIncludeFinish(), &lastResultPosition)) {
        if (Merger->IsEmpty()) {
            Merger = nullptr;
        } else {
            AFL_VERIFY(rbBuilder->IsBufferExhausted());
        }
    } else {
        Merger = nullptr;
    }
    Context->GetCommonContext()->GetCounters().OnLinearScanInterval(rbBuilder->GetRecordsCount());
    ResultBatch = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({rbBuilder->Finalize()}));
    return lastResultPosition;
}

void TBaseMergeTask::PrepareResultBatch() {
    if (!ResultBatch || ResultBatch->num_rows() == 0) {
        ResultBatch = nullptr;
        LastPK = nullptr;
        return;
    }
    {
        ResultBatch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(ResultBatch, Context->GetProgramInputColumns()->GetColumnNamesVector());
        AFL_VERIFY((ui32)ResultBatch->num_columns() == Context->GetProgramInputColumns()->GetColumnNamesVector().size());
        NArrow::TStatusValidator::Validate(Context->GetReadMetadata()->GetProgram().ApplyProgram(ResultBatch));
    }
    if (ResultBatch->num_rows()) {
        const auto& shardingPolicy = Context->GetCommonContext()->GetComputeShardingPolicy();
        if (NArrow::THashConstructor::BuildHashUI64(ResultBatch, shardingPolicy.GetColumnNames(), "__compute_sharding_hash")) {
            ShardedBatch = NArrow::TShardingSplitIndex::Apply(shardingPolicy.GetShardsCount(), ResultBatch, "__compute_sharding_hash");
        } else {
            ShardedBatch = NArrow::TShardedRecordBatch(ResultBatch);
        }
        AFL_VERIFY(!!LastPK == !!ShardedBatch->GetRecordsCount())("lpk", !!LastPK)("sb", ShardedBatch->GetRecordsCount());
    } else {
        ResultBatch = nullptr;
        LastPK = nullptr;
    }
}

bool TBaseMergeTask::DoApply(IDataReader& indexedDataRead) const {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoApply")("interval_idx", MergingContext->GetIntervalIdx());
    auto& reader = static_cast<TPlainReadData&>(indexedDataRead);
    reader.MutableScanner().OnIntervalResult(ShardedBatch, LastPK, std::move(Merger), IntervalIdx, reader);
    return true;
}

TConclusionStatus TStartMergeTask::DoExecuteImpl() {
    if (OnlyEmptySources) {
        ResultBatch = nullptr;
        return TConclusionStatus::Success();
    }
    bool sourcesInMemory = true;
    for (auto&& i : Sources) {
        if (!i.second->IsSourceInMemory()) {
            sourcesInMemory = false;
            break;
        }
    }
    if (MergingContext->IsExclusiveInterval() && sourcesInMemory) {
        TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::EXCLUSIVE", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        auto& container = Sources.begin()->second->GetStageResult().GetBatch();
        if (container && container->num_rows()) {
            ResultBatch = container->BuildTableVerified();
            LastPK = Sources.begin()->second->GetLastPK();
            ResultBatch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(ResultBatch, Context->GetProgramInputColumns()->GetColumnNamesVector());
            Context->GetCommonContext()->GetCounters().OnNoScanInterval(ResultBatch->num_rows());
            if (Context->GetCommonContext()->IsReverse()) {
                ResultBatch = NArrow::ReverseRecords(ResultBatch);
            }
            PrepareResultBatch();
        }
        Sources.clear();
        AFL_VERIFY(!!LastPK == (!!ResultBatch && ResultBatch->num_rows()));
        return TConclusionStatus::Success();
    }
    TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::COMMON", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    AFL_VERIFY(!Merger);
    Merger = Context->BuildMerger();
    {
        bool isEmpty = true;
        for (auto&& [_, i] : Sources) {
            if (auto rb = i->GetStageResult().GetBatch()) {
                if (!i->GetStageResult().IsEmpty()) {
                    isEmpty = false;
                }
                Merger->AddSource(rb, i->GetStageResult().GetNotAppliedFilter());
            }
        }
        AFL_VERIFY(Merger->GetSourcesCount() <= Sources.size());
        if (Merger->GetSourcesCount() == 0 || isEmpty) {
            ResultBatch = nullptr;
            return TConclusionStatus::Success();
        }
    }
    Merger->PutControlPoint(MergingContext->GetFinish());
    Merger->SkipToLowerBound(MergingContext->GetStart(), MergingContext->GetIncludeStart());
    const ui32 originalSourcesCount = Sources.size();
    Sources.clear();

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExecute")("interval_idx", MergingContext->GetIntervalIdx());
    std::optional<NArrow::NMerger::TCursor> lastResultPosition;
    if (Merger->GetSourcesCount() == 1 && sourcesInMemory) {
        TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::ONE", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        ResultBatch = Merger->SingleSourceDrain(MergingContext->GetFinish(), MergingContext->GetIncludeFinish(), &lastResultPosition);
        if (ResultBatch) {
            Context->GetCommonContext()->GetCounters().OnLogScanInterval(ResultBatch->num_rows());
            AFL_VERIFY(ResultBatch->schema()->Equals(Context->GetProgramInputColumns()->GetSchema()))("res", ResultBatch->schema()->ToString())("ctx", Context->GetProgramInputColumns()->GetSchema()->ToString());
        }
        if (MergingContext->GetIncludeFinish() && originalSourcesCount == 1) {
            AFL_VERIFY(Merger->IsEmpty())("merging_context_finish", MergingContext->GetFinish().DebugJson().GetStringRobust())("merger", Merger->DebugString());
        }
    } else {
        TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::MANY", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        const std::optional<ui32> bufferLimit = sourcesInMemory ? std::nullopt : std::optional<ui32>(Context->ReadSequentiallyBufferSize);
        lastResultPosition = DrainMergerLinearScan(bufferLimit);
    }
    if (lastResultPosition) {
        LastPK = lastResultPosition->ExtractSortingPosition(MergingContext->GetFinish().GetSortFields());
    }
    AFL_VERIFY(!!LastPK == (!!ResultBatch && ResultBatch->num_rows()));
    PrepareResultBatch();
    return TConclusionStatus::Success();
}

TStartMergeTask::TStartMergeTask(const std::shared_ptr<TMergingContext>& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext, THashMap<ui32, std::shared_ptr<IDataSource>>&& sources)
    : TBase(mergingContext, readContext)
    , Sources(std::move(sources))
{
    for (auto&& s : Sources) {
        AFL_VERIFY(s.second->IsDataReady());
    }
    for (auto&& [_, i] : Sources) {
        if (!i->GetStageResult().IsEmpty()) {
            OnlyEmptySources = false;
        }
    }
}

TConclusionStatus TContinueMergeTask::DoExecuteImpl() {
    TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::CONTINUE", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
    std::optional<NArrow::NMerger::TCursor> lastResultPosition = DrainMergerLinearScan(Context->ReadSequentiallyBufferSize);
    if (lastResultPosition) {
        LastPK = lastResultPosition->ExtractSortingPosition(MergingContext->GetFinish().GetSortFields());
    }
    AFL_VERIFY(!!LastPK == (!!ResultBatch && ResultBatch->num_rows()));
    PrepareResultBatch();
    return TConclusionStatus::Success();
}

}
