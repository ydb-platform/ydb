#include "interval.h"
#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TMergeTask: public IDataTasksProcessor::ITask {
private:
    using TBase = IDataTasksProcessor::ITask;
    std::shared_ptr<arrow::RecordBatch> ResultBatch;
    std::shared_ptr<arrow::RecordBatch> LastPK;
    const NColumnShard::TCounterGuard Guard;
    std::shared_ptr<TSpecialReadContext> Context;
    std::map<ui32, std::shared_ptr<IDataSource>> Sources;
    std::shared_ptr<TMergingContext> MergingContext;
    const ui32 IntervalIdx;
    std::optional<NArrow::TShardedRecordBatch> ShardedBatch;

    void PrepareResultBatch() {
        if (!ResultBatch || ResultBatch->num_rows() == 0) {
            ResultBatch = nullptr;
            LastPK = nullptr;
            return;
        }
        {
            ResultBatch = NArrow::ExtractColumns(ResultBatch, Context->GetProgramInputColumns()->GetColumnNamesVector());
            AFL_VERIFY(ResultBatch);
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

    bool EmptyFiltersOnly() const {
        for (auto&& [_, i] : Sources) {
            if (!i->IsEmptyData()) {
                return false;
            }
        }
        return true;
    }
protected:
    virtual bool DoApply(IDataReader& indexedDataRead) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoApply")("interval_idx", MergingContext->GetIntervalIdx());
        auto& reader = static_cast<TPlainReadData&>(indexedDataRead);
        reader.MutableScanner().OnIntervalResult(ShardedBatch, LastPK, IntervalIdx, reader);
        return true;
    }
    virtual bool DoExecute() override {
        if (MergingContext->IsExclusiveInterval()) {
            ResultBatch = Sources.begin()->second->GetStageResult().GetBatch();
            if (ResultBatch && ResultBatch->num_rows()) {
                LastPK = Sources.begin()->second->GetLastPK();
                ResultBatch = NArrow::ExtractColumnsValidate(ResultBatch, Context->GetProgramInputColumns()->GetColumnNamesVector());
                AFL_VERIFY(ResultBatch)("info", Context->GetProgramInputColumns()->GetSchema()->ToString());
                Context->GetCommonContext()->GetCounters().OnNoScanInterval(ResultBatch->num_rows());
                if (Context->GetCommonContext()->IsReverse()) {
                    ResultBatch = NArrow::ReverseRecords(ResultBatch);
                }
                PrepareResultBatch();
            }
            Sources.clear();
            AFL_VERIFY(!!LastPK == (!!ResultBatch && ResultBatch->num_rows()));
            return true;
        }
        if (EmptyFiltersOnly()) {
            ResultBatch = NArrow::MakeEmptyBatch(Context->GetProgramInputColumns()->GetSchema());
            return true;
        }
        std::shared_ptr<NIndexedReader::TMergePartialStream> merger = Context->BuildMerger();
        for (auto&& [_, i] : Sources) {
            if (auto rb = i->GetStageResult().GetBatch()) {
                merger->AddSource(rb, i->GetStageResult().GetNotAppliedFilter());
            }
        }
        AFL_VERIFY(merger->GetSourcesCount() <= Sources.size());
        const ui32 originalSourcesCount = Sources.size();
        Sources.clear();

        if (merger->GetSourcesCount() == 0) {
            ResultBatch = nullptr;
            return true;
        }

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExecute")("interval_idx", MergingContext->GetIntervalIdx());
        merger->SkipToLowerBound(MergingContext->GetStart(), MergingContext->GetIncludeStart());
        std::optional<NIndexedReader::TSortableBatchPosition> lastResultPosition;
        if (merger->GetSourcesCount() == 1) {
            ResultBatch = merger->SingleSourceDrain(MergingContext->GetFinish(), MergingContext->GetIncludeFinish(), &lastResultPosition);
            if (ResultBatch) {
                Context->GetCommonContext()->GetCounters().OnLogScanInterval(ResultBatch->num_rows());
                AFL_VERIFY(ResultBatch->schema()->Equals(Context->GetProgramInputColumns()->GetSchema()))("res", ResultBatch->schema()->ToString())("ctx", Context->GetProgramInputColumns()->GetSchema()->ToString());
            }
            if (MergingContext->GetIncludeFinish() && originalSourcesCount == 1) {
                AFL_VERIFY(merger->IsEmpty())("merging_context_finish", MergingContext->GetFinish().DebugJson().GetStringRobust())("merger", merger->DebugString());
            }
        } else {
            auto rbBuilder = std::make_shared<NIndexedReader::TRecordBatchBuilder>(Context->GetProgramInputColumns()->GetSchema()->fields());
            merger->DrainCurrentTo(*rbBuilder, MergingContext->GetFinish(), MergingContext->GetIncludeFinish(), &lastResultPosition);
            Context->GetCommonContext()->GetCounters().OnLinearScanInterval(rbBuilder->GetRecordsCount());
            ResultBatch = rbBuilder->Finalize();
        }
        if (lastResultPosition) {
            LastPK = lastResultPosition->ExtractSortingPosition();
        }
        AFL_VERIFY(!!LastPK == (!!ResultBatch && ResultBatch->num_rows()));
        PrepareResultBatch();
        return true;
    }
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "CS::MERGE_RESULT";
    }

    TMergeTask(std::shared_ptr<TMergingContext>&& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext, std::map<ui32, std::shared_ptr<IDataSource>>&& sources)
        : TBase(readContext->GetCommonContext()->GetScanActorId())
        , Guard(readContext->GetCommonContext()->GetCounters().GetMergeTasksGuard())
        , Context(readContext)
        , Sources(std::move(sources))
        , MergingContext(std::move(mergingContext))
        , IntervalIdx(MergingContext->GetIntervalIdx())
    {
        for (auto&& s : Sources) {
            AFL_VERIFY(s.second->IsDataReady());
        }

    }
};

void TFetchingInterval::ConstructResult() {
    if (ReadySourcesCount.Val() != WaitSourcesCount || !ReadyGuards.Val()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_construct_result")("interval_idx", IntervalIdx);
        return;
    } else {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "start_construct_result")("interval_idx", IntervalIdx);
    }
    if (AtomicCas(&SourcesFinalized, 1, 0)) {
        auto task = std::make_shared<TMergeTask>(std::move(MergingContext), Context, std::move(Sources));
        task->SetPriority(NConveyor::ITask::EPriority::High);
        NConveyor::TScanServiceOperator::SendTaskToExecute(task);
    }
}

void TFetchingInterval::OnInitResourcesGuard(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "allocated")("interval_idx", IntervalIdx);
    AFL_VERIFY(guard);
    AFL_VERIFY(!ResourcesGuard);
    ResourcesGuard = guard;
    AFL_VERIFY(ReadyGuards.Inc() <= 1);
    ConstructResult();
}

void TFetchingInterval::OnSourceFetchStageReady(const ui32 /*sourceIdx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "fetched")("interval_idx", IntervalIdx);
    AFL_VERIFY(ReadySourcesCount.Inc() <= WaitSourcesCount);
    ConstructResult();
}

TFetchingInterval::TFetchingInterval(const NIndexedReader::TSortableBatchPosition& start, const NIndexedReader::TSortableBatchPosition& finish,
    const ui32 intervalIdx, const std::map<ui32, std::shared_ptr<IDataSource>>& sources, const std::shared_ptr<TSpecialReadContext>& context,
    const bool includeFinish, const bool includeStart, const bool isExclusiveInterval)
    : TTaskBase(0, context->GetMemoryForSources(sources, isExclusiveInterval), "", context->GetCommonContext()->GetResourcesTaskContext())
    , MergingContext(std::make_shared<TMergingContext>(start, finish, intervalIdx, includeFinish, includeStart, isExclusiveInterval))
    , Context(context)
    , TaskGuard(Context->GetCommonContext()->GetCounters().GetResourcesAllocationTasksGuard())
    , Sources(sources)
    , IntervalIdx(intervalIdx)
{
    Y_ABORT_UNLESS(Sources.size());
    for (auto&& [_, i] : Sources) {
        if (!i->IsDataReady()) {
            ++WaitSourcesCount;
        }
        i->RegisterInterval(*this);
    }
}

void TFetchingInterval::DoOnAllocationSuccess(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& guard) {
    AFL_VERIFY(guard);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("interval_idx", IntervalIdx)("event", "resources_allocated")
        ("resources", guard->DebugString())("start", MergingContext->GetIncludeStart())("finish", MergingContext->GetIncludeFinish())("sources", Sources.size());
    for (auto&& [_, i] : Sources) {
        i->InitFetchingPlan(Context->GetColumnsFetchingPlan(i, MergingContext->IsExclusiveInterval()), i, MergingContext->IsExclusiveInterval());
    }
    OnInitResourcesGuard(guard);
}

}
