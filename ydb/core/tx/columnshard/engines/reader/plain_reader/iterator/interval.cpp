#include "interval.h"
#include "scanner.h"
#include "plain_read_data.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>

namespace NKikimr::NOlap::NReader::NPlain {

class TBaseMergeTask: public IDataTasksProcessor::ITask {
private:
    using TBase = IDataTasksProcessor::ITask;
protected:
    std::shared_ptr<arrow::Table> ResultBatch;
    std::shared_ptr<arrow::RecordBatch> LastPK;
    const NColumnShard::TCounterGuard Guard;
    std::shared_ptr<TSpecialReadContext> Context;
    mutable std::unique_ptr<NArrow::NMerger::TMergePartialStream> Merger;
    std::shared_ptr<TMergingContext> MergingContext;
    const ui32 IntervalIdx;
    std::optional<NArrow::TShardedRecordBatch> ShardedBatch;

    [[nodiscard]] std::optional<NArrow::NMerger::TSortableBatchPosition> DrainMergerLinearScan(const std::optional<ui32> resultBufferLimit) {
        std::optional<NArrow::NMerger::TSortableBatchPosition> lastResultPosition;
        AFL_VERIFY(!ResultBatch);
        auto rbBuilder = std::make_shared<NArrow::NMerger::TRecordBatchBuilder>(Context->GetProgramInputColumns()->GetSchema()->fields());
        rbBuilder->SetMemoryBufferLimit(resultBufferLimit);
        //                rbBuilder->SetMemoryBufferLimit(MergingContext->GetMemoryBufferLimit());
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
private:
    virtual bool DoApply(IDataReader& indexedDataRead) const override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoApply")("interval_idx", MergingContext->GetIntervalIdx());
        auto& reader = static_cast<TPlainReadData&>(indexedDataRead);
        reader.MutableScanner().OnIntervalResult(ShardedBatch, LastPK, std::move(Merger), IntervalIdx, reader);
        return true;
    }
public:
    TBaseMergeTask(const std::shared_ptr<TMergingContext>& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext)
        : TBase(readContext->GetCommonContext()->GetScanActorId())
        , Guard(readContext->GetCommonContext()->GetCounters().GetMergeTasksGuard())
        , Context(readContext)
        , MergingContext(mergingContext)
        , IntervalIdx(MergingContext->GetIntervalIdx()) {

    }
};

class TStartMergeTask: public TBaseMergeTask {
private:
    using TBase = TBaseMergeTask;
    std::map<ui32, std::shared_ptr<IDataSource>> Sources;

    bool EmptyFiltersOnly() const {
        for (auto&& [_, i] : Sources) {
            if (!i->IsEmptyData()) {
                return false;
            }
        }
        return true;
    }
protected:
    virtual bool DoExecute() override {
        if (EmptyFiltersOnly()) {
            ResultBatch = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({NArrow::MakeEmptyBatch(Context->GetProgramInputColumns()->GetSchema())}));
            return true;
        }
        bool sourcesInMemory = true;
        for (auto&& i : Sources) {
            if (!i.second->IsSourceInMemory()) {
                sourcesInMemory = false;
                break;
            }
        }
        if (MergingContext->IsExclusiveInterval() && sourcesInMemory) {
            TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::EXCLUSIVE");
            auto& container = Sources.begin()->second->GetStageResult().GetBatch();
            if (container && container->num_rows()) {
                ResultBatch = container->BuildTable();
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
        TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::COMMON");
        AFL_VERIFY(!Merger);
        Merger = Context->BuildMerger();
        for (auto&& [_, i] : Sources) {
            if (auto rb = i->GetStageResult().GetBatch()) {
                Merger->AddSource(rb, i->GetStageResult().GetNotAppliedFilter());
            }
        }
        AFL_VERIFY(Merger->GetSourcesCount() <= Sources.size());
        if (Merger->GetSourcesCount() == 0) {
            ResultBatch = nullptr;
            return true;
        }
        Merger->PutControlPoint(std::make_shared<NArrow::NMerger::TSortableBatchPosition>(MergingContext->GetFinish()));
        Merger->SkipToLowerBound(MergingContext->GetStart(), MergingContext->GetIncludeStart());
        const ui32 originalSourcesCount = Sources.size();
        Sources.clear();

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "DoExecute")("interval_idx", MergingContext->GetIntervalIdx());
        std::optional<NArrow::NMerger::TSortableBatchPosition> lastResultPosition;
        if (Merger->GetSourcesCount() == 1 && sourcesInMemory) {
            TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::ONE");
            ResultBatch = Merger->SingleSourceDrain(MergingContext->GetFinish(), MergingContext->GetIncludeFinish(), &lastResultPosition);
            if (ResultBatch) {
                Context->GetCommonContext()->GetCounters().OnLogScanInterval(ResultBatch->num_rows());
                AFL_VERIFY(ResultBatch->schema()->Equals(Context->GetProgramInputColumns()->GetSchema()))("res", ResultBatch->schema()->ToString())("ctx", Context->GetProgramInputColumns()->GetSchema()->ToString());
            }
            if (MergingContext->GetIncludeFinish() && originalSourcesCount == 1) {
                AFL_VERIFY(Merger->IsEmpty())("merging_context_finish", MergingContext->GetFinish().DebugJson().GetStringRobust())("merger", Merger->DebugString());
            }
        } else {
            TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::MANY");
            const std::optional<ui32> bufferLimit = sourcesInMemory ? std::nullopt : std::optional<ui32>(Context->ReadSequentiallyBufferSize);
            lastResultPosition = DrainMergerLinearScan(bufferLimit);
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
        return "CS::MERGE_START";
    }

    TStartMergeTask(const std::shared_ptr<TMergingContext>& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext, std::map<ui32, std::shared_ptr<IDataSource>>&& sources)
        : TBase(mergingContext, readContext)
        , Sources(std::move(sources))
    {
        for (auto&& s : Sources) {
            AFL_VERIFY(s.second->IsDataReady());
        }

    }
};

class TContinueMergeTask: public TBaseMergeTask {
private:
    using TBase = TBaseMergeTask;
protected:
    virtual bool DoExecute() override {
        TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::CONTINUE");
        std::optional<NArrow::NMerger::TSortableBatchPosition> lastResultPosition = DrainMergerLinearScan(Context->ReadSequentiallyBufferSize);
        if (lastResultPosition) {
            LastPK = lastResultPosition->ExtractSortingPosition();
        }
        AFL_VERIFY(!!LastPK == (!!ResultBatch && ResultBatch->num_rows()));
        PrepareResultBatch();
        return true;
    }
public:
    virtual TString GetTaskClassIdentifier() const override {
        return "CS::MERGE_CONTINUE";
    }

    TContinueMergeTask(const std::shared_ptr<TMergingContext>& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext, std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger)
        : TBase(mergingContext, readContext) {
        AFL_VERIFY(merger);
        Merger = std::move(merger);
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
        auto task = std::make_shared<TStartMergeTask>(MergingContext, Context, std::move(Sources));
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

TFetchingInterval::TFetchingInterval(const NArrow::NMerger::TSortableBatchPosition& start, const NArrow::NMerger::TSortableBatchPosition& finish,
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
    OnInitResourcesGuard(guard);
}

void TFetchingInterval::SetMerger(std::unique_ptr<NArrow::NMerger::TMergePartialStream>&& merger) {
    AFL_VERIFY(!Merger);
    AFL_VERIFY(AtomicCas(&PartSendingWait, 1, 0));
    Merger = std::move(merger);
}

bool TFetchingInterval::HasMerger() const {
    return !!Merger;
}

void TFetchingInterval::OnPartSendingComplete() {
    AFL_VERIFY(Merger);
    AFL_VERIFY(AtomicCas(&PartSendingWait, 0, 1));
    AFL_VERIFY(AtomicGet(SourcesFinalized) == 1);
    if (AbortedFlag) {
        return;
    }
    auto task = std::make_shared<TContinueMergeTask>(MergingContext, Context, std::move(Merger));
    task->SetPriority(NConveyor::ITask::EPriority::High);
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
}

}
