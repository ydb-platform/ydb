#include "merge.h"
#include "plain_read_data.h"
#include "source.h"

#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/formats/arrow/reader/result_builder.h>
#include <ydb/core/formats/arrow/reader/stats/stats.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

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
    ResultBatch = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({ rbBuilder->Finalize() }));
    return lastResultPosition;
}

TConclusionStatus TBaseMergeTask::PrepareResultBatch() {
    const auto collector = Context->GetCommonContext()->GetRuntimeStats();
    if (!ResultBatch || ResultBatch->num_rows() == 0) {
        AllocationGuard = nullptr;
        ResultBatch = nullptr;
        LastPK = nullptr;
        return TConclusionStatus::Success();
    }
    const ui64 dataSizeBefore = NArrow::GetTableDataSize(ResultBatch);
    const ui64 memorySizeBefore = NArrow::GetTableMemorySize(ResultBatch);
    {
        ResultBatch = NArrow::TColumnOperator().VerifyIfAbsent().Extract(ResultBatch, Context->GetProgramInputColumns()->GetColumnNamesVector());
        AFL_VERIFY((ui32)ResultBatch->num_columns() == Context->GetProgramInputColumns()->GetColumnNamesVector().size());
        auto accessors = std::make_unique<NArrow::NAccessor::TAccessorsCollection>(ResultBatch, *Context->GetCommonContext()->GetResolver());
        auto programStats =
            collector ? collector->Start(NScanStats::TStep::Reader, "PROGRAM_EXECUTION", "Execute interval program", NScanStats::Current())
                      : nullptr;
        auto conclusion = [&] {
            NScanStats::TScope programScope(programStats, collector);
            return Context->GetReadMetadata()->GetProgram().ApplyProgram(std::move(accessors));
        }();
        if (programStats) {
            programStats->Finish(conclusion.IsFail() ? NScanStats::TStep::failed : NScanStats::TStep::completed);
        }
        if (conclusion.IsFail()) {
            return conclusion;
        }
        accessors = conclusion.DetachResult();
        if (!accessors->HasSomeUsefulInfo()) {
            ResultBatch = nullptr;
        } else {
            auto buildStats = collector ? collector->Start(NScanStats::TStep::BuildResult, "BUILD_RESULT", "Materialize interval result",
                                              NScanStats::Current())
                                        : nullptr;
            if (buildStats) {
                buildStats->Update([&](NScanStats::TStep& record) {
                    if (const auto rows = accessors->GetRecordsCountActualOptional()) {
                        record.MutableDetails()->MutableBuildResult()->SetPageRows(*rows);
                    }
                });
            }
            {
                NScanStats::TScope buildScope(buildStats, collector);
                ResultBatch = accessors->ToTable(std::nullopt, Context->GetCommonContext()->GetResolver(), false);
            }
            if (buildStats) {
                buildStats->Update([&](NScanStats::TStep& record) {
                    auto* details = record.MutableDetails()->MutableBuildResult();
                    details->SetResultRows(ResultBatch ? ResultBatch->num_rows() : 0);
                    details->SetResultBytes(ResultBatch ? NArrow::GetTableDataSize(ResultBatch) : 0);
                });
                buildStats->Finish();
            }
        }
    }
    if (ResultBatch && ResultBatch->num_rows()) {
        auto outputStats =
            collector ? collector->Start(NScanStats::TStep::Ready, "OUTPUT_SHARDING", "Prepare output partitioning", NScanStats::Current())
                      : nullptr;
        {
            NScanStats::TScope outputScope(outputStats, collector);
            const auto& shardingPolicy = Context->GetCommonContext()->GetComputeShardingPolicy();
            if (NArrow::NHash::THashConstructor::BuildHashUI64(ResultBatch, shardingPolicy.GetColumnNames(), "__compute_sharding_hash")) {
                ShardedBatch = NArrow::TShardingSplitIndex::Apply(shardingPolicy.GetShardsCount(), ResultBatch, "__compute_sharding_hash");
            } else {
                ShardedBatch = NArrow::TShardedRecordBatch(ResultBatch);
            }
        }
        if (outputStats) {
            outputStats->Finish();
        }
        YDB_LOG_DEBUG("",
            {"event", "update_memory_merger"},
            {"beforeData", dataSizeBefore},
            {"beforeMemory", memorySizeBefore},
            {"afterMemory", NArrow::GetTableMemorySize(ResultBatch)},
            {"afterData", NArrow::GetTableDataSize(ResultBatch)},
            {"guard", AllocationGuard->GetMemory()});
        //        AllocationGuard->Update(NArrow::GetTableMemorySize(ResultBatch));
        AFL_VERIFY(!!LastPK == !!ShardedBatch->GetRecordsCount())("lpk", !!LastPK)("sb", ShardedBatch->GetRecordsCount());
    } else {
        AllocationGuard = nullptr;
        ResultBatch = nullptr;
        LastPK = nullptr;
    }
    return TConclusionStatus::Success();
}

bool TBaseMergeTask::DoApply(IDataReader& indexedDataRead) {
    YDB_LOG_DEBUG("",
        {"event", "DoApply"},
        {"intervalIdx", MergingContext->GetIntervalIdx()});
    auto& reader = static_cast<TPlainReadData&>(indexedDataRead);
    auto copy = AllocationGuard;
    reader.MutableScanner().OnIntervalResult(std::move(copy), std::move(ShardedBatch), LastPK, std::move(Merger), IntervalIdx, reader);
    return true;
}

bool TBaseMergeTask::DoOnAllocated(
    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) {
    if (Context->IsAborted()) {
        guard->Release();
        return false;
    }
    AllocationGuard = std::move(guard);
    Context->GetCommonContext()->SendTaskToExecute(static_pointer_cast<TBaseMergeTask>(allocation));
    return true;
}

TConclusion<bool> TStartMergeTask::DoExecuteImpl() {
    const auto collector = Context->GetCommonContext()->GetRuntimeStats();
    auto preparationStats =
        collector ? collector->Start(NScanStats::TStep::Reader, "SOURCE_PREPARATION", "Prepare interval input", {}) : nullptr;
    if (preparationStats) {
        preparationStats->Update([](NScanStats::TStep& record) {
            record.SetMetaStep("PKFilter");
            record.SetCanonicalPath("");
        });
    }
    NScanStats::TScope preparationScope(preparationStats, collector);
    std::shared_ptr<NScanStats::TOperation> mergeStats;
    auto statsConclusion = [&]() -> TConclusion<bool> {
        if (OnlyEmptySources) {
            ResultBatch = nullptr;
            return true;
        }
        bool sourcesInMemory = true;
        for (auto&& i : Sources) {
            if (!i.second->IsSourceInMemory()) {
                sourcesInMemory = false;
                break;
            }
        }
        if ((MergingContext->IsExclusiveInterval()) && sourcesInMemory) {
            TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::EXCLUSIVE", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
            auto& container = Sources.begin()->second->GetStageResult().GetBatch();
            if (container && container->num_rows()) {
                ResultBatch = container->BuildTableVerified();
                LastPK = Sources.begin()->second->GetLastPK();
                ResultBatch =
                    NArrow::TColumnOperator().VerifyIfAbsent().Extract(ResultBatch, Context->GetProgramInputColumns()->GetColumnNamesVector());
                Context->GetCommonContext()->GetCounters().OnNoScanInterval(ResultBatch->num_rows());
                if (Context->GetCommonContext()->IsReverse()) {
                    ResultBatch = NArrow::ReverseRecords(ResultBatch);
                }
                auto conclusion = PrepareResultBatch();
                if (conclusion.IsFail()) {
                    return conclusion;
                }
            }
            Sources.clear();
            AFL_VERIFY(!!LastPK == (!!ResultBatch && ResultBatch->num_rows()));
            return true;
        }
        TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::COMMON", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        if (collector) {
            mergeStats = collector->Start(NScanStats::TStep::Merge, "MERGE_INTERVAL", "Resolve visible key versions", NScanStats::Current());
        }
        NScanStats::TScope mergeScope(mergeStats, collector);
        AFL_VERIFY(!Merger);
        Merger = Context->BuildMerger();
        {
            bool isEmpty = true;
            for (auto&& [_, i] : Sources) {
                if (auto rb = i->GetStageResult().GetBatch()) {
                    if (!i->GetStageResult().IsEmpty()) {
                        isEmpty = false;
                    }
                    Merger->AddSource(rb, i->GetStageResult().GetNotAppliedFilter(),
                        NArrow::NMerger::TIterationOrder(Context->GetCommonContext()->IsReverse(), 0));
                }
            }
            AFL_VERIFY(Merger->GetSourcesCount() <= Sources.size());
            if (Merger->GetSourcesCount() == 0 || isEmpty) {
                ResultBatch = nullptr;
                return true;
            }
        }
        Merger->PutControlPoint(MergingContext->GetFinish(), false);
        Merger->SkipToBound(MergingContext->GetStart(), MergingContext->GetIncludeStart());
        const ui32 originalSourcesCount = Sources.size();
        Sources.clear();

        YDB_LOG_DEBUG("",
        {"event", "DoExecute"},
        {"intervalIdx", MergingContext->GetIntervalIdx()});
        std::optional<NArrow::NMerger::TCursor> lastResultPosition;
        if (Merger->GetSourcesCount() == 1 && sourcesInMemory) {
            TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::ONE", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
            ResultBatch = Merger->SingleSourceDrain(MergingContext->GetFinish(), MergingContext->GetIncludeFinish(), &lastResultPosition);
            if (ResultBatch) {
                Context->GetCommonContext()->GetCounters().OnLogScanInterval(ResultBatch->num_rows());
                AFL_VERIFY(ResultBatch->schema()->Equals(Context->GetProgramInputColumns()->GetSchema()))(
                    "res", ResultBatch->schema()->ToString())("ctx", Context->GetProgramInputColumns()->GetSchema()->ToString());
            }
            if (MergingContext->GetIncludeFinish() && originalSourcesCount == 1) {
                AFL_VERIFY(Merger->IsEmpty())("merging_context_finish", MergingContext->GetFinish().DebugJson().GetStringRobust())(
                    "merger", Merger->DebugString());
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
        auto statusConclusion = PrepareResultBatch();
        if (statusConclusion.IsFail()) {
            return statusConclusion;
        }
        return true;
    }();
    const auto outcome = statsConclusion.IsFail() ? NScanStats::TStep::failed : NScanStats::TStep::completed;
    if (mergeStats) {
        mergeStats->Finish(outcome);
    }
    if (preparationStats) {
        preparationStats->Finish(outcome);
    }
    return statsConclusion;
}

TStartMergeTask::TStartMergeTask(const std::shared_ptr<TMergingContext>& mergingContext, const std::shared_ptr<TSpecialReadContext>& readContext,
    THashMap<ui32, std::shared_ptr<const IDataSource>>&& sources)
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

TConclusion<bool> TContinueMergeTask::DoExecuteImpl() {
    const auto collector = Context->GetCommonContext()->GetRuntimeStats();
    auto mergeStats = collector ? collector->Start(NScanStats::TStep::Merge, "MERGE_INTERVAL", "Continue visible-version merge", {}) : nullptr;
    NScanStats::TScope mergeScope(mergeStats, collector);
    auto statsConclusion = [&]() -> TConclusion<bool> {
        TMemoryProfileGuard mGuard("SCAN_PROFILE::MERGE::CONTINUE", IS_DEBUG_LOG_ENABLED(NKikimrServices::TX_COLUMNSHARD_SCAN_MEMORY));
        std::optional<NArrow::NMerger::TCursor> lastResultPosition = DrainMergerLinearScan(Context->ReadSequentiallyBufferSize);
        if (lastResultPosition) {
            LastPK = lastResultPosition->ExtractSortingPosition(MergingContext->GetFinish().GetSortFields());
        }
        AFL_VERIFY(!!LastPK == (!!ResultBatch && ResultBatch->num_rows()));
        auto statusConclusion = PrepareResultBatch();
        if (statusConclusion.IsFail()) {
            return statusConclusion;
        }
        return true;
    }();
    if (mergeStats) {
        mergeStats->Finish(statsConclusion.IsFail() ? NScanStats::TStep::failed : NScanStats::TStep::completed);
    }
    return statsConclusion;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
