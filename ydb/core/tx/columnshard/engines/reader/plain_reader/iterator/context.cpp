#include "context.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetch_steps.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetching.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NPlain {

std::unique_ptr<NArrow::NMerger::TMergePartialStream> TSpecialReadContext::BuildMerger() const {
    return std::make_unique<NArrow::NMerger::TMergePartialStream>(GetReadMetadata()->GetReplaceKey(), GetProgramInputColumns()->GetSchema(),
        GetCommonContext()->IsReverse(), IIndexInfo::GetSnapshotColumnNames(), std::nullopt);
}

ui64 TSpecialReadContext::GetMemoryForSources(const THashMap<ui32, std::shared_ptr<IDataSource>>& sources) {
    ui64 result = 0;
    for (auto&& i : sources) {
        AFL_VERIFY(i.second->GetIntervalsCount());
        const ui64 sourceMemory = std::max<ui64>(1, i.second->GetResourceGuardsMemory() / i.second->GetIntervalsCount());
        result += sourceMemory;
    }
    AFL_VERIFY(result);
    result += ReadSequentiallyBufferSize;
    return result;
}

std::shared_ptr<TFetchingScript> TSpecialReadContext::DoGetColumnsFetchingPlan(
    const std::shared_ptr<NCommon::IDataSource>& sourceExt, const bool /*isFinalSyncPoint*/) {
    auto source = std::static_pointer_cast<IDataSource>(sourceExt);
    if (source->NeedAccessorsFetching()) {
        if (!AskAccumulatorsScript) {
            NCommon::TFetchingScriptBuilder acc(*this);
            if (ui64 size = source->PredictAccessorsMemory()) {
                acc.AddStep(std::make_shared<NCommon::TAllocateMemoryStep>(size, NArrow::NSSA::IMemoryCalculationPolicy::EStage::Accessors));
            }
            acc.AddStep(std::make_shared<TPortionAccessorFetchingStep>());
            acc.AddStep(std::make_shared<TDetectInMem>(*GetFFColumns()));
            AskAccumulatorsScript = std::move(acc).Build();
        }
        return AskAccumulatorsScript;
    }
    const bool partialUsageByPK = [&]() {
        switch (source->GetUsageClass()) {
            case TPKRangeFilter::EUsageClass::PartialUsage:
                return true;
            case TPKRangeFilter::EUsageClass::NoUsage:
                return true;
            case TPKRangeFilter::EUsageClass::FullUsage:
                return false;
        }
    }();
    const bool useIndexes = false;
    const bool isWholeExclusiveSource = source->GetExclusiveIntervalOnly() && source->IsSourceInMemory();
    const bool needSnapshots = GetReadMetadata()->GetRequestSnapshot() < source->GetRecordSnapshotMax() || !isWholeExclusiveSource;
    const bool hasDeletions = source->GetHasDeletions();
    bool needShardingFilter = false;
    if (!!GetReadMetadata()->GetRequestShardingInfo()) {
        auto ver = source->GetShardingVersionOptional();
        if (!ver || *ver < GetReadMetadata()->GetRequestShardingInfo()->GetSnapshotVersion()) {
            needShardingFilter = true;
        }
    }
    {
        auto& result = CacheFetchingScripts[needSnapshots ? 1 : 0][isWholeExclusiveSource ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0]
                                          [needShardingFilter ? 1 : 0][hasDeletions ? 1 : 0];
        if (result.NeedInitialization()) {
            TGuard<TMutex> g(Mutex);
            if (auto gInit = result.StartInitialization()) {
                gInit->InitializationFinished(BuildColumnsFetchingPlan(
                    needSnapshots, isWholeExclusiveSource, partialUsageByPK, useIndexes, needShardingFilter, hasDeletions));
            }
            AFL_VERIFY(!result.NeedInitialization());
        }
        if (result.HasScript()) {
            return result.GetScriptVerified();
        } else {
            NCommon::TFetchingScriptBuilder acc(*this);
            acc.SetBranchName("FAKE");
            acc.AddStep(std::make_shared<TBuildFakeSpec>());
            acc.AddStep(std::make_shared<NCommon::TBuildStageResultStep>());
            return std::move(acc).Build();
        }
    }
}

std::shared_ptr<TFetchingScript> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots, const bool exclusiveSource,
    const bool partialUsageByPredicateExt, const bool /*useIndexes*/, const bool needFilterSharding, const bool needFilterDeletion) const {
    const bool partialUsageByPredicate = partialUsageByPredicateExt && GetPredicateColumns()->GetColumnsCount();

    NCommon::TFetchingScriptBuilder acc(*this);
    bool hasFilterSharding = false;
    if (needFilterSharding && !GetShardingColumns()->IsEmpty()) {
        hasFilterSharding = true;
        acc.AddFetchingStep(*GetShardingColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        if (!exclusiveSource) {
            acc.AddFetchingStep(*GetPKColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
            acc.AddFetchingStep(*GetSpecColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        }
        acc.AddAssembleStep(acc.GetAddedFetchingColumns(), "SPEC_SHARDING", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
        acc.AddStep(std::make_shared<TShardingFilter>());
    }
    if (!GetEFColumns()->GetColumnsCount() && !partialUsageByPredicate) {
        acc.SetBranchName("simple");
        acc.AddFetchingStep(*GetFFColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        if (needFilterDeletion) {
            acc.AddFetchingStep(*GetDeletionColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        }
        if (needSnapshots) {
            acc.AddFetchingStep(*GetSpecColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        }
        if (!exclusiveSource) {
            acc.AddFetchingStep(*GetMergeColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        } else {
            if (acc.GetAddedFetchingColumns().GetColumnsCount() == 1 && GetSpecColumns()->Contains(acc.GetAddedFetchingColumns()) && !hasFilterSharding) {
                return nullptr;
            }
        }
        if (acc.GetAddedFetchingColumns().GetColumnsCount() || hasFilterSharding || needFilterDeletion) {
            if (needSnapshots) {
                acc.AddAssembleStep(*GetSpecColumns(), "SPEC", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching, false);
            }
            if (!exclusiveSource) {
                acc.AddAssembleStep(*GetMergeColumns(), "LAST_PK", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching, false);
            }
            if (needSnapshots) {
                acc.AddStep(std::make_shared<TSnapshotFilter>());
            }
            if (needFilterDeletion) {
                acc.AddAssembleStep(*GetDeletionColumns(), "SPEC_DELETION", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching, false);
                acc.AddStep(std::make_shared<TDeletionFilter>());
            }
            acc.AddAssembleStep(acc.GetAddedFetchingColumns().GetColumnIds(), "LAST", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching,
                !exclusiveSource);
        } else {
            return nullptr;
        }
    } else if (exclusiveSource) {
        acc.SetBranchName("exclusive");
        acc.AddFetchingStep(*GetEFColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        if (needFilterDeletion) {
            acc.AddFetchingStep(*GetDeletionColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        }
        if (needSnapshots || GetFFColumns()->Cross(*GetSpecColumns())) {
            acc.AddFetchingStep(*GetSpecColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        }
        if (partialUsageByPredicate) {
            acc.AddFetchingStep(*GetPredicateColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        }

        AFL_VERIFY(acc.GetAddedFetchingColumns().GetColumnsCount());

        if (needFilterDeletion) {
            acc.AddAssembleStep(*GetDeletionColumns(), "SPEC_DELETION", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
            acc.AddStep(std::make_shared<TDeletionFilter>());
        }
        if (partialUsageByPredicate) {
            acc.AddAssembleStep(*GetPredicateColumns(), "PREDICATE", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
            acc.AddStep(std::make_shared<TPredicateFilter>());
        }
        if (needSnapshots || GetFFColumns()->Cross(*GetSpecColumns())) {
            acc.AddAssembleStep(*GetSpecColumns(), "SPEC", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
            acc.AddStep(std::make_shared<TSnapshotFilter>());
        } else if (GetProgramInputColumns()->Cross(*GetSpecColumns())) {
            acc.AddStep(std::make_shared<TBuildFakeSpec>());
        }
        acc.AddFetchingStep(*GetFFColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        acc.AddAssembleStep(*GetFFColumns(), "LAST", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching, !exclusiveSource);
    } else {
        acc.SetBranchName("merge");
        acc.AddFetchingStep(*GetMergeColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        acc.AddFetchingStep(*GetEFColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        if (needFilterDeletion) {
            acc.AddFetchingStep(*GetDeletionColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        }
        AFL_VERIFY(acc.GetAddedFetchingColumns().GetColumnsCount());

        acc.AddAssembleStep(*GetSpecColumns(), "SPEC", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
        acc.AddAssembleStep(*GetPKColumns(), "PK", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
        if (needSnapshots) {
            acc.AddStep(std::make_shared<TSnapshotFilter>());
        }
        if (needFilterDeletion) {
            acc.AddAssembleStep(*GetDeletionColumns(), "SPEC_DELETION", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
            acc.AddStep(std::make_shared<TDeletionFilter>());
        }
        if (partialUsageByPredicate) {
            acc.AddStep(std::make_shared<TPredicateFilter>());
        }
        acc.AddFetchingStep(*GetFFColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching);
        acc.AddAssembleStep(*GetFFColumns(), "LAST", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Fetching, !exclusiveSource);
    }
    acc.AddStep(std::make_shared<NCommon::TBuildStageResultStep>());
    return std::move(acc).Build();
}

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : TBase(commonContext) {
}

TString TSpecialReadContext::ProfileDebugString() const {
    TStringBuilder sb;
    const auto GetBit = [](const ui32 val, const ui32 pos) -> ui32 {
        return (val & (1 << pos)) ? 1 : 0;
    };

    for (ui32 i = 0; i < (1 << 6); ++i) {
        auto& script = CacheFetchingScripts[GetBit(i, 0)][GetBit(i, 1)][GetBit(i, 2)][GetBit(i, 3)][GetBit(i, 4)][GetBit(i, 5)];
        if (script.HasScript()) {
            sb << script.ProfileDebugString() << ";";
        }
    }
    return sb;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
