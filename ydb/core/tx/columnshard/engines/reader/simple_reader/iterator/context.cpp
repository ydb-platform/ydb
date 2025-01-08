#include "context.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetch_steps.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<TFetchingScript> TSpecialReadContext::DoGetColumnsFetchingPlan(const std::shared_ptr<NCommon::IDataSource>& sourceExt) {
    const auto source = std::static_pointer_cast<IDataSource>(sourceExt);
    const bool needSnapshots = GetReadMetadata()->GetRequestSnapshot() < source->GetRecordSnapshotMax();
    if (!needSnapshots && GetFFColumns()->GetColumnIds().size() == 1 &&
        GetFFColumns()->GetColumnIds().contains(NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP_INDEX)) {
        std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>(*this);
        source->SetSourceInMemory(true);
        result->SetBranchName("FAKE");
        result->AddStep<TBuildFakeSpec>();
        result->AddStep<TBuildResultStep>(0, source->GetRecordsCount());
        return result;
    }
    if (!source->GetStageData().HasPortionAccessor()) {
        if (!AskAccumulatorsScript) {
            AskAccumulatorsScript = std::make_shared<TFetchingScript>(*this);
            AskAccumulatorsScript->AddStep<NCommon::TAllocateMemoryStep>(
                source->PredictAccessorsSize(GetFFColumns()->GetColumnIds()), EStageFeaturesIndexes::Accessors);
            AskAccumulatorsScript->AddStep<TPortionAccessorFetchingStep>();
            AskAccumulatorsScript->AddStep<TDetectInMem>(*GetFFColumns());
        }
        return AskAccumulatorsScript;
    }
    const bool partialUsageByPK = [&]() {
        switch (source->GetUsageClass()) {
            case TPKRangeFilter::EUsageClass::PartialUsage:
                return true;
            case TPKRangeFilter::EUsageClass::DontUsage:
                return true;
            case TPKRangeFilter::EUsageClass::FullUsage:
                return false;
        }
    }();
    const bool useIndexes = (IndexChecker ? source->HasIndexes(IndexChecker->GetIndexIds()) : false);
    const bool hasDeletions = source->GetHasDeletions();
    bool needShardingFilter = false;
    if (!!GetReadMetadata()->GetRequestShardingInfo()) {
        auto ver = source->GetShardingVersionOptional();
        if (!ver || *ver < GetReadMetadata()->GetRequestShardingInfo()->GetSnapshotVersion()) {
            needShardingFilter = true;
        }
    }
    {
        auto result = CacheFetchingScripts[needSnapshots ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0][needShardingFilter ? 1 : 0]
                                          [hasDeletions ? 1 : 0];
        if (!result) {
            TGuard<TMutex> wg(Mutex);
            result = CacheFetchingScripts[needSnapshots ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0][needShardingFilter ? 1 : 0]
                                         [hasDeletions ? 1 : 0];
            if (!result) {
                result = BuildColumnsFetchingPlan(needSnapshots, partialUsageByPK, useIndexes, needShardingFilter, hasDeletions);
                CacheFetchingScripts[needSnapshots ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0][needShardingFilter ? 1 : 0]
                                    [hasDeletions ? 1 : 0] = result;
            }
        }
        AFL_VERIFY(result);
        AFL_VERIFY(*result);
        return *result;
    }
}

std::shared_ptr<TFetchingScript> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots, const bool partialUsageByPredicateExt,
    const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion) const {
    std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>(*this);
    const bool partialUsageByPredicate = partialUsageByPredicateExt && GetPredicateColumns()->GetColumnsCount();

    NCommon::TColumnsAccumulator acc(GetMergeColumns(), GetReadMetadata()->GetResultSchema());
    if (!!IndexChecker && useIndexes) {
        result->AddStep(std::make_shared<TIndexBlobsFetchingStep>(std::make_shared<TIndexesSet>(IndexChecker->GetIndexIds())));
        result->AddStep(std::make_shared<TApplyIndexStep>(IndexChecker));
    }
    if (needFilterSharding && !GetShardingColumns()->IsEmpty()) {
        const TColumnsSetIds columnsFetch = *GetShardingColumns();
        acc.AddFetchingStep(*result, columnsFetch, EStageFeaturesIndexes::Filter);
        acc.AddAssembleStep(*result, columnsFetch, "SPEC_SHARDING", EStageFeaturesIndexes::Filter, false);
        result->AddStep(std::make_shared<TShardingFilter>());
    }
    {
        result->SetBranchName("exclusive");
        TColumnsSet columnsFetch = *GetEFColumns();
        if (needFilterDeletion) {
            columnsFetch = columnsFetch + *GetDeletionColumns();
        }
        if (needSnapshots || GetFFColumns()->Cross(*GetSpecColumns())) {
            columnsFetch = columnsFetch + *GetSpecColumns();
        }
        if (partialUsageByPredicate) {
            columnsFetch = columnsFetch + *GetPredicateColumns();
        }

        if (columnsFetch.GetColumnsCount()) {
            acc.AddFetchingStep(*result, columnsFetch, EStageFeaturesIndexes::Filter);
        }

        if (needFilterDeletion) {
            acc.AddAssembleStep(*result, *GetDeletionColumns(), "SPEC_DELETION", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TDeletionFilter>());
        }
        if (partialUsageByPredicate) {
            acc.AddAssembleStep(*result, *GetPredicateColumns(), "PREDICATE", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TPredicateFilter>());
        }
        if (needSnapshots || GetFFColumns()->Cross(*GetSpecColumns())) {
            acc.AddAssembleStep(*result, *GetSpecColumns(), "SPEC", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TSnapshotFilter>());
        }
        for (auto&& i : GetReadMetadata()->GetProgram().GetSteps()) {
            if (i->GetFilterOriginalColumnIds().empty()) {
                break;
            }
            TColumnsSet stepColumnIds(i->GetFilterOriginalColumnIds(), GetReadMetadata()->GetResultSchema());
            acc.AddAssembleStep(*result, stepColumnIds, "EF", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TFilterProgramStep>(i));
            if (!i->IsFilterOnly()) {
                break;
            }
        }
        if (GetReadMetadata()->HasLimit()) {
            result->AddStep(std::make_shared<TFilterCutLimit>(GetReadMetadata()->GetLimitRobust(), GetReadMetadata()->IsDescSorted()));
        }
        acc.AddFetchingStep(*result, *GetFFColumns(), EStageFeaturesIndexes::Fetching);
        acc.AddAssembleStep(*result, *GetFFColumns(), "LAST", EStageFeaturesIndexes::Fetching, false);
    }
    result->AddStep<NCommon::TBuildStageResultStep>();
    result->AddStep<TPrepareResultStep>();
    return result;
}

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : TBase(commonContext) {
}

TString TSpecialReadContext::ProfileDebugString() const {
    TStringBuilder sb;
    const auto GetBit = [](const ui32 val, const ui32 pos) -> ui32 {
        return (val & (1 << pos)) ? 1 : 0;
    };

    for (ui32 i = 0; i < (1 << 5); ++i) {
        auto script = CacheFetchingScripts[GetBit(i, 0)][GetBit(i, 1)][GetBit(i, 2)][GetBit(i, 3)][GetBit(i, 4)];
        if (script && *script) {
            sb << (*script)->DebugString() << ";";
        }
    }
    return sb;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
