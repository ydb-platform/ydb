#include "context.h"
#include "source.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<TFetchingScript> TSpecialReadContext::GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source) {
    const bool needSnapshots = ReadMetadata->GetRequestSnapshot() < source->GetRecordSnapshotMax();
    if (!needSnapshots && GetFFColumns()->GetColumnIds().size() == 1 &&
        GetFFColumns()->GetColumnIds().contains(NOlap::NPortion::TSpecialColumns::SPEC_COL_PLAN_STEP_INDEX)) {
        std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>(*this);
        source->SetSourceInMemory(true);
        result->SetBranchName("FAKE");
        result->AddStep(std::make_shared<TBuildFakeSpec>(source->GetRecordsCount()));
        result->AddStep<TBuildResultStep>(0, source->GetRecordsCount());
        return result;
    }
    if (!source->GetStageData().HasPortionAccessor()) {
        if (!AskAccumulatorsScript) {
            AskAccumulatorsScript = std::make_shared<TFetchingScript>(*this);
            AskAccumulatorsScript->AddStep<TAllocateMemoryStep>(
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
    if (!!ReadMetadata->GetRequestShardingInfo()) {
        auto ver = source->GetShardingVersionOptional();
        if (!ver || *ver < ReadMetadata->GetRequestShardingInfo()->GetSnapshotVersion()) {
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

class TColumnsAccumulator {
private:
    TColumnsSetIds FetchingReadyColumns;
    TColumnsSetIds AssemblerReadyColumns;
    ISnapshotSchema::TPtr FullSchema;
    std::shared_ptr<TColumnsSetIds> GuaranteeNotOptional;

public:
    TColumnsAccumulator(const std::shared_ptr<TColumnsSetIds>& guaranteeNotOptional, const ISnapshotSchema::TPtr& fullSchema)
        : FullSchema(fullSchema)
        , GuaranteeNotOptional(guaranteeNotOptional) {
    }

    TColumnsSetIds GetNotFetchedAlready(const TColumnsSetIds& columns) const {
        return columns - FetchingReadyColumns;
    }

    bool AddFetchingStep(TFetchingScript& script, const TColumnsSetIds& columns, const EStageFeaturesIndexes stage) {
        auto actualColumns = GetNotFetchedAlready(columns);
        FetchingReadyColumns = FetchingReadyColumns + (TColumnsSetIds)columns;
        if (!actualColumns.IsEmpty()) {
            script.Allocation(columns.GetColumnIds(), stage, EMemType::Blob);
            script.AddStep(std::make_shared<TColumnBlobsFetchingStep>(actualColumns));
            return true;
        }
        return false;
    }
    bool AddAssembleStep(TFetchingScript& script, const TColumnsSetIds& columns, const TString& purposeId, const EStageFeaturesIndexes stage,
        const bool sequential) {
        auto actualColumns = columns - AssemblerReadyColumns;
        AssemblerReadyColumns = AssemblerReadyColumns + columns;
        if (actualColumns.IsEmpty()) {
            return false;
        }
        auto actualSet = std::make_shared<TColumnsSet>(actualColumns.GetColumnIds(), FullSchema);
        if (sequential) {
            const auto notSequentialColumnIds = GuaranteeNotOptional->Intersect(*actualSet);
            if (notSequentialColumnIds.size()) {
                script.Allocation(notSequentialColumnIds, stage, EMemType::Raw);
                std::shared_ptr<TColumnsSet> cross = actualSet->BuildSamePtr(notSequentialColumnIds);
                script.AddStep<TAssemblerStep>(cross, purposeId);
                *actualSet = *actualSet - *cross;
            }
            if (!actualSet->IsEmpty()) {
                script.Allocation(notSequentialColumnIds, stage, EMemType::RawSequential);
                script.AddStep<TOptionalAssemblerStep>(actualSet, purposeId);
            }
        } else {
            script.Allocation(actualColumns.GetColumnIds(), stage, EMemType::Raw);
            script.AddStep<TAssemblerStep>(actualSet, purposeId);
        }
        return true;
    }
};

std::shared_ptr<TFetchingScript> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots, const bool partialUsageByPredicateExt,
    const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion) const {
    std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>(*this);
    const bool partialUsageByPredicate = partialUsageByPredicateExt && GetPredicateColumns()->GetColumnsCount();

    TColumnsAccumulator acc(GetMergeColumns(), ReadMetadata->GetResultSchema());
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
        for (auto&& i : ReadMetadata->GetProgram().GetSteps()) {
            if (i->GetFilterOriginalColumnIds().empty()) {
                break;
            }
            TColumnsSet stepColumnIds(i->GetFilterOriginalColumnIds(), ReadMetadata->GetResultSchema());
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
    result->AddStep<TPrepareResultStep>();
    return result;
}

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : TBase(commonContext) {
    ReadMetadata = GetCommonContext()->GetReadMetadataPtrVerifiedAs<TReadMetadata>();
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
