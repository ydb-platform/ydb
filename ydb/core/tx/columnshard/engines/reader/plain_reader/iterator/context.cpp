#include "context.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetch_steps.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetching.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NPlain {

std::unique_ptr<NArrow::NMerger::TMergePartialStream> TSpecialReadContext::BuildMerger() const {
    return std::make_unique<NArrow::NMerger::TMergePartialStream>(GetReadMetadata()->GetReplaceKey(), GetProgramInputColumns()->GetSchema(),
        GetCommonContext()->IsReverse(), IIndexInfo::GetSnapshotColumnNames());
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

std::shared_ptr<TFetchingScript> TSpecialReadContext::DoGetColumnsFetchingPlan(const std::shared_ptr<NCommon::IDataSource>& sourceExt) {
    auto source = std::static_pointer_cast<IDataSource>(sourceExt);
    if (source->NeedAccessorsFetching()) {
        if (!AskAccumulatorsScript) {
            AskAccumulatorsScript = std::make_shared<TFetchingScript>(*this);
            if (ui64 size = source->PredictAccessorsMemory()) {
                AskAccumulatorsScript->AddStep<NCommon::TAllocateMemoryStep>(size, EStageFeaturesIndexes::Accessors);
            }
            AskAccumulatorsScript->AddStep<TPortionAccessorFetchingStep>();
            AskAccumulatorsScript->AddStep<TDetectInMem>(*GetFFColumns());
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
    const bool useTtl = [&]() {
        const auto& ttlBound = GetReadMetadata()->GetTtlBound();
        if (!ttlBound) {
            return false;
        }
        if (ttlBound->GetColumnId() != GetReadMetadata()->GetIndexInfo().GetPKFirstColumnId()) {
            return false;
        }
        if (NArrow::ScalarLess(ttlBound->GetLargestExpiredScalar(), NArrow::TReplaceKey::ToScalar(source->GetMinReplaceKey(), 0))) {
            return false;
        }
        return true;
    }();
    const bool useIndexes = (IndexChecker ? source->HasIndexes(IndexChecker->GetIndexIds()) : false);
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
                                           [needShardingFilter ? 1 : 0][hasDeletions ? 1 : 0][useTtl ? 1 : 0];
        if (result.NeedInitialization()) {
            TGuard<TMutex> g(Mutex);
            if (auto gInit = result.StartInitialization()) {
                gInit->InitializationFinished(BuildColumnsFetchingPlan(
                    needSnapshots, isWholeExclusiveSource, partialUsageByPK, useIndexes, needShardingFilter, hasDeletions, useTtl));
            }
            AFL_VERIFY(!result.NeedInitialization());
        }
        if (result.HasScript()) {
            return result.GetScriptVerified();
        } else {
            std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>(*this);
            result->SetBranchName("FAKE");
            result->AddStep<TBuildFakeSpec>();
            return result;
        }
    }
}

std::shared_ptr<TFetchingScript> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots, const bool exclusiveSource,
    const bool partialUsageByPredicateExt, const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion,
    const bool needFilterTtl) const {
    std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>(*this);
    const bool partialUsageByPredicate = partialUsageByPredicateExt && GetPredicateColumns()->GetColumnsCount();

    NCommon::TColumnsAccumulator acc(GetMergeColumns(), GetReadMetadata()->GetResultSchema());
    if (!!IndexChecker && useIndexes && exclusiveSource) {
        result->AddStep(std::make_shared<TIndexBlobsFetchingStep>(std::make_shared<TIndexesSet>(IndexChecker->GetIndexIds())));
        result->AddStep(std::make_shared<TApplyIndexStep>(IndexChecker));
    }
    bool hasFilterSharding = false;
    if (needFilterSharding && !GetShardingColumns()->IsEmpty()) {
        hasFilterSharding = true;
        acc.AddFetchingStep(*result, *GetShardingColumns(), EStageFeaturesIndexes::Filter);
        if (!exclusiveSource) {
            acc.AddFetchingStep(*result, *GetPKColumns(), EStageFeaturesIndexes::Filter);
            acc.AddFetchingStep(*result, *GetSpecColumns(), EStageFeaturesIndexes::Filter);
        }
        acc.AddAssembleStep(*result, acc.GetFetchingReadyColumns(), "SPEC_SHARDING", EStageFeaturesIndexes::Filter, false);
        result->AddStep(std::make_shared<TShardingFilter>());
    }
    if (needFilterTtl) {
        acc.AddFetchingStep(*result,
            TColumnsSet(std::vector<ui32>({ TValidator::CheckNotNull(GetReadMetadata()->GetTtlBound())->GetColumnId() }),
                GetReadMetadata()->GetResultSchema()),
            EStageFeaturesIndexes::Filter);
    }
    if (!GetEFColumns()->GetColumnsCount() && !partialUsageByPredicate) {
        result->SetBranchName("simple");
        acc.AddFetchingStep(*result, *GetFFColumns(), EStageFeaturesIndexes::Fetching);
        if (needFilterDeletion) {
            acc.AddFetchingStep(*result, *GetDeletionColumns(), EStageFeaturesIndexes::Fetching);
        }
        if (needSnapshots) {
            acc.AddFetchingStep(*result, *GetSpecColumns(), EStageFeaturesIndexes::Fetching);
        }
        if (!exclusiveSource) {
            acc.AddFetchingStep(*result, *GetMergeColumns(), EStageFeaturesIndexes::Fetching);
        } else {
            if (acc.GetFetchingReadyColumns().Size() == 1 && GetSpecColumns()->Contains(acc.GetFetchingReadyColumns()) && !hasFilterSharding) {
                return nullptr;
            }
        }
        if (acc.GetFetchingReadyColumns().Size() || hasFilterSharding || needFilterDeletion) {
            acc.AddFetchingStep(*result, *GetProgramInputColumns(), EStageFeaturesIndexes::Filter);
            acc.AddAssembleStep(*result, *GetProgramInputColumns(), "PROGRAM", EStageFeaturesIndexes::Fetching, false);
            if (needSnapshots) {
                acc.AddAssembleStep(*result, *GetSpecColumns(), "SPEC", EStageFeaturesIndexes::Fetching, false);
            }
            if (!exclusiveSource) {
                acc.AddAssembleStep(*result, *GetMergeColumns(), "LAST_PK", EStageFeaturesIndexes::Fetching, false);
            }
            if (needSnapshots) {
                result->AddStep(std::make_shared<TSnapshotFilter>());
            }
            if (needFilterDeletion) {
                acc.AddAssembleStep(*result, *GetDeletionColumns(), "SPEC_DELETION", EStageFeaturesIndexes::Fetching, false);
                result->AddStep(std::make_shared<TDeletionFilter>());
            }
            acc.AddAssembleStep(
                *result, acc.GetFetchingReadyColumns().GetColumnIds(), "LAST", EStageFeaturesIndexes::Fetching, !exclusiveSource);
        } else {
            return nullptr;
        }
    } else if (exclusiveSource) {
        result->SetBranchName("exclusive");
        acc.AddFetchingStep(*result, *GetEFColumns(), EStageFeaturesIndexes::Filter);
        if (needFilterDeletion) {
            acc.AddFetchingStep(*result, *GetDeletionColumns(), EStageFeaturesIndexes::Filter);
        }
        if (needSnapshots || GetFFColumns()->Cross(*GetSpecColumns())) {
            acc.AddFetchingStep(*result, *GetSpecColumns(), EStageFeaturesIndexes::Filter);
        }
        if (partialUsageByPredicate) {
            acc.AddFetchingStep(*result, *GetPredicateColumns(), EStageFeaturesIndexes::Filter);
        }

        AFL_VERIFY(acc.GetFetchingReadyColumns().Size());

        if (needFilterDeletion) {
            acc.AddAssembleStep(*result, *GetDeletionColumns(), "SPEC_DELETION", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TDeletionFilter>());
        }
        if (partialUsageByPredicate) {
            acc.AddAssembleStep(*result, *GetPredicateColumns(), "PREDICATE", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TPredicateFilter>());
        }
        if (needFilterTtl) {
            acc.AddAssembleStep(
                *result, std::vector<ui32>({ GetReadMetadata()->GetTtlBound()->GetColumnId() }), "TTL", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<NCommon::TTtlFilter>());
        }
        if (needSnapshots || GetFFColumns()->Cross(*GetSpecColumns())) {
            acc.AddAssembleStep(*result, *GetSpecColumns(), "SPEC", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TSnapshotFilter>());
        }
        acc.AddFetchingStep(*result, *GetFFColumns(), EStageFeaturesIndexes::Fetching);
        acc.AddAssembleStep(*result, *GetFFColumns(), "LAST", EStageFeaturesIndexes::Fetching, !exclusiveSource);
    } else {
        result->SetBranchName("merge");
        acc.AddFetchingStep(*result, *GetMergeColumns(), EStageFeaturesIndexes::Filter);
        acc.AddFetchingStep(*result, *GetEFColumns(), EStageFeaturesIndexes::Filter);
        if (needFilterDeletion) {
            acc.AddFetchingStep(*result, *GetDeletionColumns(), EStageFeaturesIndexes::Filter);
        }
        AFL_VERIFY(acc.GetFetchingReadyColumns().Size());

        acc.AddAssembleStep(*result, *GetSpecColumns(), "SPEC", EStageFeaturesIndexes::Filter, false);
        acc.AddAssembleStep(*result, *GetPKColumns(), "PK", EStageFeaturesIndexes::Filter, false);
        if (needSnapshots) {
            result->AddStep(std::make_shared<TSnapshotFilter>());
        }
        if (needFilterDeletion) {
            acc.AddAssembleStep(*result, *GetDeletionColumns(), "SPEC_DELETION", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TDeletionFilter>());
        }
        if (partialUsageByPredicate) {
            result->AddStep(std::make_shared<TPredicateFilter>());
        }
        if (needFilterTtl) {
            acc.AddAssembleStep(
                *result, std::vector<ui32>({ GetReadMetadata()->GetTtlBound()->GetColumnId() }), "TTL", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<NCommon::TTtlFilter>());
        }
        acc.AddFetchingStep(*result, *GetFFColumns(), EStageFeaturesIndexes::Fetching);
        acc.AddAssembleStep(*result, *GetFFColumns(), "LAST", EStageFeaturesIndexes::Fetching, !exclusiveSource);
    }
    result->AddStep<NCommon::TBuildStageResultStep>();
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

    for (ui32 i = 0; i < (1 << 6); ++i) {
        auto& script = CacheFetchingScripts[GetBit(i, 0)][GetBit(i, 1)][GetBit(i, 2)][GetBit(i, 3)][GetBit(i, 4)][GetBit(i, 5)][GetBit(i, 6)];
        if (script.HasScript()) {
            sb << script.DebugString() << ";";
        }
    }
    return sb;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
