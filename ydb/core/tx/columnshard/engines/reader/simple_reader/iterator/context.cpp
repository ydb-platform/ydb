#include "context.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/fetch_steps.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/duplicates/manager.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple {

std::shared_ptr<TFetchingScript> TSpecialReadContext::DoGetColumnsFetchingPlan(
    const std::shared_ptr<NCommon::IDataSource>& sourceExt, const bool isFinalSyncPoint) {
    const bool partialUsageByPK = [&]() {
        if (sourceExt->GetType() == NCommon::IDataSource::EType::SimplePortion) {
            const auto source = std::static_pointer_cast<TPortionDataSource>(sourceExt);
            switch (source->GetUsageClass()) {
                case TPKRangeFilter::EUsageClass::PartialUsage:
                    return true;
                case TPKRangeFilter::EUsageClass::NoUsage:
                    return true;
                case TPKRangeFilter::EUsageClass::FullUsage:
                    return false;
            }
        }
        return false;
    }();
    const auto* source = sourceExt->GetAs<IDataSource>();
    const bool needSnapshots = GetReadMetadata()->GetRequestSnapshot() < source->GetRecordSnapshotMax();

    const bool useIndexes = false;
    const bool hasDeletions = source->GetHasDeletions();
    bool needShardingFilter = false;
    if (!!GetReadMetadata()->GetRequestShardingInfo()) {
        auto ver = source->GetShardingVersionOptional();
        if (!ver || *ver < GetReadMetadata()->GetRequestShardingInfo()->GetSnapshotVersion()) {
            needShardingFilter = true;
        }
    }
    const bool preventDuplicates = GetReadMetadata()->GetDeduplicationPolicy() == EDeduplicationPolicy::PREVENT_DUPLICATES;
    {
        auto& result = CacheFetchingScripts[needSnapshots ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0][needShardingFilter ? 1 : 0]
                                           [hasDeletions ? 1 : 0][preventDuplicates ? 1 : 0];
        if (result.NeedInitialization()) {
            TGuard<TMutex> g(Mutex);
            if (auto gInit = result.StartInitialization()) {
                gInit->InitializationFinished(BuildColumnsFetchingPlan(
                    needSnapshots, partialUsageByPK, useIndexes, needShardingFilter, hasDeletions, preventDuplicates, isFinalSyncPoint));
            }
            AFL_VERIFY(!result.NeedInitialization());
        }
        return result.GetScriptVerified();
    }
}

std::shared_ptr<TFetchingScript> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots, const bool partialUsageByPredicateExt,
    const bool /*useIndexes*/, const bool needFilterSharding, const bool needFilterDeletion, const bool preventDuplicates,
    const bool isFinalSyncPoint) const {
    const bool partialUsageByPredicate = partialUsageByPredicateExt && GetPredicateColumns()->GetColumnsCount();

    NCommon::TFetchingScriptBuilder acc(*this);
    acc.AddStep(std::make_shared<TInitializeSourceStep>());
    acc.AddStep(std::make_shared<TDetectInMemFlag>(*GetFFColumns()));
    if (needFilterSharding && !GetShardingColumns()->IsEmpty()) {
        const TColumnsSetIds columnsFetch = *GetShardingColumns();
        acc.AddFetchingStep(columnsFetch, NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        acc.AddAssembleStep(columnsFetch, "SPEC_SHARDING", NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter, false);
        acc.AddStep(std::make_shared<TShardingFilter>());
    }
    {
        acc.SetBranchName("exclusive");
        if (needFilterDeletion) {
            acc.AddFetchingStep(*GetDeletionColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        }
        if (partialUsageByPredicate) {
            acc.AddFetchingStep(*GetPredicateColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        }
        if (needSnapshots || GetFFColumns()->Cross(*GetSpecColumns())) {
            acc.AddFetchingStep(*GetSpecColumns(), NArrow::NSSA::IMemoryCalculationPolicy::EStage::Filter);
        }
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
        }
        if (preventDuplicates) {
            acc.AddStep(std::make_shared<TDuplicateFilter>());
        }
        const auto& chainProgram = GetReadMetadata()->GetProgram().GetChainVerified();
        acc.AddStep(std::make_shared<NCommon::TProgramStep>(chainProgram));
    }
    if (!GetSourcesAggregationScript()) {
        acc.AddStep(std::make_shared<NCommon::TBuildStageResultStep>());
        acc.AddStep(std::make_shared<TPrepareResultStep>(isFinalSyncPoint));
    }
    return std::move(acc).Build();
}

void TSpecialReadContext::RegisterActors() {
    AFL_VERIFY(!DuplicatesManager);
    if (GetReadMetadata()->GetDeduplicationPolicy() == EDeduplicationPolicy::PREVENT_DUPLICATES) {
        DuplicatesManager = NActors::TActivationContext::Register(new NDuplicateFiltering::TDuplicateManager(*this));
    }
}

void TSpecialReadContext::UnregisterActors() {
    if (DuplicatesManager) {
        NActors::TActivationContext::AsActorContext().Send(DuplicatesManager, new NActors::TEvents::TEvPoison);
        DuplicatesManager = TActorId();
    }
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

}   // namespace NKikimr::NOlap::NReader::NSimple
