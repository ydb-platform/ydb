#include "context.h"
#include "source.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple {

std::unique_ptr<NArrow::NMerger::TMergePartialStream> TSpecialReadContext::BuildMerger() const {
    return std::make_unique<NArrow::NMerger::TMergePartialStream>(
        ReadMetadata->GetReplaceKey(), ProgramInputColumns->GetSchema(), CommonContext->IsReverse(), IIndexInfo::GetSnapshotColumnNames());
}

std::shared_ptr<TFetchingScript> TSpecialReadContext::GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source) {
    if (!source->GetStageData().HasPortionAccessor()) {
        if (!AskAccumulatorsScript) {
            AskAccumulatorsScript = std::make_shared<TFetchingScript>(*this);
            AskAccumulatorsScript->AddStep<TAllocateMemoryStep>(source->PredictAccessorsSize(), EStageFeaturesIndexes::Accessors);
            AskAccumulatorsScript->AddStep<TPortionAccessorFetchingStep>();
            AskAccumulatorsScript->AddStep<TDetectInMem>(*FFColumns);
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
    const bool needSnapshots = ReadMetadata->GetRequestSnapshot() < source->GetRecordSnapshotMax();
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
            result = CacheFetchingScripts[needSnapshots ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0]
                                         [needShardingFilter ? 1 : 0][hasDeletions ? 1 : 0];
            if (!result) {
                result = BuildColumnsFetchingPlan(needSnapshots, partialUsageByPK, useIndexes, needShardingFilter, hasDeletions);
                CacheFetchingScripts[needSnapshots ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0]
                                    [needShardingFilter ? 1 : 0][hasDeletions ? 1 : 0] = result;
            }
        }
        AFL_VERIFY(result);
        if (*result) {
            return *result;
        } else {
            std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>(*this);
            result->SetBranchName("FAKE");
            result->AddStep(std::make_shared<TBuildFakeSpec>(source->GetRecordsCount()));
            return result;
        }
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
        if (!actualColumns.IsEmpty()) {
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
        return false;
    }
};

std::shared_ptr<TFetchingScript> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots,
    const bool partialUsageByPredicateExt, const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion) const {
    std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>(*this);
    const bool partialUsageByPredicate = partialUsageByPredicateExt && PredicateColumns->GetColumnsCount();

    TColumnsAccumulator acc(MergeColumns, ReadMetadata->GetResultSchema());
    if (!!IndexChecker && useIndexes) {
        result->AddStep(std::make_shared<TIndexBlobsFetchingStep>(std::make_shared<TIndexesSet>(IndexChecker->GetIndexIds())));
        result->AddStep(std::make_shared<TApplyIndexStep>(IndexChecker));
    }
    if (needFilterSharding && !ShardingColumns->IsEmpty()) {
        const TColumnsSetIds columnsFetch = *ShardingColumns;
        acc.AddFetchingStep(*result, columnsFetch, EStageFeaturesIndexes::Filter);
        acc.AddAssembleStep(*result, columnsFetch, "SPEC_SHARDING", EStageFeaturesIndexes::Filter, false);
        result->AddStep(std::make_shared<TShardingFilter>());
    }
    {
        result->SetBranchName("exclusive");
        TColumnsSet columnsFetch = *EFColumns;
        if (needFilterDeletion) {
            columnsFetch = columnsFetch + *DeletionColumns;
        }
        if (needSnapshots || FFColumns->Cross(*SpecColumns)) {
            columnsFetch = columnsFetch + *SpecColumns;
        }
        if (partialUsageByPredicate) {
            columnsFetch = columnsFetch + *PredicateColumns;
        }

        if (columnsFetch.GetColumnsCount()) {
            acc.AddFetchingStep(*result, columnsFetch, EStageFeaturesIndexes::Filter);
        }

        if (needFilterDeletion) {
            acc.AddAssembleStep(*result, *DeletionColumns, "SPEC_DELETION", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TDeletionFilter>());
        }
        if (partialUsageByPredicate) {
            acc.AddAssembleStep(*result, *PredicateColumns, "PREDICATE", EStageFeaturesIndexes::Filter, false);
            result->AddStep(std::make_shared<TPredicateFilter>());
        }
        if (needSnapshots || FFColumns->Cross(*SpecColumns)) {
            acc.AddAssembleStep(*result, *SpecColumns, "SPEC", EStageFeaturesIndexes::Filter, false);
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
        if (GetReadMetadata()->Limit) {
            result->AddStep(std::make_shared<TFilterCutLimit>(GetReadMetadata()->Limit, GetReadMetadata()->IsDescSorted()));
        }
        acc.AddFetchingStep(*result, *FFColumns, EStageFeaturesIndexes::Fetching);
        acc.AddAssembleStep(*result, *FFColumns, "LAST", EStageFeaturesIndexes::Fetching, false);
    }
    result->AddStep<TPrepareResultStep>();
    return result;
}

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : CommonContext(commonContext) {

    ReadMetadata = dynamic_pointer_cast<const TReadMetadata>(CommonContext->GetReadMetadata());
    Y_ABORT_UNLESS(ReadMetadata);
    Y_ABORT_UNLESS(ReadMetadata->SelectInfo);

    double kffAccessors = 0.01;
    double kffFilter = 0.45;
    double kffFetching = 0.45;
    double kffMerge = 0.10;
    TString stagePrefix;
    if (ReadMetadata->GetEarlyFilterColumnIds().size()) {
        stagePrefix = "EF";
        kffFilter = 0.7;
        kffFetching = 0.15;
        kffMerge = 0.14;
        kffAccessors = 0.01;
    } else {
        stagePrefix = "FO";
        kffFilter = 0.1;
        kffFetching = 0.75;
        kffMerge = 0.14;
        kffAccessors = 0.01;
    }

    std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>> stages = { 
        NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildStageFeatures(
            stagePrefix + "::ACCESSORS", kffAccessors * TGlobalLimits::ScanMemoryLimit),
        NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildStageFeatures(
            stagePrefix + "::FILTER", kffFilter * TGlobalLimits::ScanMemoryLimit),
        NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildStageFeatures(
            stagePrefix + "::FETCHING", kffFetching * TGlobalLimits::ScanMemoryLimit),
        NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildStageFeatures(stagePrefix + "::MERGE", kffMerge * TGlobalLimits::ScanMemoryLimit)
    };
    ProcessMemoryGuard =
        NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildProcessGuard(CommonContext->GetReadMetadata()->GetTxId(), stages);
    ProcessScopeGuard =
        NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildScopeGuard(CommonContext->GetReadMetadata()->GetTxId(), GetCommonContext()->GetScanId());

    auto readSchema = ReadMetadata->GetResultSchema();
    SpecColumns = std::make_shared<TColumnsSet>(TIndexInfo::GetSnapshotColumnIdsSet(), readSchema);
    IndexChecker = ReadMetadata->GetProgram().GetIndexChecker();
    {
        auto predicateColumns = ReadMetadata->GetPKRangesFilter().GetColumnIds(ReadMetadata->GetIndexInfo());
        if (predicateColumns.size()) {
            PredicateColumns = std::make_shared<TColumnsSet>(predicateColumns, readSchema);
        } else {
            PredicateColumns = std::make_shared<TColumnsSet>();
        }
    }
    {
        std::set<ui32> columnIds = { NPortion::TSpecialColumns::SPEC_COL_DELETE_FLAG_INDEX };
        DeletionColumns = std::make_shared<TColumnsSet>(columnIds, ReadMetadata->GetResultSchema());
    }

    if (!!ReadMetadata->GetRequestShardingInfo()) {
        auto shardingColumnIds =
            ReadMetadata->GetIndexInfo().GetColumnIdsVerified(ReadMetadata->GetRequestShardingInfo()->GetShardingInfo()->GetColumnNames());
        ShardingColumns = std::make_shared<TColumnsSet>(shardingColumnIds, ReadMetadata->GetResultSchema());
    } else {
        ShardingColumns = std::make_shared<TColumnsSet>();
    }
    {
        auto efColumns = ReadMetadata->GetEarlyFilterColumnIds();
        if (efColumns.size()) {
            EFColumns = std::make_shared<TColumnsSet>(efColumns, readSchema);
        } else {
            EFColumns = std::make_shared<TColumnsSet>();
        }
    }
    if (ReadMetadata->HasProcessingColumnIds()) {
        FFColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetProcessingColumnIds(), readSchema);
        if (SpecColumns->Contains(*FFColumns) && !EFColumns->IsEmpty()) {
            FFColumns = std::make_shared<TColumnsSet>(*EFColumns + *SpecColumns);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("ff_modified", FFColumns->DebugString());
        } else {
            AFL_VERIFY(!FFColumns->Contains(*SpecColumns))("info", FFColumns->DebugString());
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("ff_first", FFColumns->DebugString());
        }
    } else {
        FFColumns = EFColumns;
    }
    if (FFColumns->IsEmpty()) {
        ProgramInputColumns = SpecColumns;
    } else {
        ProgramInputColumns = FFColumns;
    }

    PKColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetPKColumnIds(), readSchema);
    MergeColumns = std::make_shared<TColumnsSet>(*PKColumns + *SpecColumns);

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("columns_context_info", DebugString());
}

TString TSpecialReadContext::DebugString() const {
    TStringBuilder sb;
    sb << "ef=" << EFColumns->DebugString() << ";"
       << "sharding=" << ShardingColumns->DebugString() << ";"
       << "pk=" << PKColumns->DebugString() << ";"
       << "ff=" << FFColumns->DebugString() << ";"
       << "program_input=" << ProgramInputColumns->DebugString() << ";";
    return sb;
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
