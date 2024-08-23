#include "context.h"
#include "source.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NPlain {

std::unique_ptr<NArrow::NMerger::TMergePartialStream> TSpecialReadContext::BuildMerger() const {
    return std::make_unique<NArrow::NMerger::TMergePartialStream>(
        ReadMetadata->GetReplaceKey(), ProgramInputColumns->GetSchema(), CommonContext->IsReverse(), IIndexInfo::GetSnapshotColumnNames());
}

ui64 TSpecialReadContext::GetMemoryForSources(const THashMap<ui32, std::shared_ptr<IDataSource>>& sources) {
    ui64 result = 0;
    bool hasSequentialReadSources = false;
    for (auto&& i : sources) {
        auto fetchingPlan = GetColumnsFetchingPlan(i.second);
        AFL_VERIFY(i.second->GetIntervalsCount());
        const ui64 sourceMemory = std::max<ui64>(1, i.second->GetResourceGuardsMemory() / i.second->GetIntervalsCount());
        if (!i.second->IsSourceInMemory()) {
            hasSequentialReadSources = true;
        }
        result += sourceMemory;
    }
    AFL_VERIFY(result);
    if (hasSequentialReadSources) {
        result += ReadSequentiallyBufferSize;
    }
    return result;
}

std::shared_ptr<TFetchingScript> TSpecialReadContext::GetColumnsFetchingPlan(const std::shared_ptr<IDataSource>& source) {
    const bool needSnapshots = !source->GetExclusiveIntervalOnly() || ReadMetadata->GetRequestSnapshot() < source->GetRecordSnapshotMax() ||
                               !source->IsSourceInMemory();
    bool partialUsageByPK = false;
    {
        const TPKRangeFilter::EUsageClass usage =
            ReadMetadata->GetPKRangesFilter().IsPortionInPartialUsage(source->GetStartReplaceKey(), source->GetFinishReplaceKey());
        switch (usage) {
            case TPKRangeFilter::EUsageClass::PartialUsage:
                partialUsageByPK = true;
                break;
            case TPKRangeFilter::EUsageClass::DontUsage:
                partialUsageByPK = true;
                break;
            case TPKRangeFilter::EUsageClass::FullUsage:
                partialUsageByPK = false;
                break;
        }
    }
    const bool useIndexes = (IndexChecker ? source->HasIndexes(IndexChecker->GetIndexIds()) : false);
    const bool isWholeExclusiveSource = source->GetExclusiveIntervalOnly() && source->IsSourceInMemory();
    const bool hasDeletions = source->GetHasDeletions();
    bool needShardingFilter = false;
    if (!!ReadMetadata->GetRequestShardingInfo()) {
        auto ver = source->GetShardingVersionOptional();
        if (!ver || *ver < ReadMetadata->GetRequestShardingInfo()->GetSnapshotVersion()) {
            needShardingFilter = true;
        }
    }
    auto result = CacheFetchingScripts[needSnapshots ? 1 : 0][isWholeExclusiveSource ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0]
                                      [needShardingFilter ? 1 : 0][hasDeletions ? 1 : 0];
    if (!result) {
        result = BuildColumnsFetchingPlan(needSnapshots, isWholeExclusiveSource, partialUsageByPK, useIndexes, needShardingFilter, hasDeletions);
        CacheFetchingScripts[needSnapshots ? 1 : 0][isWholeExclusiveSource ? 1 : 0][partialUsageByPK ? 1 : 0][useIndexes ? 1 : 0]
                            [needShardingFilter ? 1 : 0][hasDeletions ? 1 : 0] = result;
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

    bool AddFetchingStep(TFetchingScript& script, const TColumnsSetIds& columns, const EStageFeaturesIndexes& stage) {
        auto actualColumns = (TColumnsSetIds)columns - FetchingReadyColumns;
        FetchingReadyColumns = FetchingReadyColumns + (TColumnsSetIds)columns;
        if (!actualColumns.IsEmpty()) {
            script.AddStep(std::make_shared<TAllocateMemoryStep>(actualColumns, stage));
            script.AddStep(std::make_shared<TColumnBlobsFetchingStep>(actualColumns));
            return true;
        }
        return false;
    }
    bool AddAssembleStep(TFetchingScript& script, const TColumnsSetIds& columns, const TString& purposeId, const bool optional) {
        auto actualColumns = (TColumnsSetIds)columns - AssemblerReadyColumns;
        AssemblerReadyColumns = AssemblerReadyColumns + columns;
        if (!actualColumns.IsEmpty()) {
            auto actualSet = std::make_shared<TColumnsSet>(actualColumns.GetColumnIds(), FullSchema);
            if (optional) {
                const auto notOptionalColumnIds = GuaranteeNotOptional->Intersect(*actualSet);
                if (notOptionalColumnIds.size()) {
                    std::shared_ptr<TColumnsSet> cross = actualSet->BuildSamePtr(notOptionalColumnIds);
                    script.AddStep(std::make_shared<TAssemblerStep>(cross, purposeId));
                    *actualSet = *actualSet - *cross;
                }
                if (!actualSet->IsEmpty()) {
                    script.AddStep(std::make_shared<TOptionalAssemblerStep>(actualSet, purposeId));
                }
            } else {
                script.AddStep(std::make_shared<TAssemblerStep>(actualSet, purposeId));
            }
            return true;
        }
        return false;
    }
};

std::shared_ptr<TFetchingScript> TSpecialReadContext::BuildColumnsFetchingPlan(const bool needSnapshots, const bool exclusiveSource,
    const bool partialUsageByPredicateExt, const bool useIndexes, const bool needFilterSharding, const bool needFilterDeletion) const {
    std::shared_ptr<TFetchingScript> result = std::make_shared<TFetchingScript>(*this);
    const bool partialUsageByPredicate = partialUsageByPredicateExt && PredicateColumns->GetColumnsCount();
    if (!!IndexChecker && useIndexes && exclusiveSource) {
        result->AddStep(std::make_shared<TIndexBlobsFetchingStep>(std::make_shared<TIndexesSet>(IndexChecker->GetIndexIds())));
        result->AddStep(std::make_shared<TApplyIndexStep>(IndexChecker));
    }
    bool hasFilterSharding = false;
    TColumnsAccumulator acc(MergeColumns, ReadMetadata->GetResultSchema());
    if (needFilterSharding && !ShardingColumns->IsEmpty()) {
        hasFilterSharding = true;
        TColumnsSetIds columnsFetch = *ShardingColumns;
        if (!exclusiveSource) {
            columnsFetch = columnsFetch + *PKColumns + *SpecColumns;
        }
        acc.AddFetchingStep(*result, columnsFetch, EStageFeaturesIndexes::Filter);
        acc.AddAssembleStep(*result, columnsFetch, "SPEC_SHARDING", false);
        result->AddStep(std::make_shared<TShardingFilter>());
    }
    if (!EFColumns->GetColumnsCount() && !partialUsageByPredicate) {
        result->SetBranchName("simple");
        TColumnsSetIds columnsFetch = *FFColumns;
        if (needFilterDeletion) {
            columnsFetch = columnsFetch + *DeletionColumns;
        }
        if (needSnapshots) {
            columnsFetch = columnsFetch + *SpecColumns;
        }
        if (!exclusiveSource) {
            columnsFetch = columnsFetch + *PKColumns + *SpecColumns;
        } else {
            if (columnsFetch.GetColumnsCount() == 1 && SpecColumns->Contains(columnsFetch) && !hasFilterSharding) {
                return nullptr;
            }
        }
        if (columnsFetch.GetColumnsCount() || hasFilterSharding || needFilterDeletion) {
            acc.AddFetchingStep(*result, columnsFetch, EStageFeaturesIndexes::Fetching);
            if (!exclusiveSource) {
                acc.AddAssembleStep(*result, *PKColumns + *SpecColumns, "LAST_PK", false);
            }
            if (needFilterDeletion) {
                acc.AddAssembleStep(*result, *DeletionColumns, "SPEC_DELETION", false);
                result->AddStep(std::make_shared<TDeletionFilter>());
            }
            acc.AddAssembleStep(*result, columnsFetch, "LAST", true);
        } else {
            return nullptr;
        }
    } else if (exclusiveSource) {
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

        AFL_VERIFY(columnsFetch.GetColumnsCount());
        acc.AddFetchingStep(*result, columnsFetch, EStageFeaturesIndexes::Filter);

        if (needFilterDeletion) {
            acc.AddAssembleStep(*result, *DeletionColumns, "SPEC_DELETION", false);
            result->AddStep(std::make_shared<TDeletionFilter>());
        }
        if (needSnapshots || FFColumns->Cross(*SpecColumns)) {
            acc.AddAssembleStep(*result, *SpecColumns, "SPEC", false);
            result->AddStep(std::make_shared<TSnapshotFilter>());
        }
        if (partialUsageByPredicate) {
            acc.AddAssembleStep(*result, *PredicateColumns, "PREDICATE", false);
            result->AddStep(std::make_shared<TPredicateFilter>());
        }
        for (auto&& i : ReadMetadata->GetProgram().GetSteps()) {
            if (i->GetFilterOriginalColumnIds().empty()) {
                break;
            }
            TColumnsSet stepColumnIds(i->GetFilterOriginalColumnIds(), ReadMetadata->GetResultSchema());
            acc.AddAssembleStep(*result, stepColumnIds, "EF", true);
            result->AddStep(std::make_shared<TFilterProgramStep>(i));
            if (!i->IsFilterOnly()) {
                break;
            }
        }
        if (GetReadMetadata()->Limit) {
            result->AddStep(std::make_shared<TFilterCutLimit>(GetReadMetadata()->Limit, GetReadMetadata()->IsDescSorted()));
        }
        acc.AddFetchingStep(*result, *FFColumns, EStageFeaturesIndexes::Fetching);
        acc.AddAssembleStep(*result, *FFColumns, "LAST", true);
    } else {
        result->SetBranchName("merge");
        TColumnsSet columnsFetch = *MergeColumns + *EFColumns;
        if (needFilterDeletion) {
            columnsFetch = columnsFetch + *DeletionColumns;
        }
        AFL_VERIFY(columnsFetch.GetColumnsCount());
        acc.AddFetchingStep(*result, columnsFetch, EStageFeaturesIndexes::Filter);

        acc.AddAssembleStep(*result, *SpecColumns, "SPEC", false);
        acc.AddAssembleStep(*result, *PKColumns, "PK", false);
        if (needFilterDeletion) {
            acc.AddAssembleStep(*result, *DeletionColumns, "SPEC_DELETION", false);
            result->AddStep(std::make_shared<TDeletionFilter>());
        }
        if (needSnapshots) {
            result->AddStep(std::make_shared<TSnapshotFilter>());
        }
        if (partialUsageByPredicate) {
            result->AddStep(std::make_shared<TPredicateFilter>());
        }
        for (auto&& i : ReadMetadata->GetProgram().GetSteps()) {
            if (i->GetFilterOriginalColumnIds().empty()) {
                break;
            }
            TColumnsSet stepColumnIds(i->GetFilterOriginalColumnIds(), ReadMetadata->GetResultSchema());
            acc.AddAssembleStep(*result, stepColumnIds, "EF", true);
            result->AddStep(std::make_shared<TFilterProgramStep>(i));
            if (!i->IsFilterOnly()) {
                break;
            }
        }
        acc.AddFetchingStep(*result, *FFColumns, EStageFeaturesIndexes::Fetching);
        acc.AddAssembleStep(*result, *FFColumns, "LAST", true);
    }
    return result;
}

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : CommonContext(commonContext) {

    ReadMetadata = dynamic_pointer_cast<const TReadMetadata>(CommonContext->GetReadMetadata());
    Y_ABORT_UNLESS(ReadMetadata);
    Y_ABORT_UNLESS(ReadMetadata->SelectInfo);

    double kffFilter = 0.45;
    double kffFetching = 0.45;
    double kffMerge = 0.10;
    TString stagePrefix;
    if (ReadMetadata->GetEarlyFilterColumnIds().size()) {
        stagePrefix = "EF";
        kffFilter = 0.7;
        kffFetching = 0.15;
        kffMerge = 0.15;
    } else {
        stagePrefix = "FO";
        kffFilter = 0.1;
        kffFetching = 0.75;
        kffMerge = 0.15;
    }

    std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>> stages = { 
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

    for (ui32 i = 0; i < (1 << 6); ++i) {
        auto script = CacheFetchingScripts[GetBit(i, 0)][GetBit(i, 1)][GetBit(i, 2)][GetBit(i, 3)][GetBit(i, 4)][GetBit(i, 5)];
        if (script && *script) {
            sb << (*script)->DebugString() << ";";
        }
    }
    return sb;
}

}   // namespace NKikimr::NOlap::NReader::NPlain
