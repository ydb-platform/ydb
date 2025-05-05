#include "context.h"

#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NCommon {

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : CommonContext(commonContext) {
    ReadMetadata = CommonContext->GetReadMetadataPtrVerifiedAs<TReadMetadata>();
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
    ProcessMemoryGuard = NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildProcessGuard(ReadMetadata->GetTxId(), stages);
    ProcessScopeGuard =
        NGroupedMemoryManager::TScanMemoryLimiterOperator::BuildScopeGuard(ReadMetadata->GetTxId(), GetCommonContext()->GetScanId());

    auto readSchema = ReadMetadata->GetResultSchema();
    SpecColumns = std::make_shared<TColumnsSet>(TIndexInfo::GetSnapshotColumnIdsSet(), readSchema);
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
    if (ReadMetadata->HasProcessingColumnIds() && ReadMetadata->GetProcessingColumnIds().size()) {
        FFColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetProcessingColumnIds(), readSchema);
        if (SpecColumns->Contains(*FFColumns) && !EFColumns->IsEmpty()) {
            FFColumns = std::make_shared<TColumnsSet>(*EFColumns + *SpecColumns);
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("ff_modified", FFColumns->DebugString());
        } else {
//            AFL_VERIFY(!FFColumns->Contains(*SpecColumns))("info", FFColumns->DebugString());
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
    AllUsageColumns = std::make_shared<TColumnsSet>(*FFColumns + *PredicateColumns);

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

}   // namespace NKikimr::NOlap::NReader::NCommon
