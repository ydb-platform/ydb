#include "context.h"

#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>

namespace NKikimr::NOlap::NReader::NCommon {

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : CommonContext(commonContext) {
    ReadMetadata = CommonContext->GetReadMetadataPtrVerifiedAs<TReadMetadata>();

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
    ProcessScopeGuard = ProcessMemoryGuard->BuildScopeGuard(GetCommonContext()->GetScanId());

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

/*
Returns the portion state at the moment of the scan planning on the column shard.

Scan reads portions in separate actors from the column shard, so the portions state may
be changed during the scan. To avoid anomalies caused by the fact that scan may see different
portion states, we need a way to get a stable portion state during the whole scan. Here it is.
*/
TPortionStateAtScanStart TSpecialReadContext::GetPortionStateAtScanStart(const TPortionInfo& portionInfo) const {
    bool committed = false;
    bool conflicting = false;
    TSnapshot maxRecordSnapshot = TSnapshot::Zero();
    if (portionInfo.GetPortionType() == EPortionType::Compacted) {
        // compacted portions are stable and not conflicting,
        // they have max snapshot less or equal to the request snapshot
        AFL_VERIFY(portionInfo.RecordSnapshotMax() <= GetReadMetadata()->GetRequestSnapshot())("portion_info", portionInfo.DebugString())("request_snapshot", GetReadMetadata()->GetRequestSnapshot().DebugString());
        committed = true;
        conflicting = false;
        maxRecordSnapshot = portionInfo.RecordSnapshotMax();
    } else {
        const auto& wPortionInfo = static_cast<const TWrittenPortionInfo&>(portionInfo);
        auto maybeConflicting = GetReadMetadata()->MayWriteBeConflicting(wPortionInfo.GetInsertWriteId());
        if (maybeConflicting) {
            // uncommitted portions by other txs
            committed = false;
            conflicting = true;
        } else if (!wPortionInfo.IsCommitted()) {
            // uncommitted portions by the current tx
            committed = false;
            conflicting = false;
        } else if (wPortionInfo.RecordSnapshotMax() > GetReadMetadata()->GetRequestSnapshot()) {
            // portions that were committed at the moment of the scan start
            // but have snapshot greater than the request snapshot, so the current tx
            // does not see them and may conflict with them
            committed = true;
            conflicting = true;
            maxRecordSnapshot = wPortionInfo.RecordSnapshotMax();
        } else {
            // committed, not yet compacted portions, visible to the current tx
            committed = true;
            conflicting = false;
            maxRecordSnapshot = wPortionInfo.RecordSnapshotMax();
        }
    }
    return TPortionStateAtScanStart {
        .Committed = committed,
        .Conflicting = conflicting,
        .MaxRecordSnapshot = maxRecordSnapshot
    };
}

}   // namespace NKikimr::NOlap::NReader::NCommon
