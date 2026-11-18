#include "context.h"

#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/common/limits.h>
#include <ydb/core/tx/columnshard/engines/portions/written.h>
#include <ydb/core/tx/columnshard/engines/reader/common/scan_memory_limiter.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/constructor/read_metadata.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/util/bit_run_reader.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NCommon {

TSpecialReadContext::TSpecialReadContext(const std::shared_ptr<TReadContext>& commonContext)
    : CommonContext(commonContext)
    , GroupedMemoryLimiterOperator(commonContext->GetReadMetadataPtrVerifiedAs<TReadMetadata>()->GetGroupedMemoryLimiterOperator())
{
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

    auto scanMemoryLimit = TGlobalLimits::ScanMemoryLimit;

    if (HasAppData()) {
        if (AppData()->ColumnShardConfig.HasScanMemoryLimit()) {
            scanMemoryLimit = AppData()->ColumnShardConfig.GetScanMemoryLimit();
        }
    }

    std::vector<std::shared_ptr<NGroupedMemoryManager::TStageFeatures>> stages = {
        BuildScanStageFeatures(GroupedMemoryLimiterOperator, stagePrefix + "::ACCESSORS", kffAccessors * scanMemoryLimit),
        BuildScanStageFeatures(GroupedMemoryLimiterOperator, stagePrefix + "::FILTER", kffFilter * scanMemoryLimit),
        BuildScanStageFeatures(GroupedMemoryLimiterOperator, stagePrefix + "::FETCHING", kffFetching * scanMemoryLimit),
        BuildScanStageFeatures(GroupedMemoryLimiterOperator, stagePrefix + "::MERGE", kffMerge * scanMemoryLimit)
    };
    ProcessMemoryGuard = BuildScanProcessGuard(GroupedMemoryLimiterOperator, ReadMetadata->GetTxId(), stages);
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
            YDB_LOG_DEBUG("",
                {"ffModified", FFColumns->DebugString()});
        } else {
            //            YDB_LOG_VERIFY(!FFColumns->Contains(*SpecColumns),
            //                {"info", FFColumns->DebugString()});
            YDB_LOG_DEBUG("",
                {"ffFirst", FFColumns->DebugString()});
        }
    } else {
        FFColumns = EFColumns;
    }
    if (FFColumns->IsEmpty()) {
        ProgramInputColumns = SpecColumns;
    } else {
        ProgramInputColumns = FFColumns;
    }

    const auto& readNotNullIds = readSchema->GetIndexInfo().GetReadNotNullColumnIds();
    ReadNotNullColumns = readNotNullIds.empty() ? EmptyColumns : std::make_shared<TColumnsSet>(readNotNullIds, readSchema);
    AllUsageColumns = std::make_shared<TColumnsSet>(*FFColumns + *PredicateColumns + *ReadNotNullColumns);

    PKColumns = std::make_shared<TColumnsSet>(ReadMetadata->GetPKColumnIds(), readSchema);
    MergeColumns = std::make_shared<TColumnsSet>(*PKColumns + *SpecColumns);

    YDB_LOG_DEBUG("",
        {"columnsContextInfo", DebugString()});
}

void TSpecialReadContext::IncludeReadNotNullColumnsForMerge() {
    if (!ReadNotNullColumns->IsEmpty()) {
        ProgramInputColumns = std::make_shared<TColumnsSet>(*ProgramInputColumns + *ReadNotNullColumns);
        FFColumns = ProgramInputColumns;
    }
}

void TSpecialReadContext::ApplyReadNotNullFilter(NArrow::NAccessor::TAccessorsCollection& table) const {
    for (const auto columnId : ReadNotNullColumns->GetColumnIds()) {
        const auto& accessor = table.GetAccessorVerified(columnId);
        if (!accessor->HasNulls().value_or(true)) {
            continue;
        }

        const auto column = accessor->GetChunkedArray();
        if (!column->null_count()) {
            continue;
        }

        auto valid = NArrow::TColumnFilter::BuildAllowFilter();
        for (const auto& chunk : column->chunks()) {
            const auto nulls = chunk->null_count();
            if (!nulls || nulls == chunk->length()) {
                valid.Add(!nulls, chunk->length());
                continue;
            }

            arrow::internal::BitRunReader bits(chunk->null_bitmap_data(), chunk->offset(), chunk->length());
            for (auto run = bits.NextRun(); run.length; run = bits.NextRun()) {
                valid.Add(run.set, run.length);
            }
        }

        table.AddFilter(valid);
        if (table.IsEmptyFilter()) {
            break;
        }
    }
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
