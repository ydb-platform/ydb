#include "read_metadata.h"
#include "order_controller.h"
#include <ydb/core/tx/columnshard/columnshard__index_scan.h>
#include <ydb/core/tx/columnshard/columnshard__stats_scan.h>

namespace NKikimr::NOlap {

std::unique_ptr<NColumnShard::TScanIteratorBase> TReadMetadata::StartScan(NColumnShard::TDataTasksProcessorContainer tasksProcessor, const NColumnShard::TScanCounters& scanCounters) const {
    return std::make_unique<NColumnShard::TColumnShardScanIterator>(this->shared_from_this(), tasksProcessor, scanCounters);
}

std::set<ui32> TReadMetadata::GetEarlyFilterColumnIds() const {
    std::set<ui32> result;
    if (LessPredicate) {
        for (auto&& i : LessPredicate->ColumnNames()) {
            result.emplace(IndexInfo.GetColumnId(i));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("early_filter_column", i);
        }
    }
    if (GreaterPredicate) {
        for (auto&& i : GreaterPredicate->ColumnNames()) {
            result.emplace(IndexInfo.GetColumnId(i));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("early_filter_column", i);
        }
    }
    if (Program) {
        for (auto&& i : Program->GetEarlyFilterColumns()) {
            auto id = IndexInfo.GetColumnIdOptional(i);
            if (id) {
                result.emplace(*id);
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("early_filter_column", i);
            }
        }
    }
    if (PlanStep) {
        auto snapSchema = TIndexInfo::ArrowSchemaSnapshot();
        for (auto&& i : snapSchema->fields()) {
            result.emplace(IndexInfo.GetColumnId(i->name()));
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("early_filter_column", i->name());
        }
    }
    return result;
}

std::set<ui32> TReadMetadata::GetPKColumnIds() const {
    std::set<ui32> result;
    for (auto&& i : IndexInfo.GetPrimaryKey()) {
        Y_VERIFY(result.emplace(IndexInfo.GetColumnId(i.first)).second);
    }
    return result;
}

std::set<ui32> TReadMetadata::GetUsedColumnIds() const {
    std::set<ui32> result;
    if (PlanStep) {
        auto snapSchema = TIndexInfo::ArrowSchemaSnapshot();
        for (auto&& i : snapSchema->fields()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("used_column", i->name());
            result.emplace(IndexInfo.GetColumnId(i->name()));
        }
    }
    for (auto&& f : LoadSchema->fields()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("used_column", f->name());
        result.emplace(IndexInfo.GetColumnId(f->name()));
    }
    for (auto&& i : IndexInfo.GetPrimaryKey()) {
        Y_VERIFY(result.contains(IndexInfo.GetColumnId(i.first)));
    }
    return result;
}

TVector<std::pair<TString, NScheme::TTypeInfo>> TReadStatsMetadata::GetResultYqlSchema() const {
    return NOlap::GetColumns(NColumnShard::PrimaryIndexStatsSchema, ResultColumnIds);
}

TVector<std::pair<TString, NScheme::TTypeInfo>> TReadStatsMetadata::GetKeyYqlSchema() const {
    return NOlap::GetColumns(NColumnShard::PrimaryIndexStatsSchema, NColumnShard::PrimaryIndexStatsSchema.KeyColumns);
}

std::unique_ptr<NColumnShard::TScanIteratorBase> TReadStatsMetadata::StartScan(NColumnShard::TDataTasksProcessorContainer /*tasksProcessor*/, const NColumnShard::TScanCounters& /*scanCounters*/) const {
    return std::make_unique<NColumnShard::TStatsIterator>(this->shared_from_this());
}

void TReadStats::PrintToLog() {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)
        ("event", "statistic")
        ("begin", BeginTimestamp)
        ("selected", SelectedIndex)
        ("index_granules", IndexGranules)
        ("index_portions", IndexPortions)
        ("index_batches", IndexBatches)
        ("committed_batches", CommittedBatches)
        ("schema_columns", SchemaColumns)
        ("filter_columns", FilterColumns)
        ("additional_columns", AdditionalColumns)
        ("portions_bytes", PortionsBytes)
        ("data_filter_bytes", DataFilterBytes)
        ("data_additional_bytes", DataAdditionalBytes)
        ("delta_bytes", PortionsBytes - DataFilterBytes - DataAdditionalBytes)
        ("selected_rows", SelectedRows)
        ;
}

NIndexedReader::IOrderPolicy::TPtr TReadMetadata::BuildSortingPolicy() const {
    if (Limit && Sorting != ESorting::NONE && IndexInfo.IsSorted() && IndexInfo.GetSortingKey()->num_fields()) {
        ui32 idx = 0;
        for (auto&& i : IndexInfo.GetPrimaryKey()) {
            if (idx >= IndexInfo.GetSortingKey()->fields().size()) {
                break;
            }
            if (IndexInfo.GetSortingKey()->fields()[idx]->name() != i.first) {
                return std::make_shared<NIndexedReader::TAnySorting>(this->shared_from_this());
            }
            ++idx;
        }
        
        return std::make_shared<NIndexedReader::TPKSortingWithLimit>(this->shared_from_this());
    } else {
        return std::make_shared<NIndexedReader::TAnySorting>(this->shared_from_this());
    }
}

}
