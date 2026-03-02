#include "pk_fetcher.h"
#include "private_events.h"

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/formats/arrow/rows/view.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

void TFetchPKKeysTask::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD_SCAN)("task", "fetch_pk_keys");
    
    // Convert TColumnData to TGeneralContainer similar to TDuplicateSourceCacheResult
    THashMap<ui64, THashMap<ui32, std::shared_ptr<NArrow::NAccessor::IChunkedArray>>> columnsByPortion;
    for (auto&& [address, data] : PKData) {
        AFL_VERIFY(columnsByPortion[address.GetPortionId()].emplace(address.GetColumnId(), data).second);
    }
    
    // Get the main portion data
    const ui64 mainPortionId = Context.GetMainPortionId();
    auto* portionColumns = columnsByPortion.FindPtr(mainPortionId);
    AFL_VERIFY(portionColumns)("portion_id", mainPortionId);
    
    // Verify that all required PK columns are loaded
    auto pkSchema = Context.GetPKSchema();
    AFL_VERIFY(pkSchema);
    for (int i = 0; i < pkSchema->num_fields(); ++i) {
        auto fieldName = pkSchema->field(i)->name();
        auto columnId = Context.GetSnapshotSchema()->GetColumnIdVerified(fieldName);
        AFL_VERIFY(portionColumns->FindPtr(columnId))("column_id", columnId)("field_name", fieldName);
    }
    
    // Build fields and columns from loaded PK data
    // Use the column IDs that were actually loaded, not from Context.GetColumns()
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> sortedColumns;
    
    // Sort columns by column ID to ensure consistent order
    std::vector<ui32> sortedColumnIds;
    for (const auto& [columnId, _] : *portionColumns) {
        sortedColumnIds.push_back(columnId);
    }
    // std::sort(sortedColumnIds.begin(), sortedColumnIds.end());
    
    for (const ui32 columnId : sortedColumnIds) {
        auto column = portionColumns->FindPtr(columnId);
        AFL_VERIFY(column)("column_id", columnId);
        
        // Find the field definition from context columns
        auto fieldIt = Context.GetColumns().find(columnId);
        AFL_VERIFY(fieldIt != Context.GetColumns().end());
        fields.emplace_back(fieldIt->second);
        
        sortedColumns.emplace_back(*column);
    }
    
    // Create container
    auto pkContainer = std::make_shared<NArrow::TGeneralContainer>(fields, std::move(sortedColumns));
    auto pkTable = pkContainer->BuildTableVerified();
    
    // Build list of PK keys from main portion
    auto pkKeys = std::make_shared<std::vector<NArrow::TSimpleRow>>();
    pkKeys->reserve(pkTable->num_rows());
    
    // Convert table to record batches and create TSimpleRow from each row
    arrow::TableBatchReader reader(*pkTable);
    std::shared_ptr<arrow::RecordBatch> batch;
    while (reader.ReadNext(&batch).ok() && batch) {
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            // Create TSimpleRowContent from the batch row
            TString rowString = NArrow::TSimpleRowViewV0::BuildString(batch, i);
            NArrow::TSimpleRowContent rowContent(rowString);
            // Build TSimpleRow from content and schema
            pkKeys->emplace_back(rowContent.Build(pkSchema));
        }
    }
    
    // Store PK keys in context for next stage
    Context.SetMainPortionPKKeys(pkKeys);
    
    // Send result to continue with second stage
    TActivationContext::AsActorContext().Send(Context.GetOwner(),
        new NPrivate::TEvPKKeysFetched(std::move(Context), Context.MakeResultInFlightGuard()));
}

void TFetchPKKeysTask::DoOnCannotExecute(const TString& reason) {
    TActivationContext::AsActorContext().Send(Context.GetOwner(),
        new NPrivate::TEvFilterConstructionResult(TConclusionStatus::Fail(reason), Context.MakeResultInFlightGuard()));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering