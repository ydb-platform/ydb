#include "constructor.h"
#include "filter_assembler.h"
#include "column_assembler.h"
#include "committed_assembler.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NPlainReader {

TPortionInfo::TPreparedBatchData TAssembleColumnsTaskConstructor::BuildBatchAssembler() {
    auto blobs = ExtractBlobsData();
    THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> blobsDataAssemble;
    for (auto&& i : blobs) {
        blobsDataAssemble.emplace(i.first, i.second);
    }
    for (auto&& i : NullBlocks) {
        AFL_VERIFY(blobsDataAssemble.emplace(i.first, i.second).second);
    }
    auto blobSchema = Context->GetReadMetadata()->GetLoadSchema(PortionInfo->GetMinSnapshot());
    auto readSchema = Context->GetReadMetadata()->GetLoadSchema(Context->GetReadMetadata()->GetSnapshot());
    ISnapshotSchema::TPtr resultSchema;
    if (ColumnIds.size()) {
        resultSchema = std::make_shared<TFilteredSnapshotSchema>(readSchema, ColumnIds);
    } else {
        resultSchema = readSchema;
    }

    return PortionInfo->PrepareForAssemble(*blobSchema, *resultSchema, blobsDataAssemble);
}

void TEFTaskConstructor::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    NConveyor::TScanServiceOperator::SendTaskToExecute(std::make_shared<TAssembleFilter>(Context->GetCommonContext()->GetScanActorId(), BuildBatchAssembler(),
        Context->GetReadMetadata(), Source, ColumnIds, UseEarlyFilter, Context->GetCommonContext()->GetCounters().GetAssembleTasksGuard(), PortionInfo->RecordSnapshotMax()));
}

void TFFColumnsTaskConstructor::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    NConveyor::TScanServiceOperator::SendTaskToExecute(std::make_shared<TAssembleFFBatch>(Context->GetCommonContext()->GetScanActorId(), BuildBatchAssembler(),
        Source, AppliedFilter, Context->GetCommonContext()->GetCounters().GetAssembleTasksGuard()));
}

void TCommittedColumnsTaskConstructor::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    auto blobs = ExtractBlobsData();
    Y_ABORT_UNLESS(NullBlocks.size() == 0);
    Y_ABORT_UNLESS(blobs.size() == 1);
    NConveyor::TScanServiceOperator::SendTaskToExecute(std::make_shared<TCommittedAssembler>(Context->GetCommonContext()->GetScanActorId(), blobs.begin()->second,
        Context->GetReadMetadata(), Source, CommittedBlob, Context->GetCommonContext()->GetCounters().GetAssembleTasksGuard()));
}

bool IFetchTaskConstructor::DoOnError(const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("error_on_blob_reading", range.ToString())("scan_actor_id", Context->GetCommonContext()->GetScanActorId())("status", status.GetErrorMessage())("status_code", status.GetStatus());
    NActors::TActorContext::AsActorContext().Send(Context->GetCommonContext()->GetScanActorId(), std::make_unique<NConveyor::TEvExecution::TEvTaskProcessedResult>(TConclusionStatus::Fail("cannot read blob range " + range.ToString())));
    return false;
}

}
