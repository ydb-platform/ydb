#include "constructor.h"
#include "filter_assembler.h"
#include "column_assembler.h"
#include "committed_assembler.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NPlainReader {

THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> TAssembleColumnsTaskConstructor::BuildBatchAssembler() {
    auto blobs = ExtractBlobsData();
    THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> blobsDataAssemble;
    for (auto&& i : blobs) {
        blobsDataAssemble.emplace(i.first, i.second);
    }
    for (auto&& i : NullBlocks) {
        AFL_VERIFY(blobsDataAssemble.emplace(i.first, i.second).second);
    }
    return blobsDataAssemble;
}

void TEFTaskConstructor::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    auto task = std::make_shared<TAssembleFilter>(Context, PortionInfo, Source, ColumnIds, UseEarlyFilter, BuildBatchAssembler());
    task->SetPriority(NConveyor::ITask::EPriority::Normal);
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
}

void TFFColumnsTaskConstructor::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    auto task = std::make_shared<TAssembleFFBatch>(Context, PortionInfo, Source, ColumnIds, BuildBatchAssembler(), AppliedFilter);
    task->SetPriority(NConveyor::ITask::EPriority::High);
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
}

void TCommittedColumnsTaskConstructor::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    auto blobs = ExtractBlobsData();
    Y_ABORT_UNLESS(NullBlocks.size() == 0);
    Y_ABORT_UNLESS(blobs.size() == 1);
    auto task = std::make_shared<TCommittedAssembler>(Context->GetCommonContext()->GetScanActorId(), blobs.begin()->second,
        Context->GetReadMetadata(), Source, CommittedBlob, Context->GetCommonContext()->GetCounters().GetAssembleTasksGuard());
    task->SetPriority(NConveyor::ITask::EPriority::High);
    NConveyor::TScanServiceOperator::SendTaskToExecute(task);
}

bool IFetchTaskConstructor::DoOnError(const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("error_on_blob_reading", range.ToString())("scan_actor_id", Context->GetCommonContext()->GetScanActorId())("status", status.GetErrorMessage())("status_code", status.GetStatus());
    NActors::TActorContext::AsActorContext().Send(Context->GetCommonContext()->GetScanActorId(), std::make_unique<NConveyor::TEvExecution::TEvTaskProcessedResult>(TConclusionStatus::Fail("cannot read blob range " + range.ToString())));
    return false;
}

}
