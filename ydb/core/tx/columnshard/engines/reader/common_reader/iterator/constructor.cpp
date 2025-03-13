#include "constructor.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NReader::NCommon {

void TBlobsFetcherTask::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    Source->MutableStageData().AddBlobs(Source->DecodeBlobAddresses(ExtractBlobsData()));
    AFL_VERIFY(Step.Next());
    auto task = std::make_shared<TStepAction>(Source, std::move(Step), Context->GetCommonContext()->GetScanActorId());
    NConveyor::TScanServiceOperator::SendTaskToExecute(task, Context->GetCommonContext()->GetConveyorProcessId());
}

bool TBlobsFetcherTask::DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("error_on_blob_reading", range.ToString())(
        "scan_actor_id", Context->GetCommonContext()->GetScanActorId())("status", status.GetErrorMessage())("status_code", status.GetStatus())(
        "storage_id", storageId);
    NActors::TActorContext::AsActorContext().Send(
        Context->GetCommonContext()->GetScanActorId(), std::make_unique<NColumnShard::TEvPrivate::TEvTaskProcessedResult>(
                                                           TConclusionStatus::Fail(TStringBuilder{} << "Error reading blob range for data: " << range.ToString() << ", error: " << status.GetErrorMessage() << ", status: " << NKikimrProto::EReplyStatus_Name(status.GetStatus()))));
    return false;
}

TBlobsFetcherTask::TBlobsFetcherTask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions,
    const std::shared_ptr<NCommon::IDataSource>& sourcePtr, const TFetchingScriptCursor& step,
    const std::shared_ptr<NCommon::TSpecialReadContext>& context, const TString& taskCustomer, const TString& externalTaskId)
    : TBase(readActions, taskCustomer, externalTaskId)
    , Source(sourcePtr)
    , Step(step)
    , Context(context)
    , Guard(Context->GetCommonContext()->GetCounters().GetFetchBlobsGuard()) {
}

void TColumnsFetcherTask::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    const TMonotonic start = TMonotonic::Now();
    NBlobOperations::NRead::TCompositeReadBlobs blobsData = ExtractBlobsData();
    blobsData.Merge(std::move(ProvidedBlobs));
    TReadActionsCollection readActions;
    if (auto* signals = Source->GetExecutionContext().GetCurrentStepSignalsOptional()) {
        signals->AddBytes(blobsData.GetTotalBlobsSize());
    }
    for (auto&& [_, i] : DataFetchers) {
        i->OnDataReceived(readActions, blobsData);
    }
    AFL_VERIFY(blobsData.IsEmpty());
    if (readActions.IsEmpty()) {
        for (auto&& i : DataFetchers) {
            Source->MutableStageData().AddFetcher(i.second);
        }
        auto task = std::make_shared<TStepAction>(Source, std::move(Cursor), Source->GetContext()->GetCommonContext()->GetScanActorId());
        NConveyor::TScanServiceOperator::SendTaskToExecute(task, Source->GetContext()->GetCommonContext()->GetConveyorProcessId());
    } else {
        std::shared_ptr<TColumnsFetcherTask> nextReadTask = std::make_shared<TColumnsFetcherTask>(
            std::move(readActions), DataFetchers, Source, std::move(Cursor), GetTaskCustomer(), GetExternalTaskId());
        NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(nextReadTask));
    }
    if (auto* signals = Source->GetExecutionContext().GetCurrentStepSignalsOptional()) {
        signals->AddExecutionDuration(TMonotonic::Now() - start);
    }
}

}   // namespace NKikimr::NOlap::NReader::NCommon
