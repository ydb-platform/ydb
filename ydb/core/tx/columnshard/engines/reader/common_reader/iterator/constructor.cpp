#include "constructor.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

namespace NKikimr::NOlap::NReader::NCommon {

void TBlobsFetcherTask::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("fbf"));
    Source->MutableStageData().AddBlobs(Source->DecodeBlobAddresses(ExtractBlobsData()));
    AFL_VERIFY(Step.Next());
    auto task = std::make_shared<TStepAction>(std::move(Source), std::move(Step), Context->GetCommonContext()->GetScanActorId(), false);
    NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, Context->GetCommonContext()->GetConveyorProcessId());
}

bool TBlobsFetcherTask::DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("ebf"));
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("error_on_blob_reading", range.ToString())(
        "scan_actor_id", Context->GetCommonContext()->GetScanActorId())("status", status.GetErrorMessage())("status_code", status.GetStatus())(
        "storage_id", storageId);
    NActors::TActorContext::AsActorContext().Send(Context->GetCommonContext()->GetScanActorId(),
        std::make_unique<NColumnShard::TEvPrivate::TEvTaskProcessedResult>(
            TConclusionStatus::Fail(TStringBuilder{} << "Error reading blob range for data: " << range.ToString()
                                                     << ", error: " << status.GetErrorMessage()
                                                     << ", status: " << NKikimrProto::EReplyStatus_Name(status.GetStatus())),
            std::move(Guard)));
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
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("sbf"));
}

void TColumnsFetcherTask::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("cf_reply"));
    const TMonotonic start = TMonotonic::Now();
    NBlobOperations::NRead::TCompositeReadBlobs blobsData = ExtractBlobsData();
    blobsData.Merge(std::move(ProvidedBlobs));
    TReadActionsCollection readActions;
    auto* signals = Source->GetExecutionContext().GetCurrentStepSignalsOptional();
    if (signals) {
        signals->AddBytes(blobsData.GetTotalBlobsSize());
        Source->GetContext()->GetCommonContext()->GetCounters().AddRawBytes(blobsData.GetTotalBlobsSize());
    }
    for (auto&& [_, i] : DataFetchers) {
        i->OnDataReceived(readActions, blobsData);
    }
    AFL_VERIFY(blobsData.IsEmpty());
    if (readActions.IsEmpty()) {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("cf_finished"));
        for (auto&& i : DataFetchers) {
            Source->MutableStageData().AddFetcher(i.second);
        }
        auto convProcessId = Source->GetContext()->GetCommonContext()->GetConveyorProcessId();
        auto task = std::make_shared<TStepAction>(std::move(Source), std::move(Cursor), Source->GetContext()->GetCommonContext()->GetScanActorId(), false);
        NConveyorComposite::TScanServiceOperator::SendTaskToExecute(task, convProcessId);
    } else {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, Source->AddEvent("cf_next"));
        std::shared_ptr<TColumnsFetcherTask> nextReadTask = std::make_shared<TColumnsFetcherTask>(
            std::move(readActions), DataFetchers, Source, std::move(Cursor), GetTaskCustomer(), GetExternalTaskId());
        NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(nextReadTask));
    }
    if (signals) {
        signals->AddExecutionDuration(TMonotonic::Now() - start);
    }
}

}   // namespace NKikimr::NOlap::NReader::NCommon
