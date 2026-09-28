#include "constructor.h"

#include <ydb/core/tx/columnshard/blobs_reader/actor.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_SCAN

namespace NKikimr::NOlap::NReader::NCommon {

void TBlobsFetcherTask::TStartJob::Start(std::unique_ptr<TDataSourceLease> sourceLease) {
    auto task = std::make_shared<TBlobsFetcherTask>(ReadActions, std::move(sourceLease), Step, TaskCustomer);
    NActors::TActivationContext::AsActorContext().Register(new NBlobOperations::NRead::TActor(task));
}

void TBlobsFetcherTask::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    auto& source = SourceLease->GetSource();
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("fbf"));
    source.MutableStageData().AddBlobs(source.DecodeBlobAddresses(ExtractBlobsData()));
    AFL_VERIFY(Step.Next());
    auto task = std::make_shared<TStepAction>(std::move(SourceLease), std::move(Step), Context->GetCommonContext()->GetScanActorId(), false);
    Context->GetCommonContext()->SendTaskToExecute(task);
}

bool TBlobsFetcherTask::DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, SourceLease->GetSource().AddEvent("ebf"));
    YDB_LOG_ERROR("",
        {"errorOnBlobReading", range},
        {"scanActorId", Context->GetCommonContext()->GetScanActorId()},
        {"status", status.GetErrorMessage()},
        {"statusCode", status.GetStatus()},
        {"storageId", storageId});
    NActors::TActorContext::AsActorContext().Send(Context->GetCommonContext()->GetScanActorId(),
        std::make_unique<NColumnShard::TEvPrivate::TEvTaskProcessedResult>(
            TConclusionStatus::Fail(TStringBuilder{} << "Error reading blob range for data: " << range.ToString()
                                                     << ", error: " << status.GetErrorMessage()
                                                     << ", status: " << NKikimrProto::EReplyStatus_Name(status.GetStatus())), std::move(Guard)));
    return false;
}

TBlobsFetcherTask::TBlobsFetcherTask(const std::vector<std::shared_ptr<IBlobsReadingAction>>& readActions,
    std::unique_ptr<TDataSourceLease> sourceLease, const TFetchingScriptCursor& step, const TString& taskCustomer)
    : TBase(readActions, taskCustomer, "")
    , SourceLease(std::move(sourceLease))
    , Step(step)
    , Context(SourceLease->GetSource().GetContext())
    , Guard(Context->GetCommonContext()->GetCounters().GetFetchBlobsGuard())
{
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, SourceLease->GetSource().AddEvent("sbf"));
}

void TColumnsFetcherTask::TStartJob::Start(std::unique_ptr<TDataSourceLease> sourceLease) {
    auto task = std::make_shared<TColumnsFetcherTask>(std::move(ReadActions), Fetchers, std::move(sourceLease), Cursor, TaskCustomer);
    NActors::TActivationContext::AsActorContext().Register(new NBlobOperations::NRead::TActor(task));
}

bool TColumnsFetcherTask::DoOnError(const TString& storageId, const TBlobRange& range, const IBlobsReadingAction::TErrorStatus& status) {
    YDB_LOG_ERROR_COMP(NKikimrServices::TX_COLUMNSHARD_SCAN, "",
        {"errorOnBlobReading", range},
        {"scanActorId", SourceLease->GetSource().GetContext()->GetCommonContext()->GetScanActorId()},
        {"status", status.GetErrorMessage()},
        {"statusCode", status.GetStatus()},
        {"storageId", storageId});
    NActors::TActorContext::AsActorContext().Send(SourceLease->GetSource().GetContext()->GetCommonContext()->GetScanActorId(),
        std::make_unique<NColumnShard::TEvPrivate::TEvTaskProcessedResult>(
            TConclusionStatus::Fail(TStringBuilder{} << "Error reading blob range for columns: " << range.ToString()
                                                     << ", error: " << status.GetErrorMessage()
                                                     << ", status: " << NKikimrProto::EReplyStatus_Name(status.GetStatus())), std::move(Guard)));
    return false;
}

void TColumnsFetcherTask::DoOnDataReady(const std::shared_ptr<NResourceBroker::NSubscribe::TResourcesGuard>& /*resourcesGuard*/) {
    auto& source = SourceLease->GetSource();
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("cf_reply"));
    const TMonotonic start = TMonotonic::Now();
    NBlobOperations::NRead::TCompositeReadBlobs blobsData = ExtractBlobsData();
    blobsData.Merge(std::move(ProvidedBlobs));
    TReadActionsCollection readActions;
    auto* signals = source.GetExecutionContext().GetCurrentStepSignalsOptional();
    if (signals) {
        signals->AddBytes(blobsData.GetTotalBlobsSize());
        const auto& counters = source.GetContext()->GetCommonContext()->GetCounters();
        counters.CountersForStep(this->Cursor.GetName()).RawBytesRead->Add(blobsData.GetTotalBlobsSize());
        counters.AddRawBytes(blobsData.GetTotalBlobsSize());
    }
    for (auto&& [_, i] : DataFetchers) {
        i->OnDataReceived(readActions, blobsData);
    }
    AFL_VERIFY(blobsData.IsEmpty());
    if (readActions.IsEmpty()) {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("cf_finished"));
        for (auto&& i : DataFetchers) {
            source.MutableStageData().AddFetcher(i.second);
        }
        const auto& commonContext = *source.GetContext()->GetCommonContext();
        auto task = std::make_shared<TStepAction>(std::move(SourceLease), std::move(Cursor), commonContext.GetScanActorId(), false);
        commonContext.SendTaskToExecute(task);
    } else {
        FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source.AddEvent("cf_next"));
        std::shared_ptr<TColumnsFetcherTask> nextReadTask = std::make_shared<TColumnsFetcherTask>(
            std::move(readActions), DataFetchers, std::move(SourceLease), std::move(Cursor), GetTaskCustomer(), GetExternalTaskId());
        NActors::TActivationContext::AsActorContext().Register(new NOlap::NBlobOperations::NRead::TActor(nextReadTask));
    }
    if (signals) {
        signals->AddExecutionDuration(TMonotonic::Now() - start);
    }
}

}   // namespace NKikimr::NOlap::NReader::NCommon
