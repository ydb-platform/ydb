#include "readproxy.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/dread_cache_service/caching_service.h>
#include <ydb/core/persqueue/pqtablet/batching/batch_processor.h>
#include <ydb/core/persqueue/pqtablet/common/constants.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <util/generic/algorithm.h>

#include <limits>

#define YDB_LOG_THIS_FILE_COMPONENT Service

namespace NKikimr::NPQ {

using namespace NActors;

namespace {

bool HasBatchMessages(const NKikimrClient::TCmdReadResult& readResult) {
    return AnyOf(readResult.GetResult(), [](const auto& result) {
        return result.GetIsBatch();
    });
}

} // namespace

class TReadProxy : public TBaseTabletActor<TReadProxy>, private TConstantLogPrefix {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_ANS_ACTOR;
    }

    TReadProxy(const TActorId& sender, const ui64 tabletId, const TActorId& tablet, ui64 tabletGeneration,
               const TDirectReadKey& directReadKey, const NKikimrClient::TPersQueueRequest& request,
               const TActorId& batchProcessorActor)
        : TBaseTabletActor(tabletId, tablet, NKikimrServices::PERSQUEUE)
        , Sender(sender)
        , TabletGeneration(tabletGeneration)
        , Request(request)
        , Response(new TEvPersQueue::TEvResponse)
        , DirectReadKey(directReadKey)
        , InitialReadOffset(request.GetPartitionRequest().GetCmdRead().GetOffset())
        , CanReadBatches(request.GetPartitionRequest().GetCmdRead().GetCanReadBatches())
        , BatchProcessorActor(batchProcessorActor)
    {
        AFL_ENSURE(Request.HasPartitionRequest() && Request.GetPartitionRequest().HasCmdRead());
        AFL_ENSURE(Request.GetPartitionRequest().GetCmdRead().GetPartNo() == 0); //partial request are not allowed, otherwise remove ReadProxy
        if (!directReadKey.SessionId.empty()) {
            DirectReadKey.ReadId = Request.GetPartitionRequest().GetCmdRead().GetDirectReadId();
        }
    }

    void Bootstrap(const TActorContext&)
    {
        Become(&TThis::StateFunc);
    }

    TString BuildLogPrefix() const override {
        return TStringBuilder() <<"[ReadProxy][" << SelfId() << "] ";
    }

private:
    void ReplyErrorAndDie(const TActorContext& ctx, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error)
    {
        if (!Response) {
            PassAway();
            return;
        }
        Response->Record.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
        Response->Record.SetErrorCode(errorCode);
        Response->Record.SetErrorReason(error);
        if (Request.GetPartitionRequest().HasCookie() && !Response->Record.GetPartitionResponse().HasCookie()) {
            Response->Record.MutablePartitionResponse()->SetCookie(Request.GetPartitionRequest().GetCookie());
        }
        ctx.Send(Sender, Response.Release());
        PassAway();
    }

    void Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx)
    {
        ReplyErrorAndDie(ctx, NPersQueue::NErrorCode::INITIALIZING, "tablet will be restarted right now");
    }

    void Handle(TEvents::TEvUndelivered::TPtr&, const TActorContext& ctx)
    {
        ReplyErrorAndDie(ctx, NPersQueue::NErrorCode::READ_NOT_DONE, "batch processor is unavailable");
    }

    void SendResponse(const TActorContext& ctx, bool isDirectRead, const NKikimrClient::TCmdReadResult& readResult,
                      const NKikimrClient::TPersQueuePartitionResponse& partitionResponse)
    {
        if (isDirectRead) {
            auto* prepareResponse = Response->Record.MutablePartitionResponse()->MutableCmdPrepareReadResult();
            auto sizeEstimate = Request.GetPartitionRequest().GetCmdRead().GetSizeEstimate();
            sizeEstimate = sizeEstimate ? sizeEstimate : PreparedResponse->GetPartitionResponse().ByteSize();
            PreparedResponse->MutablePartitionResponse()->MutableCmdPrepareReadResult()->SetBytesSizeEstimate(sizeEstimate);
            prepareResponse->SetBytesSizeEstimate(sizeEstimate);
            prepareResponse->SetDirectReadId(DirectReadKey.ReadId);
            prepareResponse->SetReadOffset(readResult.GetRealReadOffset());
            prepareResponse->SetLastOffset(readResult.GetLastOffset());
            prepareResponse->SetEndOffset(readResult.GetEndOffset());

            prepareResponse->SetSizeLag(readResult.GetSizeLag());
            Response->Record.MutablePartitionResponse()->SetCookie(partitionResponse.GetCookie());
            if (readResult.ResultSize()) {
                prepareResponse->SetWriteTimestampMS(readResult.GetResult(readResult.ResultSize() - 1).GetWriteTimestampMS());
            }
            Response->Record.SetStatus(PreparedResponse->GetStatus());
            Response->Record.SetErrorCode(PreparedResponse->GetErrorCode());
            ctx.Send(
                MakePQDReadCacheServiceActorId(),
                new TEvPQ::TEvStageDirectReadData(DirectReadKey, TabletGeneration, PreparedResponse)
            );
        }
        ctx.Send(Sender, Response.Release());
        PassAway();
    }

    void TryProcessBatchOrSendResponse(const TActorContext& ctx, bool isDirectRead,
                                       const NKikimrClient::TCmdReadResult& readResult,
                                       const NKikimrClient::TPersQueuePartitionResponse& partitionResponse)
    {
        const auto& responseRecord = isDirectRead ? *PreparedResponse : Response->Record;
        AFL_ENSURE(responseRecord.HasPartitionResponse() && responseRecord.GetPartitionResponse().HasCmdReadResult());
        const auto& cmdReadResult = responseRecord.GetPartitionResponse().GetCmdReadResult();

        if (!CanReadBatches && HasBatchMessages(cmdReadResult)) {
            if (!BatchProcessorActor) {
                ReplyErrorAndDie(ctx, NPersQueue::NErrorCode::READ_NOT_DONE, "batch processor is unavailable");
                return;
            }

            PendingDirectRead = isDirectRead;
            PendingPartitionResponse.CopyFrom(partitionResponse);

            auto proxyEvent = MakeHolder<TEvPQ::TEvProxyResponse>(0, false);
            proxyEvent->Response->CopyFrom(responseRecord);

            const auto& cmdRead = Request.GetPartitionRequest().GetCmdRead();
            ctx.Send(BatchProcessorActor, new NBatching::TEvProcessBatch(NBatching::TReadProcessingContext{
                .User = cmdRead.GetClientId(),
                .PartitionId = static_cast<ui32>(Request.GetPartitionRequest().GetPartition()),
                .Offset = InitialReadOffset,
                .Count = cmdRead.HasCount() ? static_cast<ui32>(cmdRead.GetCount()) : std::numeric_limits<ui32>::max(),
                .LastOffset = cmdRead.GetLastOffset() > 0 ? static_cast<ui64>(cmdRead.GetLastOffset()) : 0,
                .ResponseActor = SelfId(),
                .Event = std::move(proxyEvent)}), IEventHandle::FlagTrackDelivery);
            return;
        }

        SendResponse(ctx, isDirectRead, readResult, partitionResponse);
    }

    void DropIncompleteLastIfAny(NKikimrClient::TCmdReadResult* partResp)
    {
        if (!partResp || partResp->ResultSize() == 0) {
            return;
        }
        const auto& last = partResp->GetResult(partResp->ResultSize() - 1);
        if (last.HasPartNo() && last.GetPartNo() + 1 < last.GetTotalParts()) {
            LastSkipOffset = last.GetOffset();
            partResp->MutableResult()->RemoveLast();
        }
    }

    void ContinueFromSkippedOffset()
    {
        Request.SetRequestId(TMP_REQUEST_MARKER);
        Request.MutablePartitionRequest()->MutableCmdRead()->SetOffset(*LastSkipOffset + 1);
        Request.MutablePartitionRequest()->MutableCmdRead()->SetPartNo(0);
        THolder<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
        req->Record = Request;
        Send(TabletActorId, req.Release());
        InitialRequest = true;
    }

    // Follow-up came back empty or with an error. Keep complete messages from the first portion
    // instead of CopyFrom-ing the follow-up over them. Returns true if this call consumed the event.
    bool FinishWithAssembledOnFailedFollowUp(const TActorContext& ctx, const NKikimrClient::TResponse& record)
    {
        if (InitialRequest) {
            return false;
        }
        const bool isDirectRead = DirectReadKey.ReadId != 0;
        auto& responseRecord = isDirectRead && PreparedResponse ? *PreparedResponse : Response->Record;
        if (!responseRecord.HasPartitionResponse() || !responseRecord.GetPartitionResponse().HasCmdReadResult()) {
            return false;
        }
        auto* partResp = responseRecord.MutablePartitionResponse()->MutableCmdReadResult();
        DropIncompleteLastIfAny(partResp);
        if (partResp->ResultSize() == 0) {
            if (!LastSkipOffset.Defined()) {
                return false;
            }
            ContinueFromSkippedOffset();
            return true;
        }
        NKikimrClient::TPersQueuePartitionResponse partitionResponse;
        if (record.HasPartitionResponse()) {
            partitionResponse.CopyFrom(record.GetPartitionResponse());
        } else {
            partitionResponse.CopyFrom(responseRecord.GetPartitionResponse());
        }
        const NKikimrClient::TCmdReadResult* readResult =
            record.HasPartitionResponse() && record.GetPartitionResponse().HasCmdReadResult()
                ? &record.GetPartitionResponse().GetCmdReadResult()
                : partResp;
        TryProcessBatchOrSendResponse(ctx, isDirectRead, *readResult, partitionResponse);
        return true;
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx)
    {
        AFL_ENSURE(Response);
        const auto& record = ev->Get()->Record;
        auto isDirectRead = DirectReadKey.ReadId != 0;
        if (!record.HasPartitionResponse()
            || !record.GetPartitionResponse().HasCmdReadResult()
            || record.GetStatus() != NMsgBusProxy::MSTATUS_OK
            || record.GetErrorCode() != NPersQueue::NErrorCode::OK
            || (record.GetPartitionResponse().GetCmdReadResult().ResultSize() == 0 && !isDirectRead)
        ) {
            if (FinishWithAssembledOnFailedFollowUp(ctx, record)) {
                return;
            }
            Response->Record.CopyFrom(record);
            ctx.Send(Sender, Response.Release());
            PassAway();
            return;
        }
        const auto& readResult = record.GetPartitionResponse().GetCmdReadResult();
        if (isDirectRead) {
            if (!PreparedResponse) {
                PreparedResponse = std::make_shared<NKikimrClient::TResponse>();
            }
        }

        auto& responseRecord = isDirectRead ? *PreparedResponse : Response->Record;
        responseRecord.SetStatus(NMsgBusProxy::MSTATUS_OK);
        responseRecord.SetErrorCode(NPersQueue::NErrorCode::OK);

        const auto* appData = AppData(ctx);
        const bool skipObsoleteTimestamps = appData->FeatureFlags.GetEnableSkipMessagesWithObsoleteTimestamp();
        ui64 readFromTimestampMs = PreciseReadFromTimestampBehaviourEnabled(*appData)
                                   ? (responseRecord.HasPartitionResponse()
                                        ? responseRecord.GetPartitionResponse().GetCmdReadResult().GetReadFromTimestampMs()
                                        : readResult.GetReadFromTimestampMs())
                                   : 0;

        if (!responseRecord.HasPartitionResponse()) {
            auto partResp = responseRecord.MutablePartitionResponse();
            auto readRes = partResp->MutableCmdReadResult();
            readRes->SetBlobsFromDisk(readResult.GetBlobsFromDisk());
            readRes->SetBlobsFromCache(readResult.GetBlobsFromCache());
            if (skipObsoleteTimestamps) {
                readRes->SetReadFromTimestampMs(readFromTimestampMs);
            }
        }
        if (record.GetPartitionResponse().HasCookie()) {
            responseRecord.MutablePartitionResponse()->SetCookie(record.GetPartitionResponse().GetCookie());
        }

        auto partResp = responseRecord.MutablePartitionResponse()->MutableCmdReadResult();

        partResp->SetMaxOffset(readResult.GetMaxOffset());
        partResp->SetStartOffset(readResult.GetStartOffset());
        partResp->SetEndOffset(readResult.GetEndOffset());
        partResp->SetSizeLag(readResult.GetSizeLag());
        partResp->SetWaitQuotaTimeMs(partResp->GetWaitQuotaTimeMs() + readResult.GetWaitQuotaTimeMs());

        partResp->SetRealReadOffset(Max(partResp->GetRealReadOffset(), readResult.GetRealReadOffset()));

        auto makeErrorResponse = [&] (const TString& errorMessage) {
            partResp->MutableResult()->Clear();
            responseRecord.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
            responseRecord.SetErrorCode(NPersQueue::NErrorCode::READ_NOT_DONE);
            responseRecord.SetErrorReason(errorMessage);
            InitialRequest = false; //So we don't make any more retries but return error;
        };

        auto dropIncompleteLastIfAny = [&] {
            DropIncompleteLastIfAny(partResp);
        };

        for (ui32 i = 0; i < readResult.ResultSize(); ++i) {
            const auto& currentReadResult = readResult.GetResult(i);
            if (currentReadResult.GetData().empty()) { // This is empty parted removed by compactification
                LastSkipOffset = currentReadResult.GetOffset();
                continue; // Skip the empty part;
            }
            if (LastSkipOffset.Defined() && currentReadResult.GetOffset() == *LastSkipOffset) {
                continue; // This is part of the message which is already being skipped due to empty parts or timestamp filtering. Skip all other parts as well;
            }
            if (!InitialRequest) {
                // This is follow-up request to read missing parts;
                // There must be some data in response already.
                if (partResp->ResultSize() == 0) {
                    makeErrorResponse("Internal error - got message part on followup read request with empty current response");
                    YDB_LOG_CRIT("Handle TEvRead got message part on followup read request with empty current response. Readed now full",
                        {"logPrefix", NPQ_LOG_PREFIX},
                        {"seqNo", currentReadResult.GetSeqNo()},
                        {"partNo", currentReadResult.GetPartNo()},
                        {"requestNow", Request});
                    break;
                }
                if (currentReadResult.GetPartNo() == 0) {
                    // Partition gap-jumped to the next message: remaining parts of the previous one
                    // were deleted by retention or compactification. Drop the incomplete tail and
                    // keep assembling from this result — do not resend the same follow-up.
                    dropIncompleteLastIfAny();
                } else {
                    const auto& lastReadResult = partResp->GetResult(partResp->ResultSize() - 1);
                    if (lastReadResult.GetSeqNo() != currentReadResult.GetSeqNo() || lastReadResult.GetPartNo() + 1 != currentReadResult.GetPartNo()) {
                        dropIncompleteLastIfAny();
                        break;
                    }
                }
            }

            // If we already have some data and encounter new message that doesn't fit into current response, we don't go any further, just stop;
            // (And throw away that message to)
            if (partResp->ResultSize() > 1 && currentReadResult.GetPartNo() == 0 &&
                currentReadResult.HasTotalParts() && currentReadResult.GetTotalParts() + i > readResult.ResultSize())
            {
                break;
            }

            // Now actually add some data;
            if (currentReadResult.GetPartNo() == 0) {
                if (partResp->ResultSize()) {
                    const auto& back = partResp->GetResult(partResp->ResultSize() - 1);
                    if (back.GetPartNo() + 1 < back.GetTotalParts()) {
                        makeErrorResponse("Internal error - got message part from the middle when expecting first part");
                        YDB_LOG_CRIT("Handle TEvRead last read pos readed now full",
                            {"logPrefix", NPQ_LOG_PREFIX},
                            {"seqNoPartNo", back.GetSeqNo()},
                            {"partNo", back.GetPartNo()},
                            {"seqNo", currentReadResult.GetSeqNo()},
                            {"currentPartNo", currentReadResult.GetPartNo()},
                            {"requestNow", Request});
                        break;
                    }
                }
                if (currentReadResult.GetWriteTimestampMS() < readFromTimestampMs && skipObsoleteTimestamps) {
                    LastSkipOffset = currentReadResult.GetOffset();
                    continue;
                }
                // Create new message for first part;
                auto* added = partResp->AddResult();
                added->CopyFrom(currentReadResult);
                if (added->GetTotalSize() > added->GetData().size()) {
                    added->MutableData()->reserve(added->GetTotalSize());
                }
            } else { // Glue next part to prevous otherwise
                if(partResp->ResultSize() == 0) {
                    // This is error, Must have some data at this point;
                    YDB_LOG_CRIT("Handle TEvRead, have last read pos, readed now full",
                        {"logPrefix", NPQ_LOG_PREFIX},
                        {"seqNo", currentReadResult.GetSeqNo()},
                        {"partNo", currentReadResult.GetPartNo()},
                        {"requestNow", Request});
                    makeErrorResponse("Internal error - got message part from the middle when current response if empty");
                    break;

                }
                auto* rr = partResp->MutableResult(partResp->ResultSize() - 1);
                if (rr->GetSeqNo() != currentReadResult.GetSeqNo() || rr->GetPartNo() + 1 != currentReadResult.GetPartNo()) {
                    YDB_LOG_CRIT("Handle TEvRead last read pos readed now full",
                        {"logPrefix", NPQ_LOG_PREFIX},
                        {"seqNoPartNo", rr->GetSeqNo()},
                        {"partNo", rr->GetPartNo()},
                        {"seqNo", currentReadResult.GetSeqNo()},
                        {"currentPartNo", currentReadResult.GetPartNo()},
                        {"requestNow", Request});
                    makeErrorResponse("Internal error - got message with wrong SeqNo/PartNo when expecting");
                    break;
                }
                auto* data = rr->MutableData();
                if (rr->GetTotalSize() > data->size()) {
                    data->reserve(rr->GetTotalSize());
                }
                *data += currentReadResult.GetData();
                rr->SetPartitionKey(currentReadResult.GetPartitionKey());
                rr->SetExplicitHash(currentReadResult.GetExplicitHash());
                rr->SetPartNo(currentReadResult.GetPartNo());
                rr->SetUncompressedSize(rr->GetUncompressedSize() + currentReadResult.GetUncompressedSize());
                if (currentReadResult.GetPartNo() + 1 == currentReadResult.GetTotalParts()) {
                    // This is the last part, validate data size;
                    AFL_ENSURE((ui32)rr->GetTotalSize() == (ui32)rr->GetData().size());
                }
            }
        }
        // Nothing left to return: skipped compactified parts on the initial read, or dropped an
        // incomplete tail whose remaining parts are gone. Continue from the next offset instead of
        // repeating the same follow-up.
        if (partResp->GetResult().empty() && LastSkipOffset.Defined()) {
            const auto& cmdRead = Request.GetPartitionRequest().GetCmdRead();
            const bool skippedAheadOnInitial = InitialRequest && (ui64)cmdRead.GetOffset() < *LastSkipOffset;
            const bool droppedIncompleteOnFollowUp = !InitialRequest;
            if (skippedAheadOnInitial || droppedIncompleteOnFollowUp) {
                ContinueFromSkippedOffset();
                return;
            }
        }
        if (!partResp->GetResult().empty()) {
            const auto& lastRes = partResp->GetResult(partResp->GetResult().size() - 1);
            if (lastRes.HasPartNo() && lastRes.GetPartNo() + 1 < lastRes.GetTotalParts()) {
                // Need more data to complete the big message. Send followup read request (and switch to non-initial request state)
                Request.SetRequestId(TMP_REQUEST_MARKER);

                auto read = Request.MutablePartitionRequest()->MutableCmdRead();
                read->SetOffset(lastRes.GetOffset());
                read->SetPartNo(lastRes.GetPartNo() + 1);
                read->SetCount(1);
                read->ClearBytes();
                read->ClearTimeoutMs();
                read->ClearMaxTimeLagMs();
                read->SetReadTimestampMs(readFromTimestampMs);

                THolder<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
                req->Record = Request;
                Send(TabletActorId, req.Release());
                InitialRequest = false;
                return;
            }
        }
        if (readFromTimestampMs == 0) {
            for (const auto& rec : partResp->GetResult()) {
                partResp->SetRealReadOffset(Max(partResp->GetRealReadOffset(), rec.GetOffset()));
            }
        } else {
            ::google::protobuf::RepeatedPtrField<NKikimrClient::TCmdReadResult::TResult> records;
            records.Swap(partResp->MutableResult());
            for (auto& rec : records) {
                partResp->SetRealReadOffset(Max(partResp->GetRealReadOffset(), rec.GetOffset()));
                if (rec.GetWriteTimestampMS() >= readFromTimestampMs) {
                    partResp->AddResult()->Swap(&rec);
                }
            }
        }
        TryProcessBatchOrSendResponse(ctx, isDirectRead, readResult, record.GetPartitionResponse());
    }

    void Handle(NBatching::TEvProcessBatchResult::TPtr& ev, const TActorContext& ctx)
    {
        auto context = std::move(ev->Get()->Context);
        auto* proxyResponse = static_cast<TEvPQ::TEvProxyResponse*>(context.Event.Get());
        AFL_ENSURE(proxyResponse);

        if (PendingDirectRead) {
            PreparedResponse = proxyResponse->Response;
            SendResponse(
                ctx,
                true,
                PreparedResponse->GetPartitionResponse().GetCmdReadResult(),
                PendingPartitionResponse);
            return;
        }

        Response->Record.CopyFrom(*proxyResponse->Response);
        ctx.Send(Sender, Response.Release());
        PassAway();
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, Handle);
            HFunc(NBatching::TEvProcessBatchResult, Handle);
            HFunc(TEvents::TEvPoisonPill, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
        default:
            break;
        };
    }

    const TActorId Sender;
    const ui32 TabletGeneration;
    NKikimrClient::TPersQueueRequest Request;
    THolder<TEvPersQueue::TEvResponse> Response;
    std::shared_ptr<NKikimrClient::TResponse> PreparedResponse;
    TDirectReadKey DirectReadKey;
    const ui64 InitialReadOffset;
    const bool CanReadBatches;
    const TActorId BatchProcessorActor;
    bool InitialRequest = true;
    TMaybe<ui64> LastSkipOffset;
    bool PendingDirectRead = false;
    NKikimrClient::TPersQueuePartitionResponse PendingPartitionResponse;
};


IActor* CreateReadProxy(const TActorId& sender, ui64 tabletId, const TActorId& tablet, ui32 tabletGeneration,
                         const TDirectReadKey& directReadKey, const NKikimrClient::TPersQueueRequest& request,
                         const TActorId& batchProcessorActor)
{
    return new TReadProxy(sender, tabletId, tablet, tabletGeneration, directReadKey, request, batchProcessorActor);
}

}
