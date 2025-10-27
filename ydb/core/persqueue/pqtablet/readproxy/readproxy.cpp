#include "readproxy.h"

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/dread_cache_service/caching_service.h>
#include <ydb/core/persqueue/pqtablet/common/constants.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

namespace NKikimr::NPQ {

using namespace NActors;

class TReadProxy : public TBaseTabletActor<TReadProxy>, private TConstantLogPrefix {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_ANS_ACTOR;
    }

    TReadProxy(const TActorId& sender, const ui64 tabletId, const TActorId& tablet, ui64 tabletGeneration,
               const TDirectReadKey& directReadKey, const NKikimrClient::TPersQueueRequest& request)
        : TBaseTabletActor(tabletId, tablet, NKikimrServices::PERSQUEUE)
        , Sender(sender)
        , TabletGeneration(tabletGeneration)
        , Request(request)
        , Response(new TEvPersQueue::TEvResponse)
        , DirectReadKey(directReadKey)
    {
        AFL_ENSURE(Request.HasPartitionRequest() && Request.GetPartitionRequest().HasCmdRead());
        AFL_ENSURE(Request.GetPartitionRequest().GetCmdRead().GetPartNo() == 0); //partial request are not allowed, otherwise remove ReadProxy
        AFL_ENSURE(!Response->Record.HasPartitionResponse());
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

            Response->Record.CopyFrom(record);
            ctx.Send(Sender, Response.Release());
            Die(ctx);
            return;
        }
        AFL_ENSURE(record.HasPartitionResponse() && record.GetPartitionResponse().HasCmdReadResult());
        const auto& readResult = record.GetPartitionResponse().GetCmdReadResult();
        if (isDirectRead) {
            if (!PreparedResponse) {
                PreparedResponse = std::make_shared<NKikimrClient::TResponse>();
            }
        }

        auto& responseRecord = isDirectRead ? *PreparedResponse : Response->Record;
        responseRecord.SetStatus(NMsgBusProxy::MSTATUS_OK);
        responseRecord.SetErrorCode(NPersQueue::NErrorCode::OK);

        AFL_ENSURE(readResult.ResultSize() > 0 || isDirectRead);

        ui64 readFromTimestampMs = PreciseReadFromTimestampBehaviourEnabled(*AppData(ctx))
                                   ? (responseRecord.HasPartitionResponse()
                                        ? responseRecord.GetPartitionResponse().GetCmdReadResult().GetReadFromTimestampMs()
                                        : readResult.GetReadFromTimestampMs())
                                   : 0;

        if (!responseRecord.HasPartitionResponse()) {
            auto partResp = responseRecord.MutablePartitionResponse();
            auto readRes = partResp->MutableCmdReadResult();
            readRes->SetBlobsFromDisk(readRes->GetBlobsFromDisk() + readResult.GetBlobsFromDisk());
            readRes->SetBlobsFromCache(readRes->GetBlobsFromCache() + readResult.GetBlobsFromCache());
            if (AppData(ctx)->FeatureFlags.GetEnableSkipMessagesWithObsoleteTimestamp()) {
                readRes->SetReadFromTimestampMs(readFromTimestampMs);
            }
        }
        if (record.GetPartitionResponse().HasCookie())
            responseRecord.MutablePartitionResponse()->SetCookie(record.GetPartitionResponse().GetCookie());

        auto partResp = responseRecord.MutablePartitionResponse()->MutableCmdReadResult();

        partResp->SetMaxOffset(readResult.GetMaxOffset());
        partResp->SetStartOffset(readResult.GetStartOffset());
        partResp->SetEndOffset(readResult.GetEndOffset());
        partResp->SetSizeLag(readResult.GetSizeLag());
        partResp->SetWaitQuotaTimeMs(partResp->GetWaitQuotaTimeMs() + readResult.GetWaitQuotaTimeMs());

        partResp->SetRealReadOffset(Max(partResp->GetRealReadOffset(), readResult.GetRealReadOffset()));

        auto removeIncompleteMessageIfAny = [&] () {
            if (partResp->ResultSize() == 0)
                return;
            auto& back = partResp->GetResult(partResp->ResultSize() - 1);
            if (back.GetPartNo() + 1 < back.GetTotalParts()) {
                partResp->MutableResult()->RemoveLast();
            }
        };

        auto makeErrorResponse = [&] (const TString& errorMessage) {
            partResp->MutableResult()->Clear();
            responseRecord.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
            responseRecord.SetErrorCode(NPersQueue::NErrorCode::READ_NOT_DONE);
            responseRecord.SetErrorReason(errorMessage);
            InitialRequest = false; //So we don't make any more retries but return error;
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
                    LOG_C("Handle TEvRead got message part on followup read request with empty current response. Readed now "
                                << currentReadResult.GetSeqNo() << ", " << currentReadResult.GetPartNo()
                                << " full request(now): " << Request);
                    break;
                }
                if (currentReadResult.GetPartNo() == 0) {
                    // This is new message. If we still have another incomplete message stored previously, its' last parts were probably deleted by retention of compactification.
                    // This is fine, we can drop last message;
                    break;
                }
                const auto& lastReadResult = partResp->GetResult(partResp->ResultSize() - 1);
                if (lastReadResult.GetSeqNo() != currentReadResult.GetSeqNo() || lastReadResult.GetPartNo() + 1 != currentReadResult.GetPartNo()) {
                    break;
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
                        LOG_C("Handle TEvRead last read pos (seqno/parno): " << back.GetSeqNo() << "," << back.GetPartNo() << " readed now "
                                    << currentReadResult.GetSeqNo() << ", " << currentReadResult.GetPartNo()
                                    << " full request(now): " << Request);
                        break;
                    }
                }
                if (currentReadResult.GetWriteTimestampMS() < readFromTimestampMs && AppData(ctx)->FeatureFlags.GetEnableSkipMessagesWithObsoleteTimestamp()) {
                    LastSkipOffset = currentReadResult.GetOffset();
                    continue;
                }
                // Create new message for first part;
                partResp->AddResult()->CopyFrom(currentReadResult);
            } else { // Glue next part to prevous otherwise
                if(partResp->ResultSize() == 0) {
                    // This is error, Must have some data at this point;
                    LOG_C("Handle TEvRead, have last read pos, readed now "
                                    << currentReadResult.GetSeqNo() << ", " << currentReadResult.GetPartNo()
                                    << " full request(now): " << Request);
                    makeErrorResponse("Internal error - got message part from the middle when current response if empty");
                    break;

                }
                auto* rr = partResp->MutableResult(partResp->ResultSize() - 1);
                if (rr->GetSeqNo() != currentReadResult.GetSeqNo() || rr->GetPartNo() + 1 != currentReadResult.GetPartNo()) {
                    LOG_C("Handle TEvRead last read pos (seqno/parno): " << rr->GetSeqNo() << "," << rr->GetPartNo() << " readed now "
                                    << currentReadResult.GetSeqNo() << ", " << currentReadResult.GetPartNo()
                                    << " full request(now): " << Request);
                    makeErrorResponse("Internal error - got message with wrong SeqNo/PartNo when expecting");
                    break;
                }
                AFL_ENSURE(rr->GetSeqNo() == currentReadResult.GetSeqNo());
                (*rr->MutableData()) += currentReadResult.GetData();
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
        // We got no data during initial request - possibly skipped all the data due to compactification.
        if (InitialRequest && partResp->GetResult().empty() && LastSkipOffset.Defined()
            // Check if we did actually skip anything so we don't request the same offset again.
            && (ui64)Request.GetPartitionRequest().GetCmdRead().GetOffset() < *LastSkipOffset
        ) {
            //Try another read. Set TMP_MARKER so that response is redirected to proxy, but here we will treat is as "initial" response still.
            Request.SetRequestId(TMP_REQUEST_MARKER);
            Request.MutablePartitionRequest()->MutableCmdRead()->SetOffset(*LastSkipOffset + 1);
            Request.MutablePartitionRequest()->MutableCmdRead()->SetPartNo(0);
            THolder<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
            req->Record = Request;
            Send(TabletActorId, req.Release());
            return;
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
        removeIncompleteMessageIfAny();
        //filter old messages
        ::google::protobuf::RepeatedPtrField<NKikimrClient::TCmdReadResult::TResult> records;
        records.Swap(partResp->MutableResult());
        partResp->ClearResult();
        for (auto & rec : records) {
            partResp->SetRealReadOffset(Max(partResp->GetRealReadOffset(), rec.GetOffset()));
            if (rec.GetWriteTimestampMS() >= readFromTimestampMs) {
                auto result = partResp->AddResult();
                result->CopyFrom(rec);
            }
        }
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
            Response->Record.MutablePartitionResponse()->SetCookie(record.GetPartitionResponse().GetCookie());
            if (readResult.ResultSize()) {
                prepareResponse->SetWriteTimestampMS(readResult.GetResult(readResult.ResultSize() - 1).GetWriteTimestampMS());
            }
            Response->Record.SetStatus(responseRecord.GetStatus());
            Response->Record.SetErrorCode(responseRecord.GetErrorCode());
            ctx.Send(
                MakePQDReadCacheServiceActorId(),
                new TEvPQ::TEvStageDirectReadData(DirectReadKey, TabletGeneration, PreparedResponse)
            );
            ctx.Send(Sender, Response.Release());
        } else {
            ctx.Send(Sender, Response.Release());
        }
        Die(ctx);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, Handle);
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
    bool InitialRequest = true;
    TMaybe<ui64> LastSkipOffset;
};


IActor* CreateReadProxy(const TActorId& sender, ui64 tabletId, const TActorId& tablet, ui32 tabletGeneration,
                         const TDirectReadKey& directReadKey, const NKikimrClient::TPersQueueRequest& request)
{
    return new TReadProxy(sender, tabletId, tablet, tabletGeneration, directReadKey, request);
}

}
