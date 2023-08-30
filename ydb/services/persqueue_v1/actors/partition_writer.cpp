namespace NKikimr::NGRpcProxy::V1 {

const ui32 MAX_RESERVE_REQUESTS_INFLIGHT = 5;

template<class TEvWrite>
TPartitionWriterImpl<TEvWrite>::TPartitionWriterImpl(NKikimr::NPQ::TMultiCounter& bytesInflight,
                                                     NKikimr::NPQ::TMultiCounter& bytesInflightTotal,
                                                     ui64& bytesInflight_,
                                                     ui64& bytesInflightTotal_) :
    BytesInflight(bytesInflight),
    BytesInflightTotal(bytesInflightTotal),
    BytesInflight_(bytesInflight_),
    BytesInflightTotal_(bytesInflightTotal_)
{
}

template<class TEvWrite>
void TPartitionWriterImpl<TEvWrite>::OnEvInitResult(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev)
{
    const auto& result = *ev->Get();
    Y_VERIFY(result.IsSuccess());
    OwnerCookie = result.GetResult().OwnerCookie;
}

template<class TEvWrite>
ui64 TPartitionWriterImpl<TEvWrite>::OnEvWriteAccepted(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev)
{
    Y_VERIFY(!SentRequests.empty());

    auto request = std::move(SentRequests.front());
    Y_VERIFY(ev->Get()->Cookie == request->Cookie);

    SentRequests.pop_front();

    ui64 size = request->ByteSize;

    AcceptedRequests.emplace_back(std::move(request));

    return size;
}

template<class TEvWrite>
auto TPartitionWriterImpl<TEvWrite>::OnEvWriteResponse(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev) -> TWriteRequestInfoPtr
{
    Y_VERIFY(!AcceptedRequests.empty());

    auto request = std::move(AcceptedRequests.front());
    AcceptedRequests.pop_front();

    const auto& resp = ev->Get()->Record.GetPartitionResponse();
    Y_VERIFY(resp.GetCookie() == request->Cookie);

    return request;
}

template<class TEvWrite>
bool TPartitionWriterImpl<TEvWrite>::AnyRequests() const
{
    return !QuotedRequests.empty() || !SentRequests.empty() || !AcceptedRequests.empty();
}

template<class TEvWrite>
void TPartitionWriterImpl<TEvWrite>::AddWriteRequest(TWriteRequestInfoPtr request, const TActorContext& ctx)
{
    if (!QuotedRequests.empty() && SentRequests.size() < MAX_RESERVE_REQUESTS_INFLIGHT) {
        SendRequest(std::move(request), ctx);
    } else {
        QuotedRequests.emplace_back(std::move(request));
    }
}

template<class TEvWrite>
bool TPartitionWriterImpl<TEvWrite>::TrySendNextQuotedRequest(const TActorContext& ctx)
{
    if (QuotedRequests.empty()) {
        return false;
    }

    SendRequest(std::move(QuotedRequests.front()), ctx);
    QuotedRequests.pop_front();

    return true;
}

template<class TEvWrite>
void TPartitionWriterImpl<TEvWrite>::SendRequest(TWriteRequestInfoPtr request, const TActorContext& ctx)
{
    Y_VERIFY(request->PartitionWriteRequest);

    i64 diff = 0;
    for (const auto& w : request->UserWriteRequests) {
        diff -= w->Request.ByteSize();
    }

    Y_VERIFY(-diff <= (i64)BytesInflight_);
    diff += request->PartitionWriteRequest->Record.ByteSize();
    BytesInflight_ += diff;
    BytesInflightTotal_ += diff;

    if (BytesInflight && BytesInflightTotal) {
        BytesInflight.Inc(diff);
        BytesInflightTotal.Inc(diff);
    }

    ctx.Send(PartitionWriterActor, std::move(request->PartitionWriteRequest));
    SentRequests.emplace_back(std::move(request));
}

}
