#include "partition_writer.h"

namespace NKikimr::NGRpcProxy::V1 {

const ui32 MAX_RESERVE_REQUESTS_INFLIGHT = 5;

void TPartitionWriter::OnEvInitResult(const NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev)
{
    const auto& result = *ev->Get();
    AFL_ENSURE(result.IsSuccess());

    OwnerCookie = result.GetResult().OwnerCookie;
    MaxSeqNo = result.GetResult().SourceIdInfo.GetSeqNo();
}

void TPartitionWriter::OnWriteRequest(THolder<NPQ::TEvPartitionWriter::TEvWriteRequest>&& ev, NWilson::TTraceId traceId,
                                      const TActorContext& ctx)
{
    AFL_ENSURE(ev->Record.HasPartitionRequest());

    if (SentRequests.size() < MAX_RESERVE_REQUESTS_INFLIGHT) {
        SentRequests.emplace_back(ev->Record.GetPartitionRequest().GetCookie());

        ctx.Send(Actor, ev.Release(), 0, 0, std::move(traceId));
    } else {
        QuotedRequests.emplace_back(std::move(ev));
    }
}

void TPartitionWriter::OnWriteAccepted(const NPQ::TEvPartitionWriter::TEvWriteAccepted& ev, const TActorContext& ctx)
{
    AFL_ENSURE(!SentRequests.empty());
    AFL_ENSURE(ev.Cookie == SentRequests.front().Cookie);

    const TSentRequest& front = SentRequests.front();

    AcceptedRequests.emplace_back(front.Cookie);
    SentRequests.pop_front();

    if (QuotedRequests.empty()) {
        return;
    }

    if (SentRequests.size() < MAX_RESERVE_REQUESTS_INFLIGHT) {
        auto next = std::move(QuotedRequests.front());
        QuotedRequests.pop_front();

        SentRequests.emplace_back(next.Write->Record.GetPartitionRequest().GetCookie());

        ctx.Send(Actor, next.Write.Release());
    }
}

void TPartitionWriter::OnWriteResponse(const NPQ::TEvPartitionWriter::TEvWriteResponse& ev)
{
    AFL_ENSURE(ev.IsSuccess());

    AFL_ENSURE(!AcceptedRequests.empty());
    AFL_ENSURE(ev.Record.GetPartitionResponse().GetCookie() == AcceptedRequests.front().Cookie);

    AcceptedRequests.pop_front();
}

bool TPartitionWriter::HasPendingRequests() const
{
    return !QuotedRequests.empty() || !SentRequests.empty() || !AcceptedRequests.empty();
}

}
