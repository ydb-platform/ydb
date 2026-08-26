#include "partition_writer.h"

namespace NKikimr::NPQ {

const ui32 MAX_RESERVE_REQUESTS_INFLIGHT = 5;

void TCachedPartitionWriter::OnEvInitResult(const TEvPartitionWriter::TEvInitResult::TPtr& ev)
{
    const auto& result = *ev->Get();
    AFL_ENSURE(result.IsSuccess());

    OwnerCookie = result.GetResult().OwnerCookie;
    MaxSeqNo = result.GetResult().SourceIdInfo.GetSeqNo();
}

void TCachedPartitionWriter::OnWriteRequest(THolder<TEvPartitionWriter::TEvWriteRequest>&& ev, NWilson::TTraceId traceId,
                                            const TActorContext& ctx)
{
    AFL_ENSURE(ev->Record.HasPartitionRequest());

    if (SentRequests.size() < MAX_RESERVE_REQUESTS_INFLIGHT) {
        SentRequests.emplace_back(ev->Record.GetPartitionRequest().GetCookie());

        ctx.Send(Actor, ev.Release(), 0, 0, std::move(traceId));
    } else {
        QuotedRequests.push_back(TUserWriteRequest{
            .Write = std::move(ev),
            .TraceId = std::move(traceId),
        });
    }
}

void TCachedPartitionWriter::OnWriteAccepted(const TEvPartitionWriter::TEvWriteAccepted& ev, const TActorContext& ctx)
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

        ctx.Send(Actor, next.Write.Release(), 0, 0, std::move(next.TraceId));
    }
}

void TCachedPartitionWriter::OnWriteResponse(const TEvPartitionWriter::TEvWriteResponse& ev)
{
    AFL_ENSURE(ev.IsSuccess());

    AFL_ENSURE(!AcceptedRequests.empty());
    AFL_ENSURE(ev.Record.GetPartitionResponse().GetCookie() == AcceptedRequests.front().Cookie);

    AcceptedRequests.pop_front();
}

bool TCachedPartitionWriter::HasPendingRequests() const
{
    return !QuotedRequests.empty() || !SentRequests.empty() || !AcceptedRequests.empty();
}

} // namespace NKikimr::NPQ
