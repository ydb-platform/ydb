#pragma once

#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/write_request_info.h>

namespace NKikimr::NGRpcProxy::V1 {

struct TPartitionWriter {
    TPartitionWriter() = default;

    void OnEvInitResult(const NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev);
    void OnWriteRequest(THolder<NPQ::TEvPartitionWriter::TEvWriteRequest>&& ev, const TActorContext& ctx);
    void OnWriteAccepted(const NPQ::TEvPartitionWriter::TEvWriteAccepted& ev, const TActorContext& ctx);
    void OnWriteResponse(const NPQ::TEvPartitionWriter::TEvWriteResponse& ev);

    bool HasPendingRequests() const;

    TActorId Actor;
    TString OwnerCookie;
    ui64 MaxSeqNo = 0;
    TInstant LastActivity;

    // Quoted, but not sent requests
    TDeque<THolder<NPQ::TEvPartitionWriter::TEvWriteRequest>> QuotedRequests;
    // Requests that is sent to partition actor, but not accepted
    TDeque<ui64> SentRequests;
    // Accepted requests
    TDeque<ui64> AcceptedRequests;
};

}
