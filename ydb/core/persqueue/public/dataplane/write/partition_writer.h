#pragma once

#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr::NPQ {

struct TCachedPartitionWriter {
    TCachedPartitionWriter() = default;

    void OnEvInitResult(const TEvPartitionWriter::TEvInitResult::TPtr& ev);
    void OnWriteRequest(THolder<TEvPartitionWriter::TEvWriteRequest>&& ev, NWilson::TTraceId traceId, const TActorContext& ctx);
    void OnWriteAccepted(const TEvPartitionWriter::TEvWriteAccepted& ev, const TActorContext& ctx);
    void OnWriteResponse(const TEvPartitionWriter::TEvWriteResponse& ev);

    bool HasPendingRequests() const;
    ui64 FrontPendingCookie() const;

    TActorId Actor;
    TString OwnerCookie;
    ui64 MaxSeqNo = 0;
    TInstant LastActivity;
    bool InitErrorReported = false;

    struct TUserWriteRequest {
        THolder<TEvPartitionWriter::TEvWriteRequest> Write;
        NWilson::TTraceId TraceId;
    };

    struct TSentRequest {
        ui64 Cookie;
    };

    // Quoted, but not sent requests
    TDeque<TUserWriteRequest> QuotedRequests;
    // Requests that is sent to partition actor, but not accepted
    TDeque<TSentRequest> SentRequests;
    // Accepted requests
    TDeque<TSentRequest> AcceptedRequests;
};

} // namespace NKikimr::NPQ
