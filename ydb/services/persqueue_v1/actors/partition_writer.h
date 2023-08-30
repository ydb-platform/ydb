#pragma once

#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/write_request_info.h>

namespace NKikimr::NGRpcProxy::V1 {

template<class TEvWrite>
struct TPartitionWriterImpl {
    using TWriteRequestInfoPtr = typename TWriteRequestInfoImpl<TEvWrite>::TPtr;

    TPartitionWriterImpl(NKikimr::NPQ::TMultiCounter& bytesInflight,
                         NKikimr::NPQ::TMultiCounter& bytesInflightTotal,
                         ui64& bytesInflight_,
                         ui64& bytesInflightTotal_);

    //
    // from client
    //
    void OnEvWrite(TEvPQProxy::TEvTopicWrite::TPtr& ev, const TActorContext& ctx);

    //
    // from partition writer actor
    //
    void OnEvInitResult(NPQ::TEvPartitionWriter::TEvInitResult::TPtr& ev);
    ui64 OnEvWriteAccepted(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr& ev);
    TWriteRequestInfoPtr OnEvWriteResponse(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr& ev);

    //
    // from quoter
    //
    void OnEvWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    bool AnyRequests() const;
    void AddWriteRequest(TWriteRequestInfoPtr request, const TActorContext& ctx);
    bool TrySendNextQuotedRequest(const TActorContext& ctx);
    void SendRequest(TWriteRequestInfoPtr request, const TActorContext& ctx);

    TActorId PartitionWriterActor;
    TString OwnerCookie;

    // Quoted, but not sent requests
    TDeque<TWriteRequestInfoPtr> QuotedRequests;
    // Requests that is sent to partition actor, but not accepted
    TDeque<TWriteRequestInfoPtr> SentRequests;
    // Accepted requests
    TDeque<TWriteRequestInfoPtr> AcceptedRequests;

    NKikimr::NPQ::TMultiCounter& BytesInflight;
    NKikimr::NPQ::TMultiCounter& BytesInflightTotal;

    ui64& BytesInflight_;
    ui64& BytesInflightTotal_;
};

}

#include "partition_writer.cpp"
