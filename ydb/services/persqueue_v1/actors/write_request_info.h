#pragma once
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NGRpcProxy::V1 {

template<class TEvWrite>
struct TWriteRequestInfoImpl : public TSimpleRefCount<TWriteRequestInfoImpl<TEvWrite>> {
    using TPtr = TIntrusivePtr<TWriteRequestInfoImpl<TEvWrite>>;

    struct TUserWriteRequest {
        THolder<TEvWrite> Write;
    };

    explicit TWriteRequestInfoImpl(ui64 cookie, NWilson::TSpan span)
        : PartitionWriteRequest(new NPQ::TEvPartitionWriter::TEvWriteRequest(cookie))
        , Cookie(cookie)
        , ByteSize(0)
        , RequiredQuota(0)
        , Span(std::move(span))
    {
    }

    void StartQuotaSpan() {
        QuotaSpan = NWilson::TSpan(TWilsonTopic::TopicDetailed, Span.GetTraceId(), "RequestQuota");
    }

    void SetSpanParamRequestedQuota() {
        QuotaSpan.Attribute("quota", static_cast<i64>(RequiredQuota));
    }

    std::pair<TString, TString> GetTransactionId() const;

    // Source requests from user (grpc session object)
    std::deque<TUserWriteRequest> UserWriteRequests;

    // Partition write request
    THolder<NPQ::TEvPartitionWriter::TEvWriteRequest> PartitionWriteRequest;

    // Formed write request's cookie
    ui64 Cookie;

    // Formed write request's size
    ui64 ByteSize;

    // Quota in term of RUs
    ui64 RequiredQuota;

    NWilson::TSpan QuotaSpan;
    NWilson::TSpan Span;
};

template<class TEvWrite>
std::pair<TString, TString> TWriteRequestInfoImpl<TEvWrite>::GetTransactionId() const
{
    Y_ABORT_UNLESS(!UserWriteRequests.empty());

    static constexpr bool UseMigrationProtocol = !std::is_same_v<TEvWrite, TEvPQProxy::TEvTopicWrite>;

    if constexpr (UseMigrationProtocol) {
        return {"", ""};
    } else {
        auto& request = UserWriteRequests.front().Write->Request.write_request();
        return {request.tx().session(), request.tx().id()};
    }
}

}
