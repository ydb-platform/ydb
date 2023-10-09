#pragma once

namespace NKikimr::NGRpcProxy::V1 {

template<class TEvWrite>
struct TWriteRequestInfoImpl : public TSimpleRefCount<TWriteRequestInfoImpl<TEvWrite>> {
    using TPtr = TIntrusivePtr<TWriteRequestInfoImpl<TEvWrite>>;

    explicit TWriteRequestInfoImpl(ui64 cookie)
        : PartitionWriteRequest(new NPQ::TEvPartitionWriter::TEvWriteRequest(cookie))
        , Cookie(cookie)
        , ByteSize(0)
        , RequiredQuota(0)
    {
    }

    std::pair<TString, TString> GetTransactionId() const;

    // Source requests from user (grpc session object)
    std::deque<THolder<TEvWrite>> UserWriteRequests;

    // Partition write request
    THolder<NPQ::TEvPartitionWriter::TEvWriteRequest> PartitionWriteRequest;

    // Formed write request's cookie
    ui64 Cookie;

    // Formed write request's size
    ui64 ByteSize;

    // Quota in term of RUs
    ui64 RequiredQuota;
};

template<class TEvWrite>
std::pair<TString, TString> TWriteRequestInfoImpl<TEvWrite>::GetTransactionId() const
{
    Y_ABORT_UNLESS(!UserWriteRequests.empty());

    static constexpr bool UseMigrationProtocol = !std::is_same_v<TEvWrite, TEvPQProxy::TEvTopicWrite>;

    if constexpr (UseMigrationProtocol) {
        return {"", ""};
    } else {
        auto& request = UserWriteRequests.front()->Request.write_request();
        return {request.tx().session(), request.tx().id()};
    }
}

}
