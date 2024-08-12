#include "grpc_request_context_wrapper.h"

namespace NKikimr::NPublicHttp {

    TGrpcRequestContextWrapper::TGrpcRequestContextWrapper(const THttpRequestContext& requestContext, std::unique_ptr<NProtoBuf::Message> request, TReplySender replySender)
      : RequestContext(requestContext)
      , LongProject("yandexcloud://" + requestContext.GetProject())  // todo: remove prefix
      , Request(std::move(request))
      , ReplySender(std::move(replySender))
      , AuthState(true)
      , DeadlineAt(TInstant::Max())
    {
        JsonSettings.EnumAsNumbers = false;
        JsonSettings.UI64AsString = false;
        JsonSettings.EmptyRepeated = true;
    }

    const NProtoBuf::Message* TGrpcRequestContextWrapper::GetRequest() const {
        return Request.get();
    }

    NProtoBuf::Message* TGrpcRequestContextWrapper::GetRequestMut() {
        return Request.get();
    }

    NYdbGrpc::TAuthState& TGrpcRequestContextWrapper::GetAuthState() {
        return AuthState;
    }

    void TGrpcRequestContextWrapper::Reply(NProtoBuf::Message* resp, ui32 status) {
        Y_UNUSED(resp);
        Y_UNUSED(status);
        Y_ABORT_UNLESS(resp);
        ReplySender(RequestContext, JsonSettings, resp, status);
    }

    void TGrpcRequestContextWrapper::Reply(grpc::ByteBuffer* resp, ui32 status, EStreamCtrl ctrl) {
        Y_UNUSED(resp);
        Y_UNUSED(status);
        Y_UNUSED(ctrl);
        Y_ABORT_UNLESS(false, "TGrpcRequestContextWrapper::Reply");
    }

    void TGrpcRequestContextWrapper::ReplyUnauthenticated(const TString& in) {
        RequestContext.ResponseUnauthenticated(in);
    }

    void TGrpcRequestContextWrapper::ReplyError(grpc::StatusCode code, const TString& msg, const TString& details) {
        auto errorMsg = TStringBuilder() << "Unexpected error. code: " << (int)code << ", msg: " << msg << ", details: " << details;
        RequestContext.ResponseBadRequest(Ydb::StatusIds::BAD_REQUEST, errorMsg);
    }

    TInstant TGrpcRequestContextWrapper::Deadline() const {
        return DeadlineAt;
    }

    TSet<TStringBuf> TGrpcRequestContextWrapper::GetPeerMetaKeys() const {
        return {};
    }

    TVector<TStringBuf> TGrpcRequestContextWrapper::GetPeerMetaValues(TStringBuf key) const {
        if (key == "x-ydb-database"sv) {
            return { RequestContext.GetDb() };
        }

        if (key == "x-ydb-fq-project"sv) {
            return { LongProject };
        }

        if (key == "x-ydb-auth-ticket"sv) {
            return { RequestContext.GetToken() };
        }

        return { };
    }

    google::protobuf::Arena* TGrpcRequestContextWrapper::GetArena() {
        return &Arena;
    }

    TString TGrpcRequestContextWrapper::GetPeer() const {
       return RequestContext.GetPeer();
    }

} // namespace NKikimr::NPublicHttp
