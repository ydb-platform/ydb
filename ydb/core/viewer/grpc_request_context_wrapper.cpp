#include "grpc_request_context_wrapper.h"
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

    TGrpcRequestContextWrapper::TGrpcRequestContextWrapper(TActorSystem* actorSystem, IViewer* viewer, const NMon::TEvHttpInfo::TPtr& event, std::unique_ptr<NProtoBuf::Message> request, TReplySender replySender)
      : ActorSystem(actorSystem)
      , Viewer(viewer)
      , Event(event)
      , Request(std::move(request))
      , ReplySender(std::move(replySender))
      , AuthState(true)
      , DeadlineAt(TInstant::Max())
    {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = false;
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        JsonSettings.EmptyRepeated = true;
        const TString& timeout = params.Get("timeout");
        if (timeout) {
            DeadlineAt = TInstant::Now() + TDuration::MilliSeconds(FromStringWithDefault<ui32>(timeout, 10000));
        }
        Y_UNUSED(ActorSystem);
        Y_UNUSED(Viewer);
        Y_VERIFY(ActorSystem);
        Y_VERIFY(Event);
    }

    const NProtoBuf::Message* TGrpcRequestContextWrapper::GetRequest() const {
        return Request.get();
    }

    NGrpc::TAuthState& TGrpcRequestContextWrapper::GetAuthState() {
        return AuthState;
    }

    void TGrpcRequestContextWrapper::Reply(NProtoBuf::Message* resp, ui32 status) {
        Y_UNUSED(resp);
        Y_UNUSED(status);
        Y_VERIFY(resp);
        ReplySender(ActorSystem, Viewer, Event, JsonSettings, resp, status);
    }

    void TGrpcRequestContextWrapper::Reply(grpc::ByteBuffer* resp, ui32 status) {
        Y_UNUSED(resp);
        Y_UNUSED(status);
        Y_VERIFY(false, "TGrpcRequestContextWrapper::Reply");
    }

    void TGrpcRequestContextWrapper::ReplyUnauthenticated(const TString& in) {
        Y_UNUSED(in);
        ActorSystem->Send(Event->Sender, new NMon::TEvHttpInfoRes(HTTPUNAUTHORIZEDTEXT + in, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    void TGrpcRequestContextWrapper::ReplyError(grpc::StatusCode code, const TString& msg, const TString& details) {
        Y_UNUSED(code);
        Y_UNUSED(msg);
        ActorSystem->Send(Event->Sender, new NMon::TEvHttpInfoRes(TStringBuilder() << HTTPBADREQUEST_HEADERS << "code: " << (int)code << ", msg: " << msg << ", details: " << details, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    TInstant TGrpcRequestContextWrapper::Deadline() const {
        return DeadlineAt;
    }

    TSet<TStringBuf> TGrpcRequestContextWrapper::GetPeerMetaKeys() const {
        return {};
    }

    TVector<TStringBuf> TGrpcRequestContextWrapper::GetPeerMetaValues(TStringBuf key) const {
        // todo: remap http public headers into internal grpc headers, e.g
        // Authorization -> x-ydb-auth-ticket
        // scope/project
        if (key == "x-ydb-auth-ticket"sv) {
            key = "authorization"sv;
        }
        const THttpHeaders& headers = Event->Get()->Request.GetHeaders();
        if (auto h = headers.FindHeader(key)) {
            return { h->Value() };
        }
        return {};
    }

    google::protobuf::Arena* TGrpcRequestContextWrapper::GetArena() {
        return &Arena;
    }
}
}
