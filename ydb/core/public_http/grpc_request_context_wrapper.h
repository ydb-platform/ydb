#pragma once

#include "http_req.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NKikimr::NPublicHttp {

typedef std::function<void(const THttpRequestContext& requestContext, const TJsonSettings& jsonSettings, NProtoBuf::Message* resp, ui32 status)> TReplySender;

class TGrpcRequestContextWrapper : public NYdbGrpc::IRequestContextBase {
private:
    THttpRequestContext RequestContext;
    TString LongProject;
    std::unique_ptr<NProtoBuf::Message> Request;
    TReplySender ReplySender;
    NYdbGrpc::TAuthState AuthState;
    google::protobuf::Arena Arena;
    TJsonSettings JsonSettings;
    TInstant DeadlineAt;

public:
    TGrpcRequestContextWrapper(const THttpRequestContext& requestContext, std::unique_ptr<NProtoBuf::Message> request, TReplySender replySender);
    virtual const NProtoBuf::Message* GetRequest() const;
    virtual NProtoBuf::Message* GetRequestMut();
    virtual NYdbGrpc::TAuthState& GetAuthState();
    virtual void Reply(NProtoBuf::Message* resp, ui32 status = 0);
    virtual void Reply(grpc::ByteBuffer* resp, ui32 status = 0, EStreamCtrl ctrl = EStreamCtrl::CONT);
    virtual void ReplyUnauthenticated(const TString& in);
    virtual void ReplyError(grpc::StatusCode code, const TString& msg, const TString& details);
    virtual TInstant Deadline() const;
    virtual TSet<TStringBuf> GetPeerMetaKeys() const;
    virtual TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const;
    virtual TVector<TStringBuf> FindClientCert() const {return {};}
    virtual grpc_compression_level GetCompressionLevel() const { return GRPC_COMPRESS_LEVEL_NONE; }

    virtual google::protobuf::Arena* GetArena();

    virtual void AddTrailingMetadata(const TString&, const TString&) {}

    virtual void UseDatabase(const TString& ) {}

    virtual void SetNextReplyCallback(TOnNextReply&&) {}
    virtual void FinishStreamingOk() {}
    virtual TAsyncFinishResult GetFinishFuture() { return {}; }
    virtual bool IsClientLost() const { return false; }
    virtual TString GetPeer() const;
    virtual bool SslServer() const { return false; }
    virtual bool IsStreamCall() const { return false; }
};

} // namespace NKikimr::NPublicHttp

