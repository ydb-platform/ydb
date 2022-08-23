#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/public/api/protos/yq.pb.h>

namespace NKikimr {
namespace NViewer {

class IViewer;

typedef std::function<void(TActorSystem* actorSystem, IViewer* viewer, const NMon::TEvHttpInfo::TPtr& event, const TJsonSettings& jsonSettings, NProtoBuf::Message* resp, ui32 status)> TReplySender;

class TGrpcRequestContextWrapper : public NGrpc::IRequestContextBase {
private:
    TActorSystem* ActorSystem;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    std::unique_ptr<NProtoBuf::Message> Request;
    TReplySender ReplySender;
    NGrpc::TAuthState AuthState;
    google::protobuf::Arena Arena;
    TJsonSettings JsonSettings;
    TInstant DeadlineAt;

public:
    TGrpcRequestContextWrapper(TActorSystem* actorSystem, IViewer* viewer, const NMon::TEvHttpInfo::TPtr& event, std::unique_ptr<NProtoBuf::Message> request, TReplySender replySender);
    virtual const NProtoBuf::Message* GetRequest() const;
    virtual NGrpc::TAuthState& GetAuthState();
    virtual void Reply(NProtoBuf::Message* resp, ui32 status = 0);
    virtual void Reply(grpc::ByteBuffer* resp, ui32 status = 0);
    virtual void ReplyUnauthenticated(const TString& in);
    virtual void ReplyError(grpc::StatusCode code, const TString& msg, const TString& details);
    virtual TInstant Deadline() const;
    virtual TSet<TStringBuf> GetPeerMetaKeys() const;
    virtual TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const;
    virtual grpc_compression_level GetCompressionLevel() const { return GRPC_COMPRESS_LEVEL_NONE; }

    virtual google::protobuf::Arena* GetArena();

    virtual void AddTrailingMetadata(const TString&, const TString&) {}

    virtual void UseDatabase(const TString& ) {}

    virtual void SetNextReplyCallback(TOnNextReply&&) {}
    virtual void FinishStreamingOk() {}
    virtual TAsyncFinishResult GetFinishFuture() { return {}; }
    virtual TString GetPeer() const { return {}; }
    virtual bool SslServer() const { return false; }
};

}
}
