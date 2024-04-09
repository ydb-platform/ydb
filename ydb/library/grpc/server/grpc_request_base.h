#pragma once

#include <google/protobuf/message.h>
#include <library/cpp/threading/future/future.h>

#include <grpc++/server_context.h>

namespace grpc {
class ByteBuffer;
}

namespace NYdbGrpc {

extern const char* GRPC_USER_AGENT_HEADER;

struct TAuthState {
    enum EAuthState {
        AS_NOT_PERFORMED,
        AS_OK,
        AS_FAIL,
        AS_UNAVAILABLE
    };
    TAuthState(bool needAuth)
        : NeedAuth(needAuth)
        , State(AS_NOT_PERFORMED)
    {}
    bool NeedAuth;
    EAuthState State;
};


//! An interface that may be used to limit concurrency of requests
class IGRpcRequestLimiter: public TThrRefBase {
public:
    virtual bool IncRequest() = 0;
    virtual void DecRequest() = 0;
};

using IGRpcRequestLimiterPtr = TIntrusivePtr<IGRpcRequestLimiter>;

//! State of current request
class IRequestContextBase: public TThrRefBase {
public:
    enum class EFinishStatus {
        OK,
        ERROR,
        CANCEL
    };
    enum class EStreamCtrl {
        CONT = 0,   // Continue stream
        FINISH = 1, // Finish stream just after this reply
    };

    using TAsyncFinishResult = NThreading::TFuture<EFinishStatus>;

    using TOnNextReply = std::function<void (size_t left)>;

    //! Get pointer to the request's message.
    virtual const NProtoBuf::Message* GetRequest() const = 0;

    //! Get mutable pointer to the request's message.
    virtual NProtoBuf::Message* GetRequestMut() = 0;

    //! Get current auth state
    virtual TAuthState& GetAuthState() = 0;

    //! Send common response (The request shoult be created for protobuf response type)
    //! Implementation can swap protobuf message
    virtual void Reply(NProtoBuf::Message* resp, ui32 status = 0) = 0;

    //! Send serialised response (The request shoult be created for bytes response type)
    //! Implementation can swap ByteBuffer

    //! ctrl - controll stream behaviour. Ignored in case of unary call
    virtual void Reply(grpc::ByteBuffer* resp, ui32 status = 0, EStreamCtrl ctrl = EStreamCtrl::CONT) = 0;

    //! Send grpc UNAUTHENTICATED status
    virtual void ReplyUnauthenticated(const TString& in) = 0;

    //! Send grpc error
    virtual void ReplyError(grpc::StatusCode code, const TString& msg, const TString& details = "") = 0;

    //! Returns deadline (server epoch related) if peer set it on its side, or Instanse::Max() otherwise
    virtual TInstant Deadline() const = 0;

    //! Returns available peer metadata keys
    virtual TSet<TStringBuf> GetPeerMetaKeys() const = 0;

    //! Returns peer optional metavalue
    virtual TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const = 0;

    virtual TVector<TStringBuf> FindClientCert() const = 0;

    //! Returns request compression level
    virtual grpc_compression_level GetCompressionLevel() const = 0;

    //! Returns protobuf arena allocator associated with current request
    //! Lifetime of the arena is lifetime of the context
    virtual google::protobuf::Arena* GetArena() = 0;

    //! Add trailing metadata in to grpc context
    //! The metadata will be send at the time of rpc finish
    virtual void AddTrailingMetadata(const TString& key, const TString& value) = 0;

    //! Use validated database name for counters
    virtual void UseDatabase(const TString& database) = 0;

    // Streaming part

    //! Set callback. The callback will be called when response deliverid to the client
    //! after that we can call Reply again in streaming mode. Yes, GRpc says there is only one
    //! reply in flight
    virtual void SetNextReplyCallback(TOnNextReply&& cb) = 0;

    virtual bool IsStreamCall() const = 0;

    //! Finish streaming reply
    virtual void FinishStreamingOk() = 0;

    //! Returns future to get cancel of finish notification
    virtual TAsyncFinishResult GetFinishFuture() = 0;

    //! Returns peer address
    virtual TString GetPeer() const = 0;

    //! Returns true if server is using ssl
    virtual bool SslServer() const = 0;

    //! Returns true if client was not interested in result (but we still must send response to make grpc happy)
    virtual bool IsClientLost() const = 0;
};

} // namespace NYdbGrpc
