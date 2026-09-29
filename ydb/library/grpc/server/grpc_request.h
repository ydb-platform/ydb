#pragma once

#include <ydb/library/protobuf_printer/security_printer.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/message.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/logger/priority.h>
#include <library/cpp/string_utils/quote/quote.h>

#include "grpc_response.h"
#include "event_callback.h"
#include "grpc_async_ctx_base.h"
#include "grpc_counters.h"
#include "grpc_request_base.h"
#include "grpc_server.h"
#include "logger.h"

#include <util/string/builder.h>
#include <util/system/hp_timer.h>

#include <grpc++/server.h>
#include <grpc++/server_context.h>
#include <grpc++/support/async_stream.h>
#include <grpc++/support/async_unary_call.h>
#include <grpc++/support/byte_buffer.h>
#include <grpc++/impl/codegen/async_stream.h>

namespace NYdbGrpc {

class IStreamAdaptor {
public:
    using TPtr = std::unique_ptr<IStreamAdaptor>;
    virtual void Enqueue(std::function<void()>&& fn, bool urgent) = 0;
    virtual size_t ProcessNext() = 0;
    virtual ~IStreamAdaptor() = default;
};

IStreamAdaptor::TPtr CreateStreamAdaptor();

///////////////////////////////////////////////////////////////////////////////
//! Type-independent part of the grpc server request. Holds the whole request
//! state machine; the typed TGRpcRequestImpl only issues the typed grpc request
//! call and creates the next request object.
class TGRpcRequestImplBase
    : public TBaseAsyncContextCommon
    , public IQueueEvent
    , public IRequestContextBase
{
public:
    using TOnRequest = std::function<void (IRequestContextBase* ctx)>;

    TAsyncFinishResult GetFinishFuture() override;
    bool IsClientLost() const override;
    bool IsStreamCall() const override;
    bool SslServer() const override;
    TString GetRpcMethodName() const override;

    //! Start waiting for the request unless server is shutting down
    void Run();

    bool Execute(bool ok) override;
    void DestroyRequest() override;

    TString GetPeer() const override;
    TString GetAuthority() const override;
    TInstant Deadline() const override;
    TSet<TStringBuf> GetPeerMetaKeys() const override;
    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override;
    TVector<TStringBuf> FindClientCert() const override;
    grpc_compression_level GetCompressionLevel() const override;
    TString GetEndpointId() const override;

    //! Get pointer to the request's message.
    const NProtoBuf::Message* GetRequest() const override;
    TAuthState& GetAuthState() override;

    void Reply(NProtoBuf::Message* resp, ui32 status) override;
    void Reply(grpc::ByteBuffer* resp, ui32 status, EStreamCtrl ctrl) override;
    void ReplyError(grpc::StatusCode code, const TString& msg, const TString& details) override;
    void ReplyUnauthenticated(const TString& in) override;
    void SetNextReplyCallback(TOnNextReply&& cb) override;
    void AddTrailingMetadata(const TString& key, const TString& value) override;
    void FinishStreamingOk() override;
    google::protobuf::Arena* GetArena() override;
    void UseDatabase(const TString& database) override;

protected:
    // Writer types do not depend on the response type: the unary writer
    // serializes either a message or a byte buffer by reference, the
    // streaming writer always writes already serialized byte buffers.
    using TUnaryWriter = grpc::ServerAsyncResponseWriter<TUniversalResponseRef<NProtoBuf::Message>>;
    using TStreamWriter = grpc::ServerAsyncWriter<grpc::ByteBuffer>;

    TGRpcRequestImplBase(TGrpcServiceProtectiable* server,
                         grpc::ServerCompletionQueue* cq,
                         TOnRequest&& cb,
                         const char* serviceName,
                         const char* name,
                         TLoggerPtr&& logger,
                         ICounterBlockPtr&& counters,
                         IGRpcRequestLimiterPtr&& limiter,
                         const NProtoBuf::Message& requestPrototype,
                         bool streaming,
                         bool needAuth);

public:
    ~TGRpcRequestImplBase();

protected:
    //! Issue the typed grpc Request<Method> call (called once per request)
    virtual void RequestCall() = 0;

    //! Create and run the next request object for the same method (called once per request)
    virtual void CloneAndRun(ICounterBlockPtr counters) = 0;

    // Returns pointer to IQueueEvent to pass into grpc c runtime
    // Implicit C style cast from this to void* is wrong due to multiple inheritance
    void* GetGRpcTag() {
        return static_cast<IQueueEvent*>(this);
    }

private:
    class TSharedByteBuffer;

    void Clone();
    void OnBeforeCall();
    void OnAfterCall();
    void WriteDataOk(NProtoBuf::Message* resp, ui32 status);
    void WriteByteDataOk(grpc::ByteBuffer* resp, ui32 status, EStreamCtrl ctrl);
    void EnqueueStreamWrite(TIntrusivePtr<TSharedByteBuffer> buffer, size_t sz, ui32 status, bool finish, bool byteData);
    void FinishGrpcStatus(grpc::StatusCode code, const TString& msg, const TString& details, bool urgent);
    bool SetRequestDone(bool ok);
    bool NextReply(bool ok);
    bool SetFinishDone(bool ok);
    bool SetFinishError(bool ok);
    void OnFinish(EQueueEventStatus evStatus);
    bool IncRequest();
    void DecRequest();

protected:
    TGrpcServiceProtectiable* const Server_ = nullptr;
    TOnRequest Cb_;
    const char* const ServiceName_;
    const char* const Name_;
    TLoggerPtr Logger_;
    ICounterBlockPtr Counters_;
    IGRpcRequestLimiterPtr RequestLimiter_;

    THolder<TUnaryWriter> Writer_;
    THolder<TStreamWriter> StreamWriter_;

private:
    using TStateFunc = bool (TGRpcRequestImplBase::*)(bool);
    TStateFunc StateFunc_;

protected:
    google::protobuf::Arena Arena_;
    NProtoBuf::Message* Request_ = nullptr;

private:
    TOnNextReply NextReplyCb_;
    ui32 RequestSize = 0;
    ui32 ResponseSize = 0;
    ui32 ResponseStatus = 0;
    THPTimer RequestTimer;
    TAuthState AuthState_ = 0;
    bool RequestRegistered_ = false;
    bool RequestDestroyed_ = false;
    bool CallInProgress_ = false;
    bool Finished_ = false;

    using TFixedEvent = TQueueFixedEvent<TGRpcRequestImplBase>;
    TFixedEvent OnFinishTag = { this, &TGRpcRequestImplBase::OnFinish };
    NThreading::TPromise<EFinishStatus> FinishPromise_ = NThreading::NewPromise<EFinishStatus>();
    bool SkipUpdateCountersOnError = false;
    IStreamAdaptor::TPtr StreamAdaptor_;
    std::atomic<bool> ClientLost_ = false;
};

///////////////////////////////////////////////////////////////////////////////
//! Typed grpc server request. Only the parts that really depend on the
//! request/response/service types live here: the typed Request<Method> call
//! and the creation of the next request object.
//! TInProtoPrinter and TOutProtoPrinter are kept for source compatibility and
//! are not used: messages are logged with the runtime-descriptor security printer.
template<typename TIn, typename TOut, typename TService, typename TInProtoPrinter, typename TOutProtoPrinter>
class TGRpcRequestImpl
    : public TGRpcRequestImplBase
{
    using TThis = TGRpcRequestImpl<TIn, TOut, TService, TInProtoPrinter, TOutProtoPrinter>;
    using TAsyncService = typename TService::TCurrentGRpcService::AsyncService;

public:
    using TOnRequest = TGRpcRequestImplBase::TOnRequest;
    using TRequestCallback = void (TAsyncService::*)(grpc::ServerContext*, TIn*,
        grpc::ServerAsyncResponseWriter<TOut>*, grpc::CompletionQueue*, grpc::ServerCompletionQueue*, void*);
    using TStreamRequestCallback = void (TAsyncService::*)(grpc::ServerContext*, TIn*,
        grpc::ServerAsyncWriter<TOut>*, grpc::CompletionQueue*, grpc::ServerCompletionQueue*, void*);

    TGRpcRequestImpl(TService* server,
                 TAsyncService* service,
                 grpc::ServerCompletionQueue* cq,
                 TOnRequest cb,
                 TRequestCallback requestCallback,
                 const char* name,
                 TLoggerPtr logger,
                 ICounterBlockPtr counters,
                 IGRpcRequestLimiterPtr limiter)
        : TGRpcRequestImplBase(server, cq, std::move(cb), TService::TCurrentGRpcService::service_full_name(), name,
            std::move(logger), std::move(counters), std::move(limiter), TIn::default_instance(), false, server->NeedAuth())
        , Service(service)
        , RequestCallback_(requestCallback)
        , StreamRequestCallback_(nullptr)
    {
    }

    TGRpcRequestImpl(TService* server,
                 TAsyncService* service,
                 grpc::ServerCompletionQueue* cq,
                 TOnRequest cb,
                 TStreamRequestCallback requestCallback,
                 const char* name,
                 TLoggerPtr logger,
                 ICounterBlockPtr counters,
                 IGRpcRequestLimiterPtr limiter)
        : TGRpcRequestImplBase(server, cq, std::move(cb), TService::TCurrentGRpcService::service_full_name(), name,
            std::move(logger), std::move(counters), std::move(limiter), TIn::default_instance(), true, server->NeedAuth())
        , Service(service)
        , RequestCallback_(nullptr)
        , StreamRequestCallback_(requestCallback)
    {
    }

private:
    void RequestCall() override {
        TIn* request = static_cast<TIn*>(Request_);
        if (RequestCallback_) {
            (Service->*RequestCallback_)
                    (&Context, request,
                    reinterpret_cast<grpc::ServerAsyncResponseWriter<TOut>*>(Writer_.Get()), CQ, CQ, GetGRpcTag());
        } else {
            (Service->*StreamRequestCallback_)
                    (&Context, request,
                    reinterpret_cast<grpc::ServerAsyncWriter<TOut>*>(StreamWriter_.Get()), CQ, CQ, GetGRpcTag());
        }
    }

    void CloneAndRun(ICounterBlockPtr counters) override {
        if (RequestCallback_) {
            MakeIntrusive<TThis>(
                static_cast<TService*>(Server_), Service, CQ, Cb_, RequestCallback_, Name_, Logger_, std::move(counters), RequestLimiter_)->Run();
        } else {
            MakeIntrusive<TThis>(
                static_cast<TService*>(Server_), Service, CQ, Cb_, StreamRequestCallback_, Name_, Logger_, std::move(counters), RequestLimiter_)->Run();
        }
    }

    TAsyncService* const Service;
    TRequestCallback RequestCallback_;
    TStreamRequestCallback StreamRequestCallback_;
};

template<typename TIn, typename TOut, typename TService, typename TInProtoPrinter = ::NKikimr::TSecurityTextFormatPrinter<TIn>, typename TOutProtoPrinter = ::NKikimr::TSecurityTextFormatPrinter<TOut>>
class TGRpcRequest: public TGRpcRequestImpl<TIn, TOut, TService, TInProtoPrinter, TOutProtoPrinter> {
    using TBase = TGRpcRequestImpl<TIn, TOut, TService, TInProtoPrinter, TOutProtoPrinter>;
public:
    TGRpcRequest(TService* server,
                 typename TService::TCurrentGRpcService::AsyncService* service,
                 grpc::ServerCompletionQueue* cq,
                 typename TBase::TOnRequest cb,
                 typename TBase::TRequestCallback requestCallback,
                 const char* name,
                 TLoggerPtr logger,
                 ICounterBlockPtr counters,
                 IGRpcRequestLimiterPtr limiter = nullptr)
        : TBase{server, service, cq, std::move(cb), std::move(requestCallback), name, std::move(logger), std::move(counters), std::move(limiter)}
    {
    }

    TGRpcRequest(TService* server,
                 typename TService::TCurrentGRpcService::AsyncService* service,
                 grpc::ServerCompletionQueue* cq,
                 typename TBase::TOnRequest cb,
                 typename TBase::TStreamRequestCallback requestCallback,
                 const char* name,
                 TLoggerPtr logger,
                 ICounterBlockPtr counters,
                 IGRpcRequestLimiterPtr limiter = nullptr)
        : TBase{server, service, cq, std::move(cb), std::move(requestCallback), name, std::move(logger), std::move(counters), std::move(limiter)}
    {
    }
};

} // namespace NYdbGrpc
