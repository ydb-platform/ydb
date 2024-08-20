#pragma once

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
template<typename TIn, typename TOut, typename TService, typename TInProtoPrinter, typename TOutProtoPrinter>
class TGRpcRequestImpl
    : public TBaseAsyncContext<TService>
    , public IQueueEvent
    , public IRequestContextBase
{
    using TThis = TGRpcRequestImpl<TIn, TOut, TService, TInProtoPrinter, TOutProtoPrinter>;

public:
    using TOnRequest = std::function<void (IRequestContextBase* ctx)>;
    using TRequestCallback = void (TService::TCurrentGRpcService::AsyncService::*)(grpc::ServerContext*, TIn*,
        grpc::ServerAsyncResponseWriter<TOut>*, grpc::CompletionQueue*, grpc::ServerCompletionQueue*, void*);
    using TStreamRequestCallback = void (TService::TCurrentGRpcService::AsyncService::*)(grpc::ServerContext*, TIn*,
        grpc::ServerAsyncWriter<TOut>*, grpc::CompletionQueue*, grpc::ServerCompletionQueue*, void*);

    TGRpcRequestImpl(TService* server,
                 typename TService::TCurrentGRpcService::AsyncService* service,
                 grpc::ServerCompletionQueue* cq,
                 TOnRequest cb,
                 TRequestCallback requestCallback,
                 const char* name,
                 TLoggerPtr logger,
                 ICounterBlockPtr counters,
                 IGRpcRequestLimiterPtr limiter)
        : TBaseAsyncContext<TService>(service, cq)
        , Server_(server)
        , Cb_(cb)
        , RequestCallback_(requestCallback)
        , StreamRequestCallback_(nullptr)
        , Name_(name)
        , Logger_(std::move(logger))
        , Counters_(std::move(counters))
        , RequestLimiter_(std::move(limiter))
        , Writer_(new grpc::ServerAsyncResponseWriter<TUniversalResponseRef<TOut>>(&this->Context))
        , StateFunc_(&TThis::SetRequestDone)
        , Request_(google::protobuf::Arena::CreateMessage<TIn>(&Arena_))
        , AuthState_(Server_->NeedAuth())
    {
        Y_ABORT_UNLESS(Request_);
        GRPC_LOG_DEBUG(Logger_, "[%p] created request Name# %s", this, Name_);
    }

    TGRpcRequestImpl(TService* server,
                 typename TService::TCurrentGRpcService::AsyncService* service,
                 grpc::ServerCompletionQueue* cq,
                 TOnRequest cb,
                 TStreamRequestCallback requestCallback,
                 const char* name,
                 TLoggerPtr logger,
                 ICounterBlockPtr counters,
                 IGRpcRequestLimiterPtr limiter)
        : TBaseAsyncContext<TService>(service, cq)
        , Server_(server)
        , Cb_(cb)
        , RequestCallback_(nullptr)
        , StreamRequestCallback_(requestCallback)
        , Name_(name)
        , Logger_(std::move(logger))
        , Counters_(std::move(counters))
        , RequestLimiter_(std::move(limiter))
        , StreamWriter_(new grpc::ServerAsyncWriter<TUniversalResponse<TOut>>(&this->Context))
        , StateFunc_(&TThis::SetRequestDone)
        , Request_(google::protobuf::Arena::CreateMessage<TIn>(&Arena_))
        , AuthState_(Server_->NeedAuth())
        , StreamAdaptor_(CreateStreamAdaptor())
    {
        Y_ABORT_UNLESS(Request_);
        GRPC_LOG_DEBUG(Logger_, "[%p] created streaming request Name# %s", this, Name_);
    }

    TAsyncFinishResult GetFinishFuture() override {
        return FinishPromise_.GetFuture();
    }

    bool IsClientLost() const override {
        return ClientLost_.load();
    }

    TString GetPeer() const override {
        // Decode URL-encoded square brackets
        auto ip = TString(this->Context.peer());
        CGIUnescape(ip);
        return ip;
    }

    bool SslServer() const override {
        return Server_->SslServer();
    }

    void Run() {
        // Start request unless server is shutting down
        if (auto guard = Server_->ProtectShutdown()) {
            Ref(); //For grpc c runtime
            this->Context.AsyncNotifyWhenDone(OnFinishTag.Prepare());
            OnBeforeCall();
            if (RequestCallback_) {
                (this->Service->*RequestCallback_)
                        (&this->Context, Request_,
                        reinterpret_cast<grpc::ServerAsyncResponseWriter<TOut>*>(Writer_.Get()), this->CQ, this->CQ, GetGRpcTag());
            } else {
                (this->Service->*StreamRequestCallback_)
                        (&this->Context, Request_,
                        reinterpret_cast<grpc::ServerAsyncWriter<TOut>*>(StreamWriter_.Get()), this->CQ, this->CQ, GetGRpcTag());
            }
        }
    }

    ~TGRpcRequestImpl() {
        // No direct dtor call allowed
        Y_ASSERT(RefCount() == 0);
    }

    bool Execute(bool ok) override {
        return (this->*StateFunc_)(ok);
    }

    void DestroyRequest() override {
        Y_ABORT_UNLESS(!CallInProgress_, "Unexpected DestroyRequest while another grpc call is still in progress");
        RequestDestroyed_ = true;
        if (RequestRegistered_) {
            Server_->DeregisterRequestCtx(this);
            RequestRegistered_ = false;
        }
        UnRef();
    }

    TInstant Deadline() const override {
        return TBaseAsyncContext<TService>::Deadline();
    }

    TSet<TStringBuf> GetPeerMetaKeys() const override {
        return TBaseAsyncContext<TService>::GetPeerMetaKeys();
    }

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override {
        return TBaseAsyncContext<TService>::GetPeerMetaValues(key);
    }

    TVector<TStringBuf> FindClientCert() const override {
        return TBaseAsyncContext<TService>::FindClientCert();
    }

    grpc_compression_level GetCompressionLevel() const override {
        return TBaseAsyncContext<TService>::GetCompressionLevel();
    }

    //! Get pointer to the request's message.
    const NProtoBuf::Message* GetRequest() const override {
        return Request_;
    }

    NProtoBuf::Message* GetRequestMut() override {
        return Request_;
    }


    TAuthState& GetAuthState() override {
        return AuthState_;
    }

    void Reply(NProtoBuf::Message* resp, ui32 status) override {
        WriteDataOk(resp, status);
    }

    void Reply(grpc::ByteBuffer* resp, ui32 status, EStreamCtrl ctrl) override {
        WriteByteDataOk(resp, status, ctrl);
    }

    void ReplyError(grpc::StatusCode code, const TString& msg, const TString& details) override {
        FinishGrpcStatus(code, msg, details, false);
    }

    void ReplyUnauthenticated(const TString& in) override {
        const TString message = in.empty() ? TString("unauthenticated") : TString("unauthenticated, ") + in;
        FinishGrpcStatus(grpc::StatusCode::UNAUTHENTICATED, message, "", false);
    }

    void SetNextReplyCallback(TOnNextReply&& cb) override {
        NextReplyCb_ = cb;
    }

    void AddTrailingMetadata(const TString& key, const TString& value) override {
        this->Context.AddTrailingMetadata(key, value);
    }

    void FinishStreamingOk() override {
        GRPC_LOG_DEBUG(Logger_, "[%p] finished streaming Name# %s peer# %s (enqueued)", this, Name_,
                  this->Context.peer().c_str());
        auto cb = [this]() {
            StateFunc_ = &TThis::SetFinishDone;
            GRPC_LOG_DEBUG(Logger_, "[%p] finished streaming Name# %s peer# %s (pushed to grpc)", this, Name_,
                      this->Context.peer().c_str());

            OnBeforeCall();
            Finished_ = true;
            StreamWriter_->Finish(grpc::Status::OK, GetGRpcTag());
        };
        StreamAdaptor_->Enqueue(std::move(cb), false);
    }

    google::protobuf::Arena* GetArena() override {
        return &Arena_;
    }

    void UseDatabase(const TString& database) override {
        Counters_->UseDatabase(database);
    }

private:
    void Clone() {
        if (!Server_->IsShuttingDown()) {
            if (RequestCallback_) {
                MakeIntrusive<TThis>(
                    Server_, this->Service, this->CQ, Cb_, RequestCallback_, Name_, Logger_, Counters_->Clone(), RequestLimiter_)->Run();
            } else {
                MakeIntrusive<TThis>(
                    Server_, this->Service, this->CQ, Cb_, StreamRequestCallback_, Name_, Logger_, Counters_->Clone(), RequestLimiter_)->Run();
            }
        }
    }

    void OnBeforeCall() {
        Y_ABORT_UNLESS(!RequestDestroyed_, "Cannot start grpc calls after request is already destroyed");
        Y_ABORT_UNLESS(!Finished_, "Cannot start grpc calls after request is finished");
        bool wasInProgress = std::exchange(CallInProgress_, true);
        Y_ABORT_UNLESS(!wasInProgress, "Another grpc call is already in progress");
    }

    void OnAfterCall() {
        Y_ABORT_UNLESS(!RequestDestroyed_, "Finished grpc call after request is already destroyed");
        bool wasInProgress = std::exchange(CallInProgress_, false);
        Y_ABORT_UNLESS(wasInProgress, "Finished grpc call that was not in progress");
    }

    void WriteDataOk(NProtoBuf::Message* resp, ui32 status) {
        auto makeResponseString = [&] {
            TString x;
            TOutProtoPrinter printer;
            printer.SetSingleLineMode(true);
            printer.PrintToString(*resp, &x);
            return x;
        };

        auto sz = (size_t)resp->ByteSize();
        if (Writer_) {
            GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# %s peer# %s", this, Name_,
                makeResponseString().data(), this->Context.peer().c_str());
            StateFunc_ = &TThis::SetFinishDone;
            ResponseSize = sz;
            ResponseStatus = status;
            Y_ABORT_UNLESS(this->Context.c_call());
            OnBeforeCall();
            Finished_ = true;
            Writer_->Finish(TUniversalResponseRef<TOut>(resp), grpc::Status::OK, GetGRpcTag());
        } else {
            GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# %s peer# %s (enqueued)",
                this, Name_, makeResponseString().data(), this->Context.peer().c_str());

            // because of std::function cannot hold move-only captured object
            // we allocate shared object on heap to avoid message copy
            auto uResp = MakeIntrusive<TUniversalResponse<TOut>>(resp);
            auto cb = [this, uResp = std::move(uResp), sz, status]() {
                GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s peer# %s (pushed to grpc)",
                    this, Name_, this->Context.peer().c_str());
                StateFunc_ = &TThis::NextReply;
                ResponseSize += sz;
                ResponseStatus = status;
                OnBeforeCall();
                StreamWriter_->Write(*uResp, GetGRpcTag());
            };
            StreamAdaptor_->Enqueue(std::move(cb), false);
        }
    }

    void WriteByteDataOk(grpc::ByteBuffer* resp, ui32 status, EStreamCtrl ctrl) {
        auto sz = resp->Length();
        if (Writer_) {
            GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# byteString peer# %s", this, Name_,
                this->Context.peer().c_str());
            StateFunc_ = &TThis::SetFinishDone;
            ResponseSize = sz;
            ResponseStatus = status;
            OnBeforeCall();
            Finished_ = true;
            Writer_->Finish(TUniversalResponseRef<TOut>(resp), grpc::Status::OK, GetGRpcTag());
        } else {
            GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# byteString peer# %s (enqueued)", this, Name_,
                this->Context.peer().c_str());

            // because of std::function cannot hold move-only captured object
            // we allocate shared object on heap to avoid buffer copy
            auto uResp = MakeIntrusive<TUniversalResponse<TOut>>(resp);
            const bool finish = ctrl == EStreamCtrl::FINISH;
            auto cb = [this, uResp = std::move(uResp), sz, status, finish]() {
                GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# byteString peer# %s (pushed to grpc)",
                    this, Name_, this->Context.peer().c_str());

                StateFunc_ = finish ? &TThis::SetFinishDone : &TThis::NextReply;

                ResponseSize += sz;
                ResponseStatus = status;
                OnBeforeCall();
                if (finish) {
                    Finished_ = true;
                    const auto option = grpc::WriteOptions().set_last_message();
                    StreamWriter_->WriteAndFinish(*uResp, option, grpc::Status::OK, GetGRpcTag());
                } else {
                    StreamWriter_->Write(*uResp, GetGRpcTag());
                }
            };
            StreamAdaptor_->Enqueue(std::move(cb), false);
        }
    }

    void FinishGrpcStatus(grpc::StatusCode code, const TString& msg, const TString& details, bool urgent) {
        Y_ABORT_UNLESS(code != grpc::OK);
        if (code == grpc::StatusCode::UNAUTHENTICATED) {
            Counters_->CountNotAuthenticated();
        } else if (code == grpc::StatusCode::RESOURCE_EXHAUSTED) {
            Counters_->CountResourceExhausted();
        }

        if (Writer_) {
            GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s nodata (%s) peer# %s, grpc status# (%d)", this,
                Name_, msg.c_str(), this->Context.peer().c_str(), (int)code);
            StateFunc_ = &TThis::SetFinishError;
            TOut resp;
            OnBeforeCall();
            Finished_ = true;
            Writer_->Finish(TUniversalResponseRef<TOut>(&resp), grpc::Status(code, msg, details), GetGRpcTag());
        } else {
            GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s nodata (%s) peer# %s, grpc status# (%d)"
                                    " (enqueued)", this, Name_, msg.c_str(), this->Context.peer().c_str(), (int)code);
            auto cb = [this, code, msg, details]() {
                GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s nodata (%s) peer# %s, grpc status# (%d)"
                                        " (pushed to grpc)", this, Name_, msg.c_str(),
                               this->Context.peer().c_str(), (int)code);
                StateFunc_ = &TThis::SetFinishError;
                OnBeforeCall();
                Finished_ = true;
                StreamWriter_->Finish(grpc::Status(code, msg, details), GetGRpcTag());
            };
            StreamAdaptor_->Enqueue(std::move(cb), urgent);
        }
    }

    bool SetRequestDone(bool ok) {
        OnAfterCall();

        auto makeRequestString = [&] {
            TString resp;
            if (ok) {
                TInProtoPrinter printer;
                printer.SetSingleLineMode(true);
                printer.PrintToString(*Request_, &resp);
            } else {
                resp = "<not ok>";
            }
            return resp;
        };
        GRPC_LOG_DEBUG(Logger_, "[%p] received request Name# %s ok# %s data# %s peer# %s", this, Name_,
            ok ? "true" : "false", makeRequestString().data(), this->Context.peer().c_str());

        if (this->Context.c_call() == nullptr) {
            Y_ABORT_UNLESS(!ok);
            // One ref by OnFinishTag, grpc will not call this tag if no request received
            UnRef();
        } else if (!(RequestRegistered_ = Server_->RegisterRequestCtx(this))) {
            // Request cannot be registered due to shutdown
            // It's unsafe to continue, so drop this request without processing
            GRPC_LOG_DEBUG(Logger_, "[%p] dropping request Name# %s due to shutdown", this, Name_);
            this->Context.TryCancel();
            return false;
        }

        Clone(); // TODO: Request pool?
        if (!ok) {
            Counters_->CountNotOkRequest();
            return false;
        }

        if (IncRequest()) {
            // Adjust counters.
            RequestSize = Request_->ByteSize();
            Counters_->StartProcessing(RequestSize);
            RequestTimer.Reset();

            if (!SslServer()) {
                Counters_->CountRequestWithoutTls();
            }

            //TODO: Move this in to grpc_request_proxy
            auto maybeDatabase = GetPeerMetaValues(TStringBuf("x-ydb-database"));
            if (maybeDatabase.empty()) {
                Counters_->CountRequestsWithoutDatabase();
            }
            auto maybeToken = GetPeerMetaValues(TStringBuf("x-ydb-auth-ticket"));
            if (maybeToken.empty() || maybeToken[0].empty()) {
                TString db{maybeDatabase ? maybeDatabase[0] : TStringBuf{}};
                Counters_->CountRequestsWithoutToken();
                GRPC_LOG_DEBUG(Logger_, "[%p] received request without user token "
                    "Name# %s data# %s peer# %s database# %s", this, Name_,
                    makeRequestString().data(), this->Context.peer().c_str(), db.c_str());
            }

            // Handle current request.
            Cb_(this);
        } else {
            //This request has not been counted
            SkipUpdateCountersOnError = true;
            FinishGrpcStatus(grpc::StatusCode::RESOURCE_EXHAUSTED, "no resource", "", true);
        }
        return true;
    }

    bool NextReply(bool ok) {
        OnAfterCall();

        auto logCb = [this, ok](int left) {
            GRPC_LOG_DEBUG(Logger_, "[%p] ready for next reply Name# %s ok# %s peer# %s left# %d", this, Name_,
                ok ? "true" : "false", this->Context.peer().c_str(), left);
        };

        if (!ok) {
            logCb(-1);
            DecRequest();
            Counters_->FinishProcessing(RequestSize, ResponseSize, ok, ResponseStatus,
                TDuration::Seconds(RequestTimer.Passed()));
            return false;
        }

        Ref();  // To prevent destroy during this call in case of execution Finish
        size_t left = StreamAdaptor_->ProcessNext();
        logCb(left);
        if (NextReplyCb_) {
            NextReplyCb_(left);
        }
        // Now it is safe to destroy even if Finish was called
        UnRef();
        return true;
    }

    bool SetFinishDone(bool ok) {
        OnAfterCall();

        GRPC_LOG_DEBUG(Logger_, "[%p] finished request Name# %s ok# %s peer# %s", this, Name_,
            ok ? "true" : "false", this->Context.peer().c_str());
        //PrintBackTrace();
        DecRequest();
        Counters_->FinishProcessing(RequestSize, ResponseSize, ok, ResponseStatus,
            TDuration::Seconds(RequestTimer.Passed()));
        return false;
    }

    bool SetFinishError(bool ok) {
        OnAfterCall();

        GRPC_LOG_DEBUG(Logger_, "[%p] finished request with error Name# %s ok# %s peer# %s", this, Name_,
            ok ? "true" : "false", this->Context.peer().c_str());
        if (!SkipUpdateCountersOnError) {
            DecRequest();
            Counters_->FinishProcessing(RequestSize, ResponseSize, ok, ResponseStatus,
                TDuration::Seconds(RequestTimer.Passed()));
        }
        return false;
    }

    // Returns pointer to IQueueEvent to pass into grpc c runtime
    // Implicit C style cast from this to void* is wrong due to multiple inheritance
    void* GetGRpcTag() {
        return static_cast<IQueueEvent*>(this);
    }

    void OnFinish(EQueueEventStatus evStatus) {
        if (this->Context.IsCancelled()) {
            ClientLost_.store(true);
            FinishPromise_.SetValue(EFinishStatus::CANCEL);
        } else {
            FinishPromise_.SetValue(evStatus == EQueueEventStatus::OK ? EFinishStatus::OK : EFinishStatus::ERROR);
        }
    }

    bool IncRequest() {
        if (!Server_->IncRequest())
            return false;

        if (!RequestLimiter_)
            return true;

        if (!RequestLimiter_->IncRequest()) {
            Server_->DecRequest();
            return false;
        }

        return true;
    }

    void DecRequest() {
        if (RequestLimiter_) {
            RequestLimiter_->DecRequest();
        }
        Server_->DecRequest();
    }

    using TStateFunc = bool (TThis::*)(bool);
    TService* Server_ = nullptr;
    TOnRequest Cb_;
    TRequestCallback RequestCallback_;
    TStreamRequestCallback StreamRequestCallback_;
    const char* const Name_;
    TLoggerPtr Logger_;
    ICounterBlockPtr Counters_;
    IGRpcRequestLimiterPtr RequestLimiter_;

    THolder<grpc::ServerAsyncResponseWriter<TUniversalResponseRef<TOut>>> Writer_;
    THolder<grpc::ServerAsyncWriterInterface<TUniversalResponse<TOut>>> StreamWriter_;
    TStateFunc StateFunc_;

    google::protobuf::Arena Arena_;
    TIn* Request_ = nullptr;
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

    using TFixedEvent = TQueueFixedEvent<TGRpcRequestImpl>;
    TFixedEvent OnFinishTag = { this, &TGRpcRequestImpl::OnFinish };
    NThreading::TPromise<EFinishStatus> FinishPromise_ = NThreading::NewPromise<EFinishStatus>();
    bool SkipUpdateCountersOnError = false;
    IStreamAdaptor::TPtr StreamAdaptor_;
    std::atomic<bool> ClientLost_ = false;
};

template<typename TIn, typename TOut, typename TService, typename TInProtoPrinter=google::protobuf::TextFormat::Printer, typename TOutProtoPrinter=google::protobuf::TextFormat::Printer>
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
                 ICounterBlockPtr counters)
        : TBase{server, service, cq, std::move(cb), std::move(requestCallback), name, std::move(logger), std::move(counters), nullptr}
    {
    }
};

} // namespace NYdbGrpc
