#include "grpc_request.h"

namespace NYdbGrpc {

const char* GRPC_USER_AGENT_HEADER = "user-agent";

class TStreamAdaptor: public IStreamAdaptor {
public:
    TStreamAdaptor()
        : StreamIsReady_(true)
    {}

    void Enqueue(std::function<void()>&& fn, bool urgent) override {
        with_lock(Mtx_) {
            if (!UrgentQueue_.empty() || !NormalQueue_.empty()) {
                Y_ABORT_UNLESS(!StreamIsReady_);
            }
            auto& queue = urgent ? UrgentQueue_ : NormalQueue_;
            if (StreamIsReady_ && queue.empty()) {
                StreamIsReady_ = false;
            } else {
                queue.push_back(std::move(fn));
                return;
            }
        }
        fn();
    }

    size_t ProcessNext() override {
        size_t left = 0;
        std::function<void()> fn;
        with_lock(Mtx_) {
            Y_ABORT_UNLESS(!StreamIsReady_);
            auto& queue = UrgentQueue_.empty() ? NormalQueue_ : UrgentQueue_;
            if (queue.empty()) {
                // Both queues are empty
                StreamIsReady_ = true;
            } else {
                fn = std::move(queue.front());
                queue.pop_front();
                left = UrgentQueue_.size() + NormalQueue_.size();
            }
        }
        if (fn)
            fn();
        return left;
    }
private:
    bool StreamIsReady_;
    TList<std::function<void()>> NormalQueue_;
    TList<std::function<void()>> UrgentQueue_;
    TMutex Mtx_;
};

IStreamAdaptor::TPtr CreateStreamAdaptor() {
    return std::make_unique<TStreamAdaptor>();
}

///////////////////////////////////////////////////////////////////////////////
// TGRpcRequestImplBase

namespace {

TString MakeMessageString(const NProtoBuf::Message& msg) {
    TString x;
    NKikimr::TSecurityTextFormatPrinterBase printer(msg.GetDescriptor());
    printer.SetSingleLineMode(true);
    printer.PrintToString(msg, &x);
    return x;
}

} // namespace

// because of std::function cannot hold move-only captured object
// we allocate shared object on heap to avoid buffer copy
class TGRpcRequestImplBase::TSharedByteBuffer: public TAtomicRefCount<TSharedByteBuffer> {
public:
    grpc::ByteBuffer Buffer;
};

TGRpcRequestImplBase::TGRpcRequestImplBase(TGrpcServiceProtectiable* server,
                                           grpc::ServerCompletionQueue* cq,
                                           TOnRequest&& cb,
                                           const char* serviceName,
                                           const char* name,
                                           TLoggerPtr&& logger,
                                           ICounterBlockPtr&& counters,
                                           IGRpcRequestLimiterPtr&& limiter,
                                           const NProtoBuf::Message& requestPrototype,
                                           bool streaming,
                                           bool needAuth)
    : TBaseAsyncContextCommon(cq)
    , Server_(server)
    , Cb_(std::move(cb))
    , ServiceName_(serviceName)
    , Name_(name)
    , Logger_(std::move(logger))
    , Counters_(std::move(counters))
    , RequestLimiter_(std::move(limiter))
    , StateFunc_(&TGRpcRequestImplBase::SetRequestDone)
    , AuthState_(needAuth)
{
    if (streaming) {
        StreamWriter_.Reset(new TStreamWriter(&Context));
        StreamAdaptor_ = CreateStreamAdaptor();
    } else {
        Writer_.Reset(new TUnaryWriter(&Context));
    }
    Request_ = requestPrototype.New(&Arena_);
    Y_ABORT_UNLESS(Request_);
    if (streaming) {
        GRPC_LOG_DEBUG(Logger_, "[%p] created streaming request Name# %s", this, GetRpcMethodName().c_str());
    } else {
        GRPC_LOG_DEBUG(Logger_, "[%p] created request Name# %s", this, GetRpcMethodName().c_str());
    }
}

TGRpcRequestImplBase::~TGRpcRequestImplBase() {
    // No direct dtor call allowed
    Y_ASSERT(RefCount() == 0);
}

TGRpcRequestImplBase::TAsyncFinishResult TGRpcRequestImplBase::GetFinishFuture() {
    return FinishPromise_.GetFuture();
}

bool TGRpcRequestImplBase::IsClientLost() const {
    return ClientLost_.load();
}

bool TGRpcRequestImplBase::IsStreamCall() const {
    return bool(StreamAdaptor_);
}

bool TGRpcRequestImplBase::SslServer() const {
    return Server_->SslServer();
}

TString TGRpcRequestImplBase::GetRpcMethodName() const {
    return TStringBuilder() << ServiceName_ << '/' << Name_;
}

void TGRpcRequestImplBase::Run() {
    // Start request unless server is shutting down
    if (auto guard = Server_->ProtectShutdown()) {
        Ref(); //For grpc c runtime
        Context.AsyncNotifyWhenDone(OnFinishTag.Prepare());
        OnBeforeCall();
        RequestCall();
    }
}

bool TGRpcRequestImplBase::Execute(bool ok) {
    return (this->*StateFunc_)(ok);
}

void TGRpcRequestImplBase::DestroyRequest() {
    Y_ABORT_UNLESS(!CallInProgress_, "Unexpected DestroyRequest while another grpc call is still in progress");
    RequestDestroyed_ = true;
    if (RequestRegistered_) {
        Server_->DeregisterRequestCtx(this);
        RequestRegistered_ = false;
    }
    UnRef();
}

TString TGRpcRequestImplBase::GetPeer() const {
    return TBaseAsyncContextCommon::GetPeer();
}

TString TGRpcRequestImplBase::GetAuthority() const {
    return TBaseAsyncContextCommon::GetAuthority();
}

TInstant TGRpcRequestImplBase::Deadline() const {
    return TBaseAsyncContextCommon::Deadline();
}

TSet<TStringBuf> TGRpcRequestImplBase::GetPeerMetaKeys() const {
    return TBaseAsyncContextCommon::GetPeerMetaKeys();
}

TVector<TStringBuf> TGRpcRequestImplBase::GetPeerMetaValues(TStringBuf key) const {
    return TBaseAsyncContextCommon::GetPeerMetaValues(key);
}

TVector<TStringBuf> TGRpcRequestImplBase::FindClientCert() const {
    return TBaseAsyncContextCommon::FindClientCert();
}

grpc_compression_level TGRpcRequestImplBase::GetCompressionLevel() const {
    return TBaseAsyncContextCommon::GetCompressionLevel();
}

TString TGRpcRequestImplBase::GetEndpointId() const {
    return Server_->GetEndpointId();
}

const NProtoBuf::Message* TGRpcRequestImplBase::GetRequest() const {
    return Request_;
}

TAuthState& TGRpcRequestImplBase::GetAuthState() {
    return AuthState_;
}

void TGRpcRequestImplBase::Reply(NProtoBuf::Message* resp, ui32 status) {
    WriteDataOk(resp, status);
}

void TGRpcRequestImplBase::Reply(grpc::ByteBuffer* resp, ui32 status, EStreamCtrl ctrl) {
    WriteByteDataOk(resp, status, ctrl);
}

void TGRpcRequestImplBase::ReplyError(grpc::StatusCode code, const TString& msg, const TString& details) {
    FinishGrpcStatus(code, msg, details, false);
}

void TGRpcRequestImplBase::ReplyUnauthenticated(const TString& in) {
    const TString message = in.empty() ? TString("unauthenticated") : TString("unauthenticated, ") + in;
    FinishGrpcStatus(grpc::StatusCode::UNAUTHENTICATED, message, "", false);
}

void TGRpcRequestImplBase::SetNextReplyCallback(TOnNextReply&& cb) {
    NextReplyCb_ = cb;
}

void TGRpcRequestImplBase::AddTrailingMetadata(const TString& key, const TString& value) {
    Context.AddTrailingMetadata(key, value);
}

void TGRpcRequestImplBase::FinishStreamingOk() {
    GRPC_LOG_DEBUG(Logger_, "[%p] finished streaming Name# %s peer# %s (enqueued)", this, GetRpcMethodName().c_str(),
              Context.peer().c_str());
    auto cb = [this]() {
        StateFunc_ = &TGRpcRequestImplBase::SetFinishDone;
        GRPC_LOG_DEBUG(Logger_, "[%p] finished streaming Name# %s peer# %s (pushed to grpc)", this, GetRpcMethodName().c_str(),
                  Context.peer().c_str());

        OnBeforeCall();
        Finished_ = true;
        StreamWriter_->Finish(grpc::Status::OK, GetGRpcTag());
    };
    StreamAdaptor_->Enqueue(std::move(cb), false);
}

google::protobuf::Arena* TGRpcRequestImplBase::GetArena() {
    return &Arena_;
}

void TGRpcRequestImplBase::UseDatabase(const TString& database) {
    Counters_->UseDatabase(database);
}

void TGRpcRequestImplBase::Clone() {
    if (!Server_->IsShuttingDown()) {
        CloneAndRun(Counters_->Clone());
    }
}

void TGRpcRequestImplBase::OnBeforeCall() {
    Y_ABORT_UNLESS(!RequestDestroyed_, "Cannot start grpc calls after request is already destroyed");
    Y_ABORT_UNLESS(!Finished_, "Cannot start grpc calls after request is finished");
    bool wasInProgress = std::exchange(CallInProgress_, true);
    Y_ABORT_UNLESS(!wasInProgress, "Another grpc call is already in progress");
}

void TGRpcRequestImplBase::OnAfterCall() {
    Y_ABORT_UNLESS(!RequestDestroyed_, "Finished grpc call after request is already destroyed");
    bool wasInProgress = std::exchange(CallInProgress_, false);
    Y_ABORT_UNLESS(wasInProgress, "Finished grpc call that was not in progress");
}

void TGRpcRequestImplBase::WriteDataOk(NProtoBuf::Message* resp, ui32 status) {
    auto sz = (size_t)resp->ByteSize();
    if (Writer_) {
        GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# %s peer# %s", this, GetRpcMethodName().c_str(),
            MakeMessageString(*resp).data(), Context.peer().c_str());
        StateFunc_ = &TGRpcRequestImplBase::SetFinishDone;
        ResponseSize = sz;
        ResponseStatus = status;
        Y_ABORT_UNLESS(Context.c_call());
        OnBeforeCall();
        Finished_ = true;
        Writer_->Finish(TUniversalResponseRef<NProtoBuf::Message>(resp), grpc::Status::OK, GetGRpcTag());
    } else {
        GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# %s peer# %s (enqueued)",
            this, GetRpcMethodName().c_str(), MakeMessageString(*resp).data(), Context.peer().c_str());

        // Serialize the message right away, so the stream writer only deals
        // with byte buffers. The message is left cleared, like it was when
        // it was swapped into the owning response object.
        auto buffer = MakeIntrusive<TSharedByteBuffer>();
        bool ownBuffer = false;
        const grpc::Status serializeStatus = grpc::SerializationTraits<NProtoBuf::Message>::Serialize(*resp, &buffer->Buffer, &ownBuffer);
        Y_ABORT_UNLESS(serializeStatus.ok(), "Unable to serialize response Name# %s: %s",
            GetRpcMethodName().c_str(), serializeStatus.error_message().c_str());
        resp->Clear();
        EnqueueStreamWrite(std::move(buffer), sz, status, false, false);
    }
}

void TGRpcRequestImplBase::WriteByteDataOk(grpc::ByteBuffer* resp, ui32 status, EStreamCtrl ctrl) {
    auto sz = resp->Length();
    if (Writer_) {
        GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# byteString peer# %s", this, GetRpcMethodName().c_str(),
            Context.peer().c_str());
        StateFunc_ = &TGRpcRequestImplBase::SetFinishDone;
        ResponseSize = sz;
        ResponseStatus = status;
        OnBeforeCall();
        Finished_ = true;
        Writer_->Finish(TUniversalResponseRef<NProtoBuf::Message>(resp), grpc::Status::OK, GetGRpcTag());
    } else {
        GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# byteString peer# %s (enqueued)", this, GetRpcMethodName().c_str(),
            Context.peer().c_str());

        auto buffer = MakeIntrusive<TSharedByteBuffer>();
        buffer->Buffer.Swap(resp);
        EnqueueStreamWrite(std::move(buffer), sz, status, ctrl == EStreamCtrl::FINISH, true);
    }
}

void TGRpcRequestImplBase::EnqueueStreamWrite(TIntrusivePtr<TSharedByteBuffer> buffer, size_t sz, ui32 status, bool finish, bool byteData) {
    auto cb = [this, buffer = std::move(buffer), sz, status, finish, byteData]() {
        if (byteData) {
            GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s data# byteString peer# %s (pushed to grpc)",
                this, GetRpcMethodName().c_str(), Context.peer().c_str());
        } else {
            GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s peer# %s (pushed to grpc)",
                this, GetRpcMethodName().c_str(), Context.peer().c_str());
        }

        StateFunc_ = finish ? &TGRpcRequestImplBase::SetFinishDone : &TGRpcRequestImplBase::NextReply;

        ResponseSize += sz;
        ResponseStatus = status;
        OnBeforeCall();
        if (finish) {
            Finished_ = true;
            const auto option = grpc::WriteOptions().set_last_message();
            StreamWriter_->WriteAndFinish(buffer->Buffer, option, grpc::Status::OK, GetGRpcTag());
        } else {
            StreamWriter_->Write(buffer->Buffer, GetGRpcTag());
        }
    };
    StreamAdaptor_->Enqueue(std::move(cb), false);
}

void TGRpcRequestImplBase::FinishGrpcStatus(grpc::StatusCode code, const TString& msg, const TString& details, bool urgent) {
    Y_ABORT_UNLESS(code != grpc::OK);
    if (code == grpc::StatusCode::UNAUTHENTICATED) {
        Counters_->CountNotAuthenticated();
    } else if (code == grpc::StatusCode::RESOURCE_EXHAUSTED) {
        Counters_->CountResourceExhausted();
    }

    if (Writer_) {
        GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s nodata (%s) peer# %s, grpc status# (%d)", this,
            GetRpcMethodName().c_str(), msg.c_str(), Context.peer().c_str(), (int)code);
        StateFunc_ = &TGRpcRequestImplBase::SetFinishError;
        OnBeforeCall();
        Finished_ = true;
        // The response message is never sent with a non-OK status
        Writer_->FinishWithError(grpc::Status(code, msg, details), GetGRpcTag());
    } else {
        GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s nodata (%s) peer# %s, grpc status# (%d)"
                                " (enqueued)", this, GetRpcMethodName().c_str(), msg.c_str(), Context.peer().c_str(), (int)code);
        auto cb = [this, code, msg, details]() {
            GRPC_LOG_DEBUG(Logger_, "[%p] issuing response Name# %s nodata (%s) peer# %s, grpc status# (%d)"
                                    " (pushed to grpc)", this, GetRpcMethodName().c_str(), msg.c_str(),
                           Context.peer().c_str(), (int)code);
            StateFunc_ = &TGRpcRequestImplBase::SetFinishError;
            OnBeforeCall();
            Finished_ = true;
            StreamWriter_->Finish(grpc::Status(code, msg, details), GetGRpcTag());
        };
        StreamAdaptor_->Enqueue(std::move(cb), urgent);
    }
}

bool TGRpcRequestImplBase::SetRequestDone(bool ok) {
    OnAfterCall();

    auto makeRequestString = [&] {
        TString resp;
        if (ok) {
            resp = MakeMessageString(*Request_);
        } else {
            resp = "<not ok>";
        }
        return resp;
    };
    GRPC_LOG_DEBUG(Logger_, "[%p] received request Name# %s ok# %s data# %s peer# %s", this, GetRpcMethodName().c_str(),
        ok ? "true" : "false", makeRequestString().data(), Context.peer().c_str());

    if (Context.c_call() == nullptr) {
        Y_ABORT_UNLESS(!ok);
        // One ref by OnFinishTag, grpc will not call this tag if no request received
        UnRef();
    } else if (!(RequestRegistered_ = Server_->RegisterRequestCtx(this))) {
        // Request cannot be registered due to shutdown
        // It's unsafe to continue, so drop this request without processing
        GRPC_LOG_DEBUG(Logger_, "[%p] dropping request Name# %s due to shutdown", this, GetRpcMethodName().c_str());
        Context.TryCancel();
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
        Counters_->StartProcessing(RequestSize, Deadline());
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
                "Name# %s data# %s peer# %s database# %s", this, GetRpcMethodName().c_str(),
                makeRequestString().data(), Context.peer().c_str(), db.c_str());
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

bool TGRpcRequestImplBase::NextReply(bool ok) {
    OnAfterCall();

    auto logCb = [this, ok](int left) {
        GRPC_LOG_DEBUG(Logger_, "[%p] ready for next reply Name# %s ok# %s peer# %s left# %d", this, GetRpcMethodName().c_str(),
            ok ? "true" : "false", Context.peer().c_str(), left);
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

bool TGRpcRequestImplBase::SetFinishDone(bool ok) {
    OnAfterCall();

    GRPC_LOG_DEBUG(Logger_, "[%p] finished request Name# %s ok# %s peer# %s", this, GetRpcMethodName().c_str(),
        ok ? "true" : "false", Context.peer().c_str());
    //PrintBackTrace();
    DecRequest();
    Counters_->FinishProcessing(RequestSize, ResponseSize, ok, ResponseStatus,
        TDuration::Seconds(RequestTimer.Passed()));
    return false;
}

bool TGRpcRequestImplBase::SetFinishError(bool ok) {
    OnAfterCall();

    GRPC_LOG_DEBUG(Logger_, "[%p] finished request with error Name# %s ok# %s peer# %s", this, GetRpcMethodName().c_str(),
        ok ? "true" : "false", Context.peer().c_str());
    if (!SkipUpdateCountersOnError) {
        DecRequest();
        Counters_->FinishProcessing(RequestSize, ResponseSize, ok, ResponseStatus,
            TDuration::Seconds(RequestTimer.Passed()));
    }
    return false;
}

void TGRpcRequestImplBase::OnFinish(EQueueEventStatus evStatus) {
    if (Context.IsCancelled()) {
        ClientLost_.store(true);
        FinishPromise_.SetValue(EFinishStatus::CANCEL);
    } else {
        FinishPromise_.SetValue(evStatus == EQueueEventStatus::OK ? EFinishStatus::OK : EFinishStatus::ERROR);
    }
}

bool TGRpcRequestImplBase::IncRequest() {
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

void TGRpcRequestImplBase::DecRequest() {
    if (RequestLimiter_) {
        RequestLimiter_->DecRequest();
    }
    Server_->DecRequest();
}

} // namespace NYdbGrpc
