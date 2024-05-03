#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/grpc/server/event_callback.h>
#include <ydb/library/grpc/server/grpc_async_ctx_base.h>
#include <ydb/library/grpc/server/grpc_counters.h>
#include <ydb/library/grpc/server/grpc_request_base.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/grpc/include/grpcpp/support/async_stream.h>
#include <contrib/libs/grpc/include/grpcpp/support/async_unary_call.h>

#include <atomic>

namespace NKikimr {
namespace NGRpcServer {

/**
 * Context for performing operations on a bidirectional stream
 *
 * Only one thread is allowed to call methods on this class at any time
 */
template<class TIn, class TOut>
class IGRpcStreamingContext : public TThrRefBase {
public:
    using ISelf = IGRpcStreamingContext<TIn, TOut>;

    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_GRPC_STREAMING),

        EvReadFinished,
        EvWriteFinished,
        EvNotifiedWhenDone,
    };

    struct TEvReadFinished : public TEventLocal<TEvReadFinished, EvReadFinished> {
        TIn Record;
        bool Success;
    };

    struct TEvWriteFinished : public TEventLocal<TEvWriteFinished, EvWriteFinished> {
        bool Success;
    };

    struct TEvNotifiedWhenDone : public TEventLocal<TEvNotifiedWhenDone, EvNotifiedWhenDone> {
        bool Success;

        explicit TEvNotifiedWhenDone(bool success)
            : Success(success)
        { }
    };

public:
    virtual ~IGRpcStreamingContext() = default;

public:
    /**
     * Asynchronously cancels the request
     *
     * Pending read or write operations may or may not be dropped
     */
    virtual void Cancel() = 0;

    /**
     * Attaches actor to this context
     *
     * This must be called before any Read or Write calls
     */
    virtual void Attach(TActorId actor) = 0;

    /**
     * Schedules the next message read
     *
     * May be called multiple times, in which case multiple reads will be
     * scheduled and processed in order. Each Read will result in a
     * corresponding TEvReadFinished event.
     */
    virtual bool Read() = 0;

    /**
     * Schedules the next message write
     *
     * May be called multiple times, in which case multiple writes will be
     * scheduled and processed in order. Each Write will result in a
     * corresponding TEvWriteFinished event.
     */
    virtual bool Write(TOut&& message, const grpc::WriteOptions& options = { }) = 0;

    /**
     * Schedules stream termination with the specified status
     *
     * Only the first call is accepted, after which new Read or Write calls
     * are no longer permitted and ignored.
     */
    virtual bool Finish(const grpc::Status& status) = 0;

    /**
     * Schedules the next message write combined with the status
     *
     * This is similar to Write and Finish combined into a more efficient call.
     */
    virtual bool WriteAndFinish(TOut&& message, const grpc::Status& status) = 0;

    /**
     * Schedules the next message write combined with the status
     *
     * This is similar to Write and Finish combined into a more efficient call.
     */
    virtual bool WriteAndFinish(TOut&& message, const grpc::WriteOptions& options, const grpc::Status& status) = 0;

public:
    virtual NYdbGrpc::TAuthState& GetAuthState() const = 0;
    virtual TString GetPeerName() const = 0;
    virtual TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const = 0;
    virtual grpc_compression_level GetCompressionLevel() const = 0;
    virtual void UseDatabase(const TString& database) = 0;
};

template<class TIn, class TOut, class TServer, int LoggerServiceId>
class TGRpcStreamingRequest final
    : public TThrRefBase
    , private NYdbGrpc::TBaseAsyncContext<TServer>
{
    using TSelf = TGRpcStreamingRequest<TIn, TOut, TServer, LoggerServiceId>;

public:
    using IContext = IGRpcStreamingContext<TIn, TOut>;

    using TAsyncService = typename TServer::TCurrentGRpcService::AsyncService;

    using TAcceptRequest = void (TAsyncService::*)(
        grpc::ServerContext*,
        grpc::ServerAsyncReaderWriter<TOut, TIn>*,
        grpc::CompletionQueue*,
        grpc::ServerCompletionQueue*,
        void*);

    using TAcceptCallback = std::function<void(TIntrusivePtr<IContext>)>;

    ~TGRpcStreamingRequest() {
        if (Flags.load() & FlagRegistered) {
            // This is safe to call from destructor
            Server->DeregisterRequestCtx(this);
        }
    }

    static bool Start(
        TServer* server,
        TAsyncService* service,
        grpc::ServerCompletionQueue* cq,
        TAcceptRequest acceptRequest,
        TAcceptCallback acceptCallback,
        NActors::TActorSystem& actorSystem,
        const char* name,
        NYdbGrpc::ICounterBlockPtr counters = nullptr,
        NYdbGrpc::IGRpcRequestLimiterPtr limiter = nullptr)
    {
        TIntrusivePtr<TSelf> self(new TSelf(
            server,
            service,
            cq,
            acceptRequest,
            std::move(acceptCallback),
            actorSystem,
            name,
            std::move(counters),
            limiter));
        return self->Start();
    }

private:
    TGRpcStreamingRequest(
            TServer* server,
            TAsyncService* service,
            grpc::ServerCompletionQueue* cq,
            TAcceptRequest acceptRequest,
            TAcceptCallback acceptCallback,
            NActors::TActorSystem& as,
            const char* name,
            NYdbGrpc::ICounterBlockPtr counters,
            NYdbGrpc::IGRpcRequestLimiterPtr limiter)
        : NYdbGrpc::TBaseAsyncContext<TServer>(service, cq)
        , Server(server)
        , AcceptRequest(acceptRequest)
        , AcceptCallback(acceptCallback)
        , ActorSystem(as)
        , Name(name)
        , Counters(std::move(counters))
        , Limiter(limiter)
        , AuthState(Server->NeedAuth())
        , Stream(&this->Context)
    { }

    void MaybeClone() {
        if (!Server->IsShuttingDown()) {
            Start(Server, this->Service, this->CQ, AcceptRequest, AcceptCallback, ActorSystem, Name, Counters->Clone(), Limiter);
        }
    }

    bool Start() {
        if (auto guard = Server->ProtectShutdown()) {
            this->Context.AsyncNotifyWhenDone(OnDoneTag.Prepare());
            (this->Service->*AcceptRequest)(&this->Context, &this->Stream, this->CQ, this->CQ, OnAcceptedTag.Prepare());
            return true;
        }

        return false;
    }

    void OnAccepted(NYdbGrpc::EQueueEventStatus status) {
        MaybeClone();

        if (!this->Context.c_call()) {
            // Request dropped before it could start
            Y_ABORT_UNLESS(status == NYdbGrpc::EQueueEventStatus::ERROR);
            // Drop extra reference by OnDoneTag
            this->UnRef();
            // We will be freed after return
            return;
        }

        LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] stream accepted Name# %s ok# %s peer# %s",
            this, Name,
            status == NYdbGrpc::EQueueEventStatus::OK ? "true" : "false",
            this->GetPeerName().c_str());

        if (status == NYdbGrpc::EQueueEventStatus::ERROR) {
            // Don't bother registering if accept failed
            if (Counters) {
                Counters->CountNotOkRequest();
            }
            this->Context.TryCancel();
            return;
        }

        if (!Server->RegisterRequestCtx(this)) {
            // It's unsafe to send replies, so just cancel the request
            LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] dropping request Name# %s due to shutdown",
                this, Name);
            this->Context.TryCancel();
            return;
        }

        Flags |= FlagRegistered;

        if (IncRequest()) {
            if (Counters) {
                Counters->StartProcessing(0);
            }
            Flags |= FlagStarted;

            RequestTimer.Reset();

            TIntrusivePtr<IContext> facade(new TFacade(this));
            AcceptCallback(std::move(facade));
        } else {
            // There is no facade, so after finish completes we will be freed
            FinishInternal(grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED, "Too many requests"));
        }
    }

    void OnDone(NYdbGrpc::EQueueEventStatus status) {
        LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] stream done notification Name# %s ok# %s peer# %s",
            this, Name,
            status == NYdbGrpc::EQueueEventStatus::OK ? "true" : "false",
            this->GetPeerName().c_str());

        bool success = status == NYdbGrpc::EQueueEventStatus::OK;

        auto flags = Flags.load(std::memory_order_acquire);
        auto added = FlagDoneCalled | (success ? FlagDoneSuccess : 0);
        bool attached;
        do {
            Y_DEBUG_ABORT_UNLESS(!(flags & FlagDoneCalled), "OnDone called more than once");
            attached = flags & FlagAttached;
        } while (!Flags.compare_exchange_weak(flags, flags | added, std::memory_order_acq_rel));

        if (attached) {
            ActorSystem.Send(Actor, new typename IContext::TEvNotifiedWhenDone(success));
        }
    }

    void Cancel() {
        LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] facade cancel Name# %s peer# %s",
            this, Name,
            this->GetPeerName().c_str());

        this->Context.TryCancel();
    }

    void Detach() {
        FinishInternal(grpc::Status(grpc::StatusCode::CANCELLED, "Request abandoned"));
    }

    void Attach(TActorId actor) {
        LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] facade attach Name# %s actor# %s peer# %s",
            this, Name,
            actor.ToString().c_str(),
            this->GetPeerName().c_str());

        auto guard = SingleThreaded.Enforce();

        bool doneCalled;
        bool doneSuccess;

        auto flags = Flags.load(std::memory_order_acquire);
        auto added = FlagAttached;
        do {
            Y_ABORT_UNLESS(!(flags & FlagAttached), "Attach cannot be called more than once");
            Actor = actor;
            doneCalled = flags & FlagDoneCalled;
            doneSuccess = flags & FlagDoneSuccess;
        } while (!Flags.compare_exchange_weak(flags, flags | added, std::memory_order_acq_rel));

        if (doneCalled) {
            ActorSystem.Send(actor, new typename IContext::TEvNotifiedWhenDone(doneSuccess));
        }
    }

    bool Read() {
        LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] facade read Name# %s peer# %s",
            this, Name,
            this->GetPeerName().c_str());

        auto guard = SingleThreaded.Enforce();

        auto flags = Flags.load(std::memory_order_acquire);
        Y_ABORT_UNLESS(flags & FlagAttached, "Read cannot be called before Attach");

        if (flags & FlagFinishCalled) {
            return false;
        }

        if (Y_LIKELY(0 == ReadQueue++)) {
            // This is the first read, start reading from the stream
            Y_ABORT_UNLESS(!ReadInProgress);
            ReadInProgress = MakeHolder<typename IContext::TEvReadFinished>();
            Stream.Read(&ReadInProgress->Record, OnReadDoneTag.Prepare());
        } else {
            Y_DEBUG_ABORT("Multiple outstanding reads are unsafe in grpc streaming");
        }

        return true;
    }

    void OnReadDone(NYdbGrpc::EQueueEventStatus status) {
        LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] read finished Name# %s ok# %s data# %s peer# %s",
            this, Name,
            status == NYdbGrpc::EQueueEventStatus::OK ? "true" : "false",
            NYdbGrpc::FormatMessage<TIn>(ReadInProgress->Record, status == NYdbGrpc::EQueueEventStatus::OK).c_str(),
            this->GetPeerName().c_str());

        // Take current in-progress read first
        auto read = std::move(ReadInProgress);
        Y_DEBUG_ABORT_UNLESS(read && !ReadInProgress);

        // Decrement read counter before sending reply
        auto was = ReadQueue--;
        Y_DEBUG_ABORT_UNLESS(was > 0);

        read->Success = status == NYdbGrpc::EQueueEventStatus::OK;
        if (Counters && read->Success) {
            Counters->CountRequestBytes(read->Record.ByteSize());
        }
        ActorSystem.Send(Actor, read.Release());

        if (Y_LIKELY(was == 1)) {
            // This was the last read, check if write side has already finished
            auto flags = Flags.load(std::memory_order_acquire);
            while ((flags & FlagRegistered) && (flags & FlagFinishDone) && ReadQueue.load() == 0) {
                Y_DEBUG_ABORT_UNLESS(flags & FlagFinishCalled);
                if (Flags.compare_exchange_weak(flags, flags & ~FlagRegistered, std::memory_order_acq_rel)) {
                    LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] deregistering request Name# %s peer# %s (read done)",
                        this, Name, this->GetPeerName().c_str());
                    Server->DeregisterRequestCtx(this);
                    break;
                }
            }
        } else {
            // We need to perform another read (likely unsafe)
            Y_DEBUG_ABORT("Multiple outstanding reads are unsafe in grpc streaming");
            ReadInProgress = MakeHolder<typename IContext::TEvReadFinished>();
            Stream.Read(&ReadInProgress->Record, OnReadDoneTag.Prepare());
        }
    }

    bool Write(TOut&& message, const grpc::WriteOptions& options = { }, const grpc::Status* status = nullptr) {
        if (status) {
            LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] facade write Name# %s data# %s peer# %s grpc status# (%d) message# %s",
                this, Name,
                NYdbGrpc::FormatMessage<TOut>(message).c_str(),
                this->GetPeerName().c_str(),
                static_cast<int>(status->error_code()),
                status->error_message().c_str());
        } else {
            LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] facade write Name# %s data# %s peer# %s",
                this, Name,
                NYdbGrpc::FormatMessage<TOut>(message).c_str(),
                this->GetPeerName().c_str());
        }

        Y_ABORT_UNLESS(!options.is_corked(),
            "WriteOptions::set_corked() is unsupported");
        Y_ABORT_UNLESS(!options.get_buffer_hint(),
            "WriteOptions::set_buffer_hint() is unsupported");
        Y_ABORT_UNLESS(!options.is_last_message(),
            "WriteOptions::set_last_message() is unsupported");

        auto guard = SingleThreaded.Enforce();

        with_lock (WriteLock) {
            auto flags = Flags.load(std::memory_order_acquire);
            Y_ABORT_UNLESS(flags & FlagAttached, "Write cannot be called before Attach");

            if (flags & FlagFinishCalled) {
                return false;
            }

            if (Counters) {
                Counters->CountResponseBytes(message.ByteSize());
            }

            if (status) {
                Status = *status;
                flags = (Flags |= FlagFinishCalled);
            }

            if (flags & FlagWriteActive) {
                auto queued = MakeHolder<TWriteItem>();
                queued->Message.Swap(&message);
                queued->Options = options;
                WriteQueue.push_back(std::move(queued));
                return true;
            }

            Y_DEBUG_ABORT_UNLESS(!WriteQueue);
            Y_DEBUG_ABORT_UNLESS(!(flags & FlagWriteAndFinish));

            Flags |= (FlagWriteActive | (status ? FlagWriteAndFinish : 0));
        }

        if (status) {
            Stream.WriteAndFinish(message, options, *status, OnWriteDoneTag.Prepare());
        } else {
            Stream.Write(message, options, OnWriteDoneTag.Prepare());
        }
        return true;
    }

    void OnWriteDone(NYdbGrpc::EQueueEventStatus status) {
        LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] write finished Name# %s ok# %s peer# %s",
            this, Name,
            status == NYdbGrpc::EQueueEventStatus::OK ? "true" : "false",
            this->GetPeerName().c_str());

        auto event = MakeHolder<typename IContext::TEvWriteFinished>();
        event->Success = status == NYdbGrpc::EQueueEventStatus::OK;
        ActorSystem.Send(Actor, event.Release());

        THolder<TWriteItem> next;
        const grpc::Status* nextStatus = nullptr;
        bool wasWriteAndFinish;

        with_lock (WriteLock) {
            auto flags = Flags.load(std::memory_order_acquire);
            Y_DEBUG_ABORT_UNLESS(flags & FlagWriteActive);
            wasWriteAndFinish = flags & FlagWriteAndFinish;

            if (WriteQueue) {
                Y_DEBUG_ABORT_UNLESS(!wasWriteAndFinish);
                // Take the next item from the queue
                next = std::move(WriteQueue.front());
                WriteQueue.pop_front();
                if (!WriteQueue && (flags & FlagFinishCalled)) {
                    // FlagFinishCalled set during FlagWriteActive
                    // Combine the last message with Finish
                    Y_DEBUG_ABORT_UNLESS(!(flags & FlagFinishDone));
                    Flags |= FlagWriteAndFinish;
                    nextStatus = &*Status;
                }
            } else {
                // Drop the active flag
                do {
                    if (!wasWriteAndFinish && (flags & FlagFinishCalled)) {
                        // FlagFinishCalled set during FlagWriteActive
                        Y_DEBUG_ABORT_UNLESS(!(flags & FlagFinishDone));
                        nextStatus = &*Status;
                    }
                } while (!Flags.compare_exchange_weak(flags, flags & ~FlagWriteActive, std::memory_order_acq_rel));
            }
        }

        if (next && nextStatus) {
            Stream.WriteAndFinish(next->Message, next->Options, *nextStatus, OnWriteDoneTag.Prepare());
        } else if (next) {
            Stream.Write(next->Message, next->Options, OnWriteDoneTag.Prepare());
        } else if (nextStatus) {
            Stream.Finish(*nextStatus, OnFinishDoneTag.Prepare());
        } else if (wasWriteAndFinish) {
            OnFinishDone(status);
        }
    }

    bool Finish(const grpc::Status& status) {
        LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] facade finish Name# %s peer# %s grpc status# (%d) message# %s",
            this, Name,
            this->GetPeerName().c_str(),
            static_cast<int>(status.error_code()),
            status.error_message().c_str());

        return FinishInternal(status);
    }

    bool FinishInternal(const grpc::Status& status) {
        auto guard = SingleThreaded.Enforce();

        auto flags = Flags.load(std::memory_order_acquire);
        if (flags & FlagFinishCalled) {
            return false;
        }

        Status = status;

        bool finish;

        flags = Flags.load(std::memory_order_acquire);
        do {
            Y_DEBUG_ABORT_UNLESS(!(flags & FlagFinishCalled));
            finish = !(flags & FlagWriteActive);
        } while (!Flags.compare_exchange_weak(flags, flags | FlagFinishCalled, std::memory_order_acq_rel));

        if (finish) {
            Stream.Finish(status, OnFinishDoneTag.Prepare());
        }

        return true;
    }

    void OnFinishDone(NYdbGrpc::EQueueEventStatus status) {
        LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] stream finished Name# %s ok# %s peer# %s grpc status# (%d) message# %s",
            this, Name,
            status == NYdbGrpc::EQueueEventStatus::OK ? "true" : "false",
            this->GetPeerName().c_str(),
            static_cast<int>(Status->error_code()),
            Status->error_message().c_str());

        auto flags = (Flags |= FlagFinishDone);
        Y_ABORT_UNLESS(flags & FlagFinishCalled);
        Y_DEBUG_ABORT_UNLESS(!(flags & FlagWriteActive));

        switch (Status->error_code()) {
            case grpc::StatusCode::UNAUTHENTICATED:
                if (Counters) {
                    Counters->CountNotAuthenticated();
                }
                break;
            case grpc::StatusCode::RESOURCE_EXHAUSTED:
                if (Counters) {
                    Counters->CountResourceExhausted();
                }
                break;
            default:
                break;
        }

        if (flags & FlagStarted) {
            Y_ABORT_UNLESS(Status);
            if (Counters) {
                Counters->FinishProcessing(0, 0, Status->ok(), 0, TDuration::Seconds(RequestTimer.Passed()));
            }
            DecRequest();
            flags = Flags.load(std::memory_order_acquire);
        }

        while ((flags & FlagRegistered) && ReadQueue.load() == 0) {
            if (Flags.compare_exchange_weak(flags, flags & ~FlagRegistered, std::memory_order_acq_rel)) {
                LOG_DEBUG(ActorSystem, LoggerServiceId, "[%p] deregistering request Name# %s peer# %s (finish done)",
                    this, Name, this->GetPeerName().c_str());
                Server->DeregisterRequestCtx(this);
                break;
            }
        }
    }

    void UseDatabase(const TString& database) {
        Counters->UseDatabase(database);
    }

private:
    bool IncRequest() {
        if (Limiter) {
            return Limiter->IncRequest();
        }
        return true;
    }

    void DecRequest() {
        if (Limiter) {
            Limiter->DecRequest();
        }
    }

private:
    class TFacade : public IContext {
    public:
        explicit TFacade(TSelf* self)
            : Self(self)
        { }

        ~TFacade() {
            Self->Detach();
        }

        void Cancel() override {
            return Self->Cancel();
        }

        void Attach(TActorId actor) override {
            Self->Attach(actor);
        }

        bool Read() override {
            return Self->Read();
        }

        bool Write(TOut&& msg, const grpc::WriteOptions& options) override {
            return Self->Write(std::move(msg), options);
        }

        bool WriteAndFinish(TOut&& msg, const grpc::Status& status) override {
            return Self->Write(std::move(msg), { }, &status);
        }

        bool WriteAndFinish(TOut&& msg, const grpc::WriteOptions& options, const grpc::Status& status) override {
            return Self->Write(std::move(msg), options, &status);
        }

        bool Finish(const grpc::Status& status) override {
            return Self->Finish(status);
        }

        NYdbGrpc::TAuthState& GetAuthState() const override {
            return Self->AuthState;
        }

        TString GetPeerName() const override {
            return Self->GetPeerName();
        }

        TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override {
            return Self->GetPeerMetaValues(key);
        }

        grpc_compression_level GetCompressionLevel() const override {
            return Self->GetCompressionLevel();
        }

        void UseDatabase(const TString& database) override {
            Self->UseDatabase(database);
        }

    private:
        TIntrusivePtr<TSelf> Self;
    };

    class TSingleThreaded {
    public:
        class TSingleThreadedGuard {
        public:
            explicit TSingleThreadedGuard(TSingleThreaded* owner)
                : Owner(nullptr)
            {
                size_t threads;
                Y_DEBUG_ABORT_UNLESS((threads = owner->Threads.fetch_add(1, std::memory_order_acquire)) == 0,
                    "Detected usage from %" PRISZT " threads", threads + 1);
                Owner = owner;
            }

            TSingleThreadedGuard(TSingleThreadedGuard&& other)
                : Owner(other.Owner)
            {
                other.Owner = nullptr;
            }

            ~TSingleThreadedGuard() {
                if (Owner) {
                    size_t threads;
                    Y_DEBUG_ABORT_UNLESS((threads = Owner->Threads.fetch_sub(1, std::memory_order_release)) == 1,
                        "Detected usage from %" PRISZT " threads", threads);
                }
            }

            TSingleThreadedGuard(const TSingleThreadedGuard&) = delete;
            TSingleThreadedGuard& operator=(const TSingleThreadedGuard&) = delete;

        private:
            TSingleThreaded* Owner;
        };

    public:
        TSingleThreadedGuard Enforce() {
            return TSingleThreadedGuard(this);
        }

    private:
        std::atomic<size_t> Threads{ 0 };
    };

    struct TWriteItem {
        TOut Message;
        grpc::WriteOptions Options;
    };

    enum : intptr_t {
        FlagRegistered = 1 << 0,
        FlagStarted = 1 << 1,
        FlagDoneCalled = 1 << 2,
        FlagDoneSuccess = 1 << 3,
        FlagAttached = 1 << 4,
        FlagWriteActive = 1 << 5,
        FlagWriteAndFinish = 1 << 6,
        FlagFinishCalled = 1 << 7,
        FlagFinishDone = 1 << 8,
    };

private:
    TServer* const Server;
    TAcceptRequest const AcceptRequest;
    TAcceptCallback const AcceptCallback;
    NActors::TActorSystem& ActorSystem;
    const char* const Name;
    NYdbGrpc::ICounterBlockPtr const Counters;
    NYdbGrpc::IGRpcRequestLimiterPtr Limiter;

    NYdbGrpc::TAuthState AuthState;
    grpc::ServerAsyncReaderWriter<TOut, TIn> Stream;
    TSingleThreaded SingleThreaded;
    THPTimer RequestTimer;

    std::atomic<intptr_t> Flags{ 0 };

    TActorId Actor;

    std::atomic<size_t> ReadQueue{ 0 };
    THolder<typename IContext::TEvReadFinished> ReadInProgress;

    TMutex WriteLock;
    TDeque<THolder<TWriteItem>> WriteQueue;

    TMaybe<grpc::Status> Status;

    using TFixedEvent = NYdbGrpc::TQueueFixedEvent<TSelf>;
    TFixedEvent OnDoneTag = { this, &TSelf::OnDone };
    TFixedEvent OnAcceptedTag = { this, &TSelf::OnAccepted };
    TFixedEvent OnReadDoneTag = { this, &TSelf::OnReadDone };
    TFixedEvent OnWriteDoneTag = { this, &TSelf::OnWriteDone };
    TFixedEvent OnFinishDoneTag = { this, &TSelf::OnFinishDone };
};

}
}
