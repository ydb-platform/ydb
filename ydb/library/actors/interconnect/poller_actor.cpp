#include "poller_actor.h"
#include "interconnect_common.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/probes.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/library/actors/util/funnel_queue.h>

#include <util/generic/intrlist.h>
#include <util/system/thread.h>
#include <util/system/event.h>
#include <util/system/pipe.h>

#include <variant>

namespace NActors {

    LWTRACE_USING(ACTORLIB_PROVIDER);

    namespace {
        int LastSocketError() {
#if defined(_win_)
            return WSAGetLastError();
#else
            return errno;
#endif
        }
    }

    struct TSocketRecord : TThrRefBase {
        const TIntrusivePtr<TSharedDescriptor> Socket;
        const TActorId ReadActorId;
        const TActorId WriteActorId;
        std::atomic_uint32_t Flags = 0;

        TSocketRecord(TEvPollerRegister& ev)
            : Socket(std::move(ev.Socket))
            , ReadActorId(ev.ReadActorId)
            , WriteActorId(ev.WriteActorId)
        {}
    };

    template<typename TDerived>
    class TPollerThreadBase : public ISimpleThread {
    protected:
        struct TPollerExitThread {}; // issued then we need to terminate the poller thread

        struct TPollerWakeup {};

        struct TPollerUnregisterSocket {
            TIntrusivePtr<TSharedDescriptor> Socket;

            TPollerUnregisterSocket(TIntrusivePtr<TSharedDescriptor> socket)
                : Socket(std::move(socket))
            {}
        };

        using TPollerSyncOperation = std::variant<TPollerExitThread, TPollerWakeup, TPollerUnregisterSocket>;

        struct TPollerSyncOperationWrapper {
            TPollerSyncOperation Operation;
            TManualEvent Event;

            TPollerSyncOperationWrapper(TPollerSyncOperation&& operation)
                : Operation(std::move(operation))
            {}

            void Wait() {
                Event.WaitI();
            }

            void SignalDone() {
                Event.Signal();
            }
        };

        TActorSystem *ActorSystem;
        TPipeHandle ReadEnd, WriteEnd; // pipe for sync event processor
        TFunnelQueue<TPollerSyncOperationWrapper*> SyncOperationsQ; // operation queue

    public:
        TPollerThreadBase(TActorSystem *actorSystem)
            : ActorSystem(actorSystem)
        {
            // create a pipe for notifications
            try {
                TPipeHandle::Pipe(ReadEnd, WriteEnd, CloseOnExec);
            } catch (const TFileError& err) {
                Y_ABORT("failed to create pipe");
            }

            // switch the read/write ends to nonblocking mode
            SetNonBlock(ReadEnd);
            SetNonBlock(WriteEnd);
        }

        void UnregisterSocket(const TIntrusivePtr<TSocketRecord>& record) {
            ExecuteSyncOperation(TPollerUnregisterSocket(record->Socket));
        }

    protected:
        void Notify(TSocketRecord *record, bool read, bool write) {
            auto issue = [&](const TActorId& recipient) {
                ActorSystem->Send(new IEventHandle(recipient, {}, new TEvPollerReady(record->Socket, read, write)));
            };
            if (read && record->ReadActorId) {
                issue(record->ReadActorId);
                if (write && record->WriteActorId && record->WriteActorId != record->ReadActorId) {
                    issue(record->WriteActorId);
                }
            } else if (write && record->WriteActorId) {
                issue(record->WriteActorId);
            }
        }

        void Stop() {
            // signal poller thread to stop and wait for the thread
            ExecuteSyncOperation(TPollerExitThread());
            ISimpleThread::Join();
        }

        void ExecuteSyncOperation(TPollerSyncOperation&& op) {
            TPollerSyncOperationWrapper wrapper(std::move(op));
            if (SyncOperationsQ.Push(&wrapper)) {
                // this was the first entry, so we push notification through the pipe
                for (;;) {
                    char buffer = '\x00';
                    ssize_t nwritten = WriteEnd.Write(&buffer, sizeof(buffer));
                    if (nwritten < 0) {
                        const int err = LastSocketError();
                        if (err == EINTR) {
                            continue;
                        } else {
                            Y_ABORT("WriteEnd.Write() failed with %s", strerror(err));
                        }
                    } else {
                        Y_ABORT_UNLESS(nwritten);
                        break;
                    }
                }
            }
            // wait for operation to complete
            wrapper.Wait();
        }

        void DrainReadEnd() {
            char buffer[4096];
            for (;;) {
                ssize_t n = ReadEnd.Read(buffer, sizeof(buffer));
                if (n < 0) {
                    const int error = LastSocketError();
                    if (error == EINTR) {
                        continue;
                    } else if (error == EAGAIN || error == EWOULDBLOCK) {
                        break;
                    } else {
                        Y_ABORT("read() failed with %s", strerror(errno));
                    }
                } else {
                    Y_ABORT_UNLESS(n);
                }
            }
        }

        bool ProcessSyncOpQueue() {
            Y_ABORT_UNLESS(!SyncOperationsQ.IsEmpty());
            do {
                TPollerSyncOperationWrapper *op = SyncOperationsQ.Top();
                if (auto *unregister = std::get_if<TPollerUnregisterSocket>(&op->Operation)) {
                    static_cast<TDerived&>(*this).UnregisterSocketInLoop(unregister->Socket);
                    op->SignalDone();
                } else if (std::get_if<TPollerExitThread>(&op->Operation)) {
                    op->SignalDone();
                    return false; // terminate the thread
                } else if (std::get_if<TPollerWakeup>(&op->Operation)) {
                    op->SignalDone();
                } else {
                    Y_ABORT();
                }
            } while (SyncOperationsQ.Pop());
            return true;
        }

        void *ThreadProc() override {
            SetCurrentThreadName("network poller");
            for (;;) {
                if (static_cast<TDerived&>(*this).ProcessEventsInLoop()) { // need to process the queue
                    DrainReadEnd();
                    if (!ProcessSyncOpQueue()) {
                        break;
                    }
                }
            }
            return nullptr;
        }
    };

} // namespace NActors

#if defined(_linux_)
#   include "poller_actor_linux.h"
#elif defined(_darwin_)
#   include "poller_actor_darwin.h"
#elif defined(_win_)
#   include "poller_actor_win.h"
#else
#   error "Unsupported platform"
#endif

namespace NActors {

    class TPollerToken::TImpl {
        std::weak_ptr<TPollerThread> Thread;
        TIntrusivePtr<TSocketRecord> Record; // valid only when Thread is held locked

    public:
        TImpl(std::shared_ptr<TPollerThread> thread, TIntrusivePtr<TSocketRecord> record)
            : Thread(thread)
            , Record(std::move(record))
        {
            thread->RegisterSocket(Record);
        }

        ~TImpl() {
            if (auto thread = Thread.lock()) {
                thread->UnregisterSocket(Record);
            }
        }

        void Request(bool read, bool write) {
            if (auto thread = Thread.lock()) {
                thread->Request(Record, read, write, false, false);
            }
        }

        bool RequestReadNotificationAfterWouldBlock() {
            if (auto thread = Thread.lock()) {
                return thread->Request(Record, true, false, true, true);
            } else {
                return false;
            }
        }

        bool RequestWriteNotificationAfterWouldBlock() {
            if (auto thread = Thread.lock()) {
                return thread->Request(Record, false, true, true, true);
            } else {
                return false;
            }
        }

        const TIntrusivePtr<TSharedDescriptor>& Socket() const {
            return Record->Socket;
        }
    };

    class TPollerActor: public TActorBootstrapped<TPollerActor> {
        // poller thread
        std::shared_ptr<TPollerThread> PollerThread;

    public:
        static constexpr IActor::EActivityType ActorActivityType() {
            return IActor::EActivityType::INTERCONNECT_POLLER;
        }

        void Bootstrap() {
            PollerThread = std::make_shared<TPollerThread>(TlsActivationContext->ExecutorThread.ActorSystem);
            Become(&TPollerActor::StateFunc);
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvPollerRegister, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )

        void Handle(TEvPollerRegister::TPtr& ev) {
            auto *msg = ev->Get();
            auto impl = std::make_unique<TPollerToken::TImpl>(PollerThread, MakeIntrusive<TSocketRecord>(*msg));
            auto socket = impl->Socket();
            TPollerToken::TPtr token(new TPollerToken(std::move(impl)));
            if (msg->ReadActorId && msg->WriteActorId && msg->WriteActorId != msg->ReadActorId) {
                Send(msg->ReadActorId, new TEvPollerRegisterResult(socket, token));
                Send(msg->WriteActorId, new TEvPollerRegisterResult(socket, std::move(token)));
            } else if (msg->ReadActorId) {
                Send(msg->ReadActorId, new TEvPollerRegisterResult(socket, std::move(token)));
            } else if (msg->WriteActorId) {
                Send(msg->WriteActorId, new TEvPollerRegisterResult(socket, std::move(token)));
            }
        }
    };

    TPollerToken::TPollerToken(std::unique_ptr<TImpl> impl)
        : Impl(std::move(impl))
    {}

    TPollerToken::~TPollerToken()
    {}

    void TPollerToken::Request(bool read, bool write) {
        Impl->Request(read, write);
    }

    bool TPollerToken::RequestReadNotificationAfterWouldBlock() {
        return Impl->RequestReadNotificationAfterWouldBlock();
    }

    bool TPollerToken::RequestWriteNotificationAfterWouldBlock() {
        return Impl->RequestWriteNotificationAfterWouldBlock();
    }

    IActor* CreatePollerActor() {
        return new TPollerActor();
    }

}
