#include "event_loop.h"

#include "network.h"
#include "thread_extra.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/hash.h>
#include <util/network/pair.h>
#include <util/network/poller.h>
#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/system/yassert.h>
#include <util/thread/lfqueue.h>

#include <errno.h>

using namespace NEventLoop;

namespace {
    enum ERunningState {
        EVENT_LOOP_CREATED,
        EVENT_LOOP_RUNNING,
        EVENT_LOOP_STOPPED,
    };

    enum EOperation {
        OP_READ = 1,
        OP_WRITE = 2,
        OP_READ_WRITE = OP_READ | OP_WRITE,
    };
}

class TChannel::TImpl {
public:
    TImpl(TEventLoop::TImpl* eventLoop, TSocket socket, TEventHandlerPtr, void* cookie);
    ~TImpl();

    void EnableRead();
    void DisableRead();
    void EnableWrite();
    void DisableWrite();

    void Unregister();

    SOCKET GetSocket() const;
    TSocket GetSocketPtr() const;

    void Update(int pollerFlags, bool enable);
    void CallHandler();

    TEventLoop::TImpl* EventLoop;
    TSocket Socket;
    TEventHandlerPtr EventHandler;
    void* Cookie;

    TMutex Mutex;

    int CurrentFlags;
    bool Close;
};

class TEventLoop::TImpl {
public:
    TImpl(const char* name);

    void Run();
    void Wakeup();
    void Stop();

    TChannelPtr Register(TSocket socket, TEventHandlerPtr eventHandler, void* cookie);
    void Unregister(SOCKET socket);

    typedef THashMap<SOCKET, TChannelPtr> TData;

    void AddToPoller(SOCKET socket, void* cookie, int flags);

    TMutex Mutex;

    const char* Name;

    TAtomic RunningState;
    TAtomic StopSignal;
    TSystemEvent StoppedEvent;
    TData Data;

    TLockFreeQueue<SOCKET> SocketsToRemove;

    TSocketPoller Poller;
    TSocketHolder WakeupReadSocket;
    TSocketHolder WakeupWriteSocket;
};

TChannel::~TChannel() {
}

void TChannel::EnableRead() {
    Impl->EnableRead();
}

void TChannel::DisableRead() {
    Impl->DisableRead();
}

void TChannel::EnableWrite() {
    Impl->EnableWrite();
}

void TChannel::DisableWrite() {
    Impl->DisableWrite();
}

void TChannel::Unregister() {
    Impl->Unregister();
}

SOCKET TChannel::GetSocket() const {
    return Impl->GetSocket();
}

TSocket TChannel::GetSocketPtr() const {
    return Impl->GetSocketPtr();
}

TChannel::TChannel(TImpl* impl)
    : Impl(impl)
{
}

TEventLoop::TEventLoop(const char* name)
    : Impl(new TImpl(name))
{
}

TEventLoop::~TEventLoop() {
}

void TEventLoop::Run() {
    Impl->Run();
}

void TEventLoop::Stop() {
    Impl->Stop();
}

bool TEventLoop::IsRunning() {
    return AtomicGet(Impl->RunningState) == EVENT_LOOP_RUNNING;
}

TChannelPtr TEventLoop::Register(TSocket socket, TEventHandlerPtr eventHandler, void* cookie) {
    return Impl->Register(socket, eventHandler, cookie);
}

TChannel::TImpl::TImpl(TEventLoop::TImpl* eventLoop, TSocket socket, TEventHandlerPtr eventHandler, void* cookie)
    : EventLoop(eventLoop)
    , Socket(socket)
    , EventHandler(eventHandler)
    , Cookie(cookie)
    , CurrentFlags(0)
    , Close(false)
{
}

TChannel::TImpl::~TImpl() {
    Y_ASSERT(Close);
}

void TChannel::TImpl::EnableRead() {
    Update(OP_READ, true);
}

void TChannel::TImpl::DisableRead() {
    Update(OP_READ, false);
}

void TChannel::TImpl::EnableWrite() {
    Update(OP_WRITE, true);
}

void TChannel::TImpl::DisableWrite() {
    Update(OP_WRITE, false);
}

void TChannel::TImpl::Unregister() {
    TGuard<TMutex> guard(Mutex);

    if (Close) {
        return;
    }

    Close = true;
    if (CurrentFlags != 0) {
        EventLoop->Poller.Unwait(Socket);
        CurrentFlags = 0;
    }
    EventHandler.Drop();

    EventLoop->SocketsToRemove.Enqueue(Socket);
    EventLoop->Wakeup();
}

void TChannel::TImpl::Update(int flags, bool enable) {
    TGuard<TMutex> guard(Mutex);

    if (Close) {
        return;
    }

    int newFlags = enable ? (CurrentFlags | flags) : (CurrentFlags & ~flags);

    if (CurrentFlags == newFlags) {
        return;
    }

    if (!newFlags) {
        EventLoop->Poller.Unwait(Socket);
    } else {
        void* cookie = reinterpret_cast<void*>(this);
        EventLoop->AddToPoller(Socket, cookie, newFlags);
    }

    CurrentFlags = newFlags;
}

SOCKET TChannel::TImpl::GetSocket() const {
    return Socket;
}

TSocket TChannel::TImpl::GetSocketPtr() const {
    return Socket;
}

void TChannel::TImpl::CallHandler() {
    TEventHandlerPtr handler;

    {
        TGuard<TMutex> guard(Mutex);

        // other thread may have re-added socket to epoll
        // so even if CurrentFlags is 0, epoll may fire again
        // so please use non-blocking operations
        CurrentFlags = 0;

        if (Close) {
            return;
        }

        handler = EventHandler;
    }

    if (!!handler) {
        handler->HandleEvent(Socket, Cookie);
    }
}

TEventLoop::TImpl::TImpl(const char* name)
    : Name(name)
    , RunningState(EVENT_LOOP_CREATED)
    , StopSignal(0)
{
    SOCKET wakeupSockets[2];

    if (SocketPair(wakeupSockets) < 0) {
        Y_ABORT("failed to create socket pair for wakeup sockets: %s", LastSystemErrorText());
    }

    TSocketHolder wakeupReadSocket(wakeupSockets[0]);
    TSocketHolder wakeupWriteSocket(wakeupSockets[1]);

    WakeupReadSocket.Swap(wakeupReadSocket);
    WakeupWriteSocket.Swap(wakeupWriteSocket);

    SetNonBlock(WakeupWriteSocket, true);
    SetNonBlock(WakeupReadSocket, true);

    Poller.WaitRead(WakeupReadSocket,
                    reinterpret_cast<void*>(this));
}

void TEventLoop::TImpl::Run() {
    bool res = AtomicCas(&RunningState, EVENT_LOOP_RUNNING, EVENT_LOOP_CREATED);
    Y_ABORT_UNLESS(res, "Invalid mbus event loop state");

    if (!!Name) {
        SetCurrentThreadName(Name);
    }

    while (AtomicGet(StopSignal) == 0) {
        void* cookies[1024];
        const size_t count = Poller.WaitI(cookies, Y_ARRAY_SIZE(cookies));

        void** end = cookies + count;
        for (void** c = cookies; c != end; ++c) {
            TChannel::TImpl* s = reinterpret_cast<TChannel::TImpl*>(*c);

            if (*c == this) {
                char buf[0x1000];
                if (NBus::NPrivate::SocketRecv(WakeupReadSocket, buf) < 0) {
                    Y_ABORT("failed to recv from wakeup socket: %s", LastSystemErrorText());
                }
                continue;
            }

            s->CallHandler();
        }

        SOCKET socket = -1;
        while (SocketsToRemove.Dequeue(&socket)) {
            TGuard<TMutex> guard(Mutex);
            Y_ABORT_UNLESS(Data.erase(socket) == 1, "must be removed once");
        }
    }

    {
        TGuard<TMutex> guard(Mutex);
        for (auto& it : Data) {
            it.second->Unregister();
        }

        // release file descriptors
        Data.clear();
    }

    res = AtomicCas(&RunningState, EVENT_LOOP_STOPPED, EVENT_LOOP_RUNNING);

    Y_ABORT_UNLESS(res);

    StoppedEvent.Signal();
}

void TEventLoop::TImpl::Stop() {
    AtomicSet(StopSignal, 1);

    if (AtomicGet(RunningState) == EVENT_LOOP_RUNNING) {
        Wakeup();

        StoppedEvent.WaitI();
    }
}

TChannelPtr TEventLoop::TImpl::Register(TSocket socket, TEventHandlerPtr eventHandler, void* cookie) {
    Y_ABORT_UNLESS(socket != INVALID_SOCKET, "must be a valid socket");

    TChannelPtr channel = new TChannel(new TChannel::TImpl(this, socket, eventHandler, cookie));

    TGuard<TMutex> guard(Mutex);

    Y_ABORT_UNLESS(Data.insert(std::make_pair(socket, channel)).second, "must not be already inserted");

    return channel;
}

void TEventLoop::TImpl::Wakeup() {
    if (NBus::NPrivate::SocketSend(WakeupWriteSocket, TArrayRef<const char>("", 1)) < 0) {
        if (LastSystemError() != EAGAIN) {
            Y_ABORT("failed to send to wakeup socket: %s", LastSystemErrorText());
        }
    }
}

void TEventLoop::TImpl::AddToPoller(SOCKET socket, void* cookie, int flags) {
    if (flags == OP_READ) {
        Poller.WaitReadOneShot(socket, cookie);
    } else if (flags == OP_WRITE) {
        Poller.WaitWriteOneShot(socket, cookie);
    } else if (flags == OP_READ_WRITE) {
        Poller.WaitReadWriteOneShot(socket, cookie);
    } else {
        Y_ABORT("Wrong flags: %d", int(flags));
    }
}
