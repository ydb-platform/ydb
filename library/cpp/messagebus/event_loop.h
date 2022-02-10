#pragma once

#include <util/generic/object_counter.h>
#include <util/generic/ptr.h>
#include <util/network/init.h>
#include <util/network/socket.h>

namespace NEventLoop {
    struct IEventHandler
       : public TAtomicRefCount<IEventHandler> {
        virtual void HandleEvent(SOCKET socket, void* cookie) = 0;
        virtual ~IEventHandler() {
        }
    };

    typedef TIntrusivePtr<IEventHandler> TEventHandlerPtr;

    class TEventLoop;

    // TODO: make TChannel itself a pointer
    // to avoid confusion with Drop and Unregister
    class TChannel
       : public TAtomicRefCount<TChannel> {
    public:
        ~TChannel();

        void EnableRead();
        void DisableRead();
        void EnableWrite();
        void DisableWrite();

        void Unregister();

        SOCKET GetSocket() const;
        TSocket GetSocketPtr() const;

    private:
        class TImpl;
        friend class TEventLoop;

        TObjectCounter<TChannel> ObjectCounter;

        TChannel(TImpl*);

    private:
        THolder<TImpl> Impl;
    };

    typedef TIntrusivePtr<TChannel> TChannelPtr;

    class TEventLoop {
    public:
        TEventLoop(const char* name = nullptr);
        ~TEventLoop();

        void Run();
        void Stop();
        bool IsRunning();

        TChannelPtr Register(TSocket socket, TEventHandlerPtr, void* cookie = nullptr);

    private:
        class TImpl;
        friend class TChannel;

        TObjectCounter<TEventLoop> ObjectCounter;

    private:
        THolder<TImpl> Impl;
    };

}
