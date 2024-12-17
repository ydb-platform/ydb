#include "listener.h"
#include "connection.h"
#include "private.h"

#include <yt/yt/core/concurrency/pollable_detail.h>

#include <yt/yt/core/net/socket.h>

#include <yt/yt/core/misc/proc.h>

#include <util/network/pollerimpl.h>

namespace NYT::NNet {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = NetLogger;

////////////////////////////////////////////////////////////////////////////////

class TListener
    : public TPollableBase
    , public IListener
{
public:
    TListener(
        SOCKET serverSocket,
        const TNetworkAddress& address,
        const TString& name,
        IPollerPtr poller,
        IPollerPtr acceptor)
        : Name_(name)
        , Address_(address)
        , ServerSocket_(serverSocket)
        , Poller_(poller)
        , Acceptor_(acceptor)
    { }

    // IPollable implementation
    const TString& GetLoggingTag() const override
    {
        return Name_;
    }

    void OnEvent(EPollControl /*control*/) override
    {
        try {
            while (true) {
                TPromise<IConnectionPtr> promise;

                {
                    auto guard = Guard(Lock_);
                    if (!Error_.IsOK()) {
                        break;
                    }
                    if (Queue_.empty()) {
                        Pending_ = true;
                        break;
                    }
                    promise = std::move(Queue_.front());
                    Queue_.pop_front();
                    Pending_ = false;
                }

                if (!TryAccept(promise)) {
                    auto guard = Guard(Lock_);
                    Queue_.push_back(promise);
                    if (!Pending_) {
                        break;
                    }
                }
            }
        } catch (const TErrorException& ex) {
            auto error = TError(ex) << TErrorAttribute("listener", Name_);
            Abort(error);
            YT_LOG_FATAL(error, "Listener crashed with fatal error");
        }

        auto guard = Guard(Lock_);
        if (Error_.IsOK() && !Pending_) {
            Acceptor_->Arm(ServerSocket_, this, EPollControl::Read | EPollControl::EdgeTriggered | EPollControl::BacklogEmpty);
        }
    }

    void OnShutdown() override
    {
        decltype(Queue_) queue;
        {
            auto guard = Guard(Lock_);
            if (Error_.IsOK()) {
                Error_ = TError("Listener is shut down");
            }
            std::swap(Queue_, queue);
            Acceptor_->Unarm(ServerSocket_, this);
            YT_VERIFY(TryClose(ServerSocket_, false));
        }

        for (auto& promise : queue) {
            promise.Set(Error_);
        }
    }

    const TNetworkAddress& GetAddress() const override
    {
        return Address_;
    }

    // IListener implementation
    TFuture<IConnectionPtr> Accept() override
    {
        auto promise = NewPromise<IConnectionPtr>();

        if (!Pending_ || !TryAccept(promise)) {
            auto guard = Guard(Lock_);
            if (Error_.IsOK()) {
                Queue_.push_back(promise);
                if (Pending_) {
                    Pending_ = false;
                    Acceptor_->Retry(this);
                }
            } else {
                promise.Set(Error_);
            }
        }

        promise.OnCanceled(BIND([promise, this, thisWeak_ = MakeWeak(this)] (const TError& error) {
            if (auto this_ = thisWeak_.Lock()) {
                auto guard = Guard(Lock_);
                auto it = std::find(Queue_.begin(), Queue_.end(), promise);
                if (it != Queue_.end()) {
                    Queue_.erase(it);
                }
            }
            promise.TrySet(TError(NYT::EErrorCode::Canceled, "Accept canceled")
                << error);
        }));

        return promise.ToFuture();
    }

    void Shutdown() override
    {
        Abort(TError("Listener is shut down"));
    }

private:
    const TString Name_;
    const TNetworkAddress Address_;
    const SOCKET ServerSocket_;
    const IPollerPtr Poller_;
    const IPollerPtr Acceptor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::atomic<bool> Pending_ = false;
    std::deque<TPromise<IConnectionPtr>> Queue_;
    TError Error_;


    void Abort(const TError& error)
    {
        YT_VERIFY(!error.IsOK());

        {
            auto guard = Guard(Lock_);

            if (!Error_.IsOK()) {
                return;
            }

            Pending_ = false;
            Error_ = error
                << TErrorAttribute("listener", Name_);
            Acceptor_->Unarm(ServerSocket_, this);
        }

        YT_UNUSED_FUTURE(Acceptor_->Unregister(this));
    }

    bool TryAccept(TPromise<IConnectionPtr> &promise)
    {
        TNetworkAddress clientAddress;
        auto clientSocket = AcceptSocket(ServerSocket_, &clientAddress);
        if (clientSocket == INVALID_SOCKET) {
            return false;
        }

        auto localAddress = GetSocketName(clientSocket);
        promise.TrySet(CreateConnectionFromFD(
            clientSocket,
            localAddress,
            clientAddress,
            Poller_));

        return true;
    }
};

DECLARE_REFCOUNTED_CLASS(TListener)
DEFINE_REFCOUNTED_TYPE(TListener)

////////////////////////////////////////////////////////////////////////////////

IListenerPtr CreateListener(
    const TNetworkAddress& address,
    const NConcurrency::IPollerPtr& poller,
    const NConcurrency::IPollerPtr& acceptor,
    int maxBacklogSize)
{
    auto serverSocket = address.GetSockAddr()->sa_family == AF_UNIX
        ? CreateUnixServerSocket()
        : CreateTcpServerSocket();

    try {
        BindSocket(serverSocket, address);
        // Client might have specified port == 0, find real address.
        auto realAddress = GetSocketName(serverSocket);

        ListenSocket(serverSocket, maxBacklogSize);
        auto listener = New<TListener>(
            serverSocket,
            realAddress,
            Format("Listener{%v}", realAddress),
            poller,
            acceptor);
        if (!acceptor->TryRegister(listener)) {
            THROW_ERROR_EXCEPTION("Cannot register listener pollable");
        }
        acceptor->Arm(serverSocket, listener, EPollControl::Read | EPollControl::EdgeTriggered);
        return listener;
    } catch (...) {
        YT_VERIFY(TryClose(serverSocket, false));
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
