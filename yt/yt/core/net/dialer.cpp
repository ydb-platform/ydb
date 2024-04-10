#include "dialer.h"
#include "connection.h"
#include "config.h"

#include <yt/yt/core/concurrency/pollable_detail.h>

#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/net/socket.h>

#include <util/random/random.h>

namespace NYT::NNet {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDialSession
    : public TRefCounted
{
public:
    TDialSession(
        const TNetworkAddress& remoteAddress,
        const IAsyncDialerPtr& asyncDialer,
        IPollerPtr poller)
        : Name_(Format("dialer[%v]", remoteAddress))
        , RemoteAddress_(remoteAddress)
        , Poller_(std::move(poller))
        , Session_(asyncDialer->CreateSession(
            remoteAddress,
            BIND(&TDialSession::OnDialerFinished, MakeWeak(this))))
    {
        Session_->Dial();

        Promise_.OnCanceled(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
            Promise_.TrySet(TError(NYT::EErrorCode::Canceled, "Dial canceled")
                << TErrorAttribute("dialer", Name_)
                << error);
        }));
    }

    TFuture<IConnectionPtr> GetFuture() const
    {
        return Promise_.ToFuture();
    }

private:
    const TString Name_;
    const TNetworkAddress RemoteAddress_;
    const IPollerPtr Poller_;
    const IAsyncDialerSessionPtr Session_;

    const TPromise<IConnectionPtr> Promise_ = NewPromise<IConnectionPtr>();

    void OnDialerFinished(const TErrorOr<SOCKET>& socketOrError)
    {
        if (socketOrError.IsOK()) {
            auto socket = socketOrError.Value();
            Promise_.TrySet(CreateConnectionFromFD(
                socket,
                GetSocketName(socket),
                RemoteAddress_,
                Poller_));
        } else {
            Promise_.TrySet(socketOrError
                << TErrorAttribute("dialer", Name_));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDialer
    : public IDialer
{
public:
    TDialer(
        TDialerConfigPtr config,
        IPollerPtr poller,
        const NLogging::TLogger& logger)
        : AsyncDialer_(CreateAsyncDialer(
            std::move(config),
            poller,
            logger))
        , Poller_(std::move(poller))
    { }

    TFuture<IConnectionPtr> Dial(
        const TNetworkAddress& remote,
        TDialerContextPtr /*context*/) override
    {
        auto session = New<TDialSession>(
            remote,
            AsyncDialer_,
            Poller_);
        return session->GetFuture();
    }

private:
    const IAsyncDialerPtr AsyncDialer_;
    const IPollerPtr Poller_;
};

DEFINE_REFCOUNTED_TYPE(TDialer)

////////////////////////////////////////////////////////////////////////////////

IDialerPtr CreateDialer(
    TDialerConfigPtr config,
    IPollerPtr poller,
    const NLogging::TLogger& logger)
{
    return New<TDialer>(
        std::move(config),
        std::move(poller),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncDialerSession
    : public IAsyncDialerSession
{
public:
    TAsyncDialerSession(
        TDialerConfigPtr config,
        IPollerPtr poller,
        const NLogging::TLogger& logger,
        const TNetworkAddress& address,
        TAsyncDialerCallback onFinished)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , Address_(address)
        , OnFinished_(std::move(onFinished))
        , Id_(TGuid::Create())
        , Logger(logger.WithTag("AsyncDialerSession: %v", Id_))
        , Timeout_(Config_->MinRto * GetRandomVariation())
    { }

    ~TAsyncDialerSession()
    {
        auto guard = Guard(SpinLock_);

        Finished_ = true;
        CloseSocket();
    }

    void Dial() override
    {
        auto guard = Guard(SpinLock_);

        YT_VERIFY(!Dialed_);
        Dialed_ = true;

        Connect(guard);
    }

private:
    class TPollable
        : public TPollableBase
    {
    public:
        TPollable(TAsyncDialerSession* owner, TGuid id, SOCKET socket)
            : Owner_(MakeWeak(owner))
            , LoggingTag_(Format("AsyncDialerSession{%v:%v}", id, socket))
        { }

        const TString& GetLoggingTag() const override
        {
            return LoggingTag_;
        }

        void OnEvent(EPollControl /*control*/) override
        {
            if (auto owner = Owner_.Lock()) {
                owner->OnConnected(this);
            }
        }

        void OnShutdown() override
        {
            if (auto owner = Owner_.Lock()) {
                owner->OnShutdown(this);
            }
        }

    private:
        const NLogging::TLogger Logger;
        const TWeakPtr<TAsyncDialerSession> Owner_;
        const TString LoggingTag_;
    };

    using TPollablePtr = TIntrusivePtr<TPollable>;

    const TDialerConfigPtr Config_;
    const IPollerPtr Poller_;
    const TNetworkAddress Address_;
    const TAsyncDialerCallback OnFinished_;
    const TGuid Id_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    SOCKET Socket_ = INVALID_SOCKET;
    bool Dialed_ = false;
    bool Finished_ = false;
    TDuration Timeout_;
    TDelayedExecutorCookie TimeoutCookie_;
    TPollablePtr Pollable_;

    void CloseSocket()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (Socket_ != INVALID_SOCKET) {
            YT_VERIFY(TryClose(Socket_));
            Socket_ = INVALID_SOCKET;
        }
    }

    bool TryRegisterPollable()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        auto pollable = New<TPollable>(this, Id_, Socket_);
        if (!Poller_->TryRegister(pollable)) {
            return false;
        }

        YT_VERIFY(!Pollable_);
        Pollable_ = std::move(pollable);
        Poller_->Arm(Socket_, Pollable_, EPollControl::Read | EPollControl::Write | EPollControl::EdgeTriggered);
        return true;
    }

    void UnregisterPollable()
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        YT_VERIFY(Socket_ != INVALID_SOCKET);
        Poller_->Unarm(Socket_, Pollable_);

        YT_VERIFY(Pollable_);
        auto pollable = std::move(Pollable_);

        auto inverseGuard = Unguard(SpinLock_);
        YT_UNUSED_FUTURE(Poller_->Unregister(pollable));
    }

    void Connect(TGuard<NThreading::TSpinLock>& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        try {
            auto family = Address_.GetSockAddr()->sa_family;

            YT_VERIFY(Socket_ == INVALID_SOCKET);
            if (family == AF_UNIX) {
                Socket_ = CreateUnixClientSocket();
            } else {
                Socket_ = CreateTcpClientSocket(family);
                if (Config_->EnableNoDelay && !TrySetSocketNoDelay(Socket_)) {
                    YT_LOG_DEBUG("Failed to set socket no delay option");
                }
                if (!TrySetSocketKeepAlive(Socket_)) {
                    YT_LOG_DEBUG("Failed to set socket keep alive option");
                }
            }

            if (ConnectSocket(Socket_, Address_) == 0) {
                // Connection was established synchronously.
                SOCKET socket = Socket_;
                Socket_ = INVALID_SOCKET;
                Finished_ = true;
                guard.Release();
                OnFinished_(socket);
                return;
            }

            if (!TryRegisterPollable()) {
                THROW_ERROR_EXCEPTION("Cannot register dailer pollable");
            }
        } catch (const std::exception& ex) {
            Finished_ = true;
            CloseSocket();
            guard.Release();
            OnFinished_(ex);
            return;
        }

        if (Config_->EnableAggressiveReconnect) {
            TimeoutCookie_ = TDelayedExecutor::Submit(
                BIND(&TAsyncDialerSession::OnTimeout, MakeWeak(this)),
                Timeout_);
        }
    }

    void OnConnected(TPollable* pollable)
    {
        auto guard = Guard(SpinLock_);

        if (Finished_ || pollable != Pollable_) {
            return;
        }

        Finished_ = true;

        UnregisterPollable();

        TDelayedExecutor::CancelAndClear(TimeoutCookie_);

        SOCKET socket = Socket_;
        YT_VERIFY(socket != INVALID_SOCKET);

        if (int socketError = GetSocketError(socket); socketError == 0) {
            Socket_ = INVALID_SOCKET;
            guard.Release();
            OnFinished_(socket);
        } else {
            auto error = TError(NRpc::EErrorCode::TransportError, "Connect error")
                << TError::FromSystem(socketError);
            CloseSocket();
            guard.Release();
            OnFinished_(error);
        }
    }


    void OnShutdown(TPollable* pollable)
    {
        auto guard = Guard(SpinLock_);

        if (Finished_ || pollable != Pollable_) {
            return;
        }

        Finished_ = true;

        Pollable_.Reset();
        TDelayedExecutor::CancelAndClear(TimeoutCookie_);

        guard.Release();

        OnFinished_(TError("Dialer session was shut down"));
    }

    void OnTimeout()
    {
        auto guard = Guard(SpinLock_);

        if (Finished_) {
            return;
        }

        UnregisterPollable();

        CloseSocket();

        if (Timeout_ < Config_->MaxRto) {
            Timeout_ *= Config_->RtoScale * GetRandomVariation();
        }

        YT_LOG_DEBUG("Connect timeout; trying to reconnect (Timeout: %v)",
            Timeout_);

        Connect(guard);
    }

    static float GetRandomVariation()
    {
        return (0.9 + RandomNumber<float>() / 5);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAsyncDialer
    : public IAsyncDialer
{
public:
    TAsyncDialer(
        TDialerConfigPtr config,
        IPollerPtr poller,
        const NLogging::TLogger& logger)
        : Config_(std::move(config))
        , Poller_(std::move(poller))
        , Logger(logger)
    { }

    IAsyncDialerSessionPtr CreateSession(
        const TNetworkAddress& address,
        TAsyncDialerCallback onFinished) override
    {
        return New<TAsyncDialerSession>(
            Config_,
            Poller_,
            Logger,
            address,
            std::move(onFinished));
    }

private:
    const TDialerConfigPtr Config_;
    const IPollerPtr Poller_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

IAsyncDialerPtr CreateAsyncDialer(
    TDialerConfigPtr config,
    IPollerPtr poller,
    const NLogging::TLogger& logger)
{
    return New<TAsyncDialer>(
        std::move(config),
        std::move(poller),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
