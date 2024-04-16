#pragma once

#include <optional>
#include <util/network/sock.h>
#include <ydb/library/actors/interconnect/poller_actor.h>
#include "sock_settings.h"
#include "sock_ssl.h"

namespace NKikimr::NRawSocket {

class TInet64StreamSocket : public TStreamSocket {
    using TBase = TStreamSocket;
protected:
    TInet64StreamSocket(const TInet64StreamSocket& parent, SOCKET fd)
        : TStreamSocket(fd)
        , AF(parent.AF)
    {
    }

public:
    TInet64StreamSocket(const TSocketSettings& socketSettings = {}) {
        CreateSocket(socketSettings);
    }

    TInet64StreamSocket(TInet64StreamSocket&& socket) = default;
    virtual ~TInet64StreamSocket() = default;

    static bool IsIPv6(const TString& host) {
        if (host.find_first_not_of(":0123456789abcdef") != TString::npos) {
            return false;
        }
        if (std::count(host.begin(), host.end(), ':') < 2) {
            return false;
        }
        return true;
    }

    std::shared_ptr<ISockAddr> MakeAddress(const TString& address, int port) {
        if (!address) {
            if (AF == AF_INET6) {
                return std::make_shared<TSockAddrInet6>("::", port);
            } else {
                return std::make_shared<TSockAddrInet>(INADDR_ANY, port);
            }
        }
        if (IsIPv6(address)) {
            return std::make_shared<TSockAddrInet6>(address.data(), port);
        } else {
            return std::make_shared<TSockAddrInet>(address.data(), port);
        }
    }

    static std::shared_ptr<ISockAddr> MakeAddress(const sockaddr_storage& storage) {
        std::shared_ptr<ISockAddr> addr;
        switch (storage.ss_family) {
            case AF_INET:
                addr = std::make_shared<TSockAddrInet>();
                break;
            case AF_INET6:
                addr = std::make_shared<TSockAddrInet6>();
                break;
        }
        if (addr) {
            memcpy(addr->SockAddr(), &storage, addr->Size());
        }
        return addr;
    }

    std::optional<TInet64StreamSocket> Accept(std::shared_ptr<ISockAddr>& acceptedAddr) {
        sockaddr_storage addrStorage = {};
        socklen_t addrLen = sizeof(addrStorage);
        SOCKET s = accept((SOCKET)*this, reinterpret_cast<sockaddr*>(&addrStorage), &addrLen);
        if (s == INVALID_SOCKET) {
            return {};
        }
        acceptedAddr = MakeAddress(addrStorage);
        return TInet64StreamSocket(*this, s);
    }

    bool WantsToRead = false;
    bool WantsToWrite = false;

    void ResetPollerState() {
        WantsToRead = false;
        WantsToWrite = false;
    }

    void RequestPoller(NActors::TPollerToken::TPtr& pollerToken) {
        if (pollerToken && (WantsToRead || WantsToWrite)) {
            pollerToken->Request(WantsToRead, WantsToWrite);
            ResetPollerState();
        }
    }

    virtual ssize_t Send(const void* msg, size_t len, int flags = 0) {
        ssize_t res = TBase::Send(msg, len, flags);
        if (res < 0) {
            if (-res == EAGAIN || -res == EWOULDBLOCK) {
                WantsToWrite = true;
            }
        }
        return res;
    }

    virtual ssize_t Recv(void* buf, size_t len, int flags = 0) {
        ssize_t res = TBase::Recv(buf, len, flags);
        if (res < 0) {
            if (-res == EAGAIN || -res == EWOULDBLOCK) {
                WantsToRead = true;
            }
        }
        return res;
    }

protected:
    int AF = 0;

    void CreateSocket(const TSocketSettings& socketSettings) {
        SOCKET s;
        if (socketSettings.AF == 0) {
            s = socket(AF = AF_INET6, SOCK_STREAM, 0);
            if (s < 0) {
                s = socket(AF = AF_INET, SOCK_STREAM, 0);
            }
        } else {
            s = socket(AF = socketSettings.AF, SOCK_STREAM, 0);
        }
        SetSockOpt(s, IPPROTO_TCP, TCP_NODELAY, (int)socketSettings.TcpNotDelay);
        if (AF == AF_INET6) {
            SetSockOpt(s, SOL_SOCKET, IPV6_V6ONLY, (int)false);
        }
        TSocketHolder sock(s);
        sock.Swap(*this);
    }
};

class TInet64SecureStreamSocket : public TInet64StreamSocket, TSslLayer<TStreamSocket> {
    template<typename T>
    using TSslHolder = TSslLayer<TStreamSocket>::TSslHolder<T>;

    TSslHolder<BIO> Bio;
    TSslHolder<SSL> Ssl;

public:
    TInet64SecureStreamSocket(const TSocketSettings& socketSettings = {})
        : TInet64StreamSocket(socketSettings)
    {}

    TInet64SecureStreamSocket(TInet64StreamSocket&& socket)
        : TInet64StreamSocket(std::move(socket))
    {}

    void InitServerSsl(SSL_CTX* ctx) {
        Bio.reset(BIO_new(TSslLayer<TStreamSocket>::IoMethod()));
        BIO_set_data(Bio.get(), static_cast<TStreamSocket*>(this));
        BIO_set_nbio(Bio.get(), 1);
        Ssl = TSslHelpers::ConstructSsl(ctx, Bio.get());
        SSL_set_accept_state(Ssl.get());
    }

    int ProcessResult(int res) {
        if (res <= 0) {
            res = SSL_get_error(Ssl.get(), res);
            switch(res) {
            case SSL_ERROR_WANT_READ:
                WantsToRead = true;
                return -EAGAIN;
            case SSL_ERROR_WANT_WRITE:
                WantsToWrite = true;
                return -EAGAIN;
            default:
                return -EIO;
            }
        }
        return res;
    }

    int SecureAccept(SSL_CTX* ctx) {
        if (!Ssl) {
            InitServerSsl(ctx);
        }
        int res = SSL_accept(Ssl.get());
        return ProcessResult(res);
    }

    ssize_t Send(const void* msg, size_t len, int flags = 0) override {
        Y_UNUSED(flags);
        int res = SSL_write(Ssl.get(), msg, len);
        return ProcessResult(res);
    }

    ssize_t Recv(void* buf, size_t len, int flags = 0) override {
        Y_UNUSED(flags);
        int res = SSL_read(Ssl.get(), buf, len);
        return ProcessResult(res);
    }
};

} // namespace NKikimr::NRawSocket
