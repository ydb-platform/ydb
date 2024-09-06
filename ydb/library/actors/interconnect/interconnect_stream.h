#pragma once

#include <util/generic/string.h>
#include <util/generic/noncopyable.h>
#include <util/network/address.h>
#include <util/network/init.h>
#include <util/system/defaults.h>

#include "poller.h"

#include "interconnect_address.h"

#include <memory>

#include <sys/uio.h>

namespace NActors {
    class TPollerToken;
    struct TInterconnectProxyCommon;
}

namespace NInterconnect {
    class TSocket: public NActors::TSharedDescriptor, public TNonCopyable {
    protected:
        TSocket(SOCKET fd);

        virtual ~TSocket() override;

        SOCKET Descriptor;

        virtual int GetDescriptor() override;

    private:
        friend class TSecureSocket;

        SOCKET ReleaseDescriptor() {
            return std::exchange(Descriptor, INVALID_SOCKET);
        }

    public:
        operator SOCKET() const {
            return Descriptor;
        }

        int Bind(const TAddress& addr) const;
        int Shutdown(int how) const;
        int GetConnectStatus() const;
    };

    class TStreamSocket: public TSocket {
    public:
        TStreamSocket(SOCKET fd);

        static TIntrusivePtr<TStreamSocket> Make(int domain, int *error = nullptr);

        virtual ssize_t Send(const void* msg, size_t len, TString *err = nullptr) const;
        virtual ssize_t Recv(void* buf, size_t len, TString *err = nullptr) const;

        virtual ssize_t WriteV(const struct iovec* iov, int iovcnt) const;
        virtual ssize_t ReadV(const struct iovec* iov, int iovcnt) const;

        int Connect(const TAddress& addr) const;
        int Connect(const NAddr::IRemoteAddr* addr) const;
        int Listen(int backlog) const;
        int Accept(TAddress& acceptedAddr) const;

        ssize_t GetUnsentQueueSize() const;

        void SetSendBufferSize(i32 len) const;
        ui32 GetSendBufferSize() const;

        virtual void Request(NActors::TPollerToken& token, bool read, bool write);
        virtual bool RequestReadNotificationAfterWouldBlock(NActors::TPollerToken& token);
        virtual bool RequestWriteNotificationAfterWouldBlock(NActors::TPollerToken& token);

        virtual size_t ExpectedWriteLength() const;
    };

    class TSecureSocketContext {
        class TImpl;
        THolder<TImpl> Impl;

        friend class TSecureSocket;

    public:
        TSecureSocketContext(TIntrusivePtr<NActors::TInterconnectProxyCommon> common);
        ~TSecureSocketContext();

    public:
        using TPtr = std::shared_ptr<TSecureSocketContext>;
    };

    class TSecureSocket : public TStreamSocket {
        TSecureSocketContext::TPtr Context;

        class TImpl;
        THolder<TImpl> Impl;

    public:
        enum class EStatus {
            SUCCESS,
            ERROR,
            WANT_READ,
            WANT_WRITE,
        };

    public:
        TSecureSocket(TStreamSocket& socket, TSecureSocketContext::TPtr context);
        ~TSecureSocket();

        EStatus Establish(bool server, bool authOnly, TString& err) const;
        TIntrusivePtr<TStreamSocket> Detach();

        ssize_t Send(const void* msg, size_t len, TString *err) const override;
        ssize_t Recv(void* msg, size_t len, TString *err) const override;

        ssize_t WriteV(const struct iovec* iov, int iovcnt) const override;
        ssize_t ReadV(const struct iovec* iov, int iovcnt) const override;

        TString GetCipherName() const;
        int GetCipherBits() const;
        TString GetProtocolName() const;
        TString GetPeerCommonName() const;
        TString GetSignatureAlgorithm() const;

        bool WantRead() const;
        bool WantWrite() const;
        void Request(NActors::TPollerToken& token, bool read, bool write) override;
        bool RequestReadNotificationAfterWouldBlock(NActors::TPollerToken& token) override;
        bool RequestWriteNotificationAfterWouldBlock(NActors::TPollerToken& token) override;
        size_t ExpectedWriteLength() const override;
    };

    class TDatagramSocket: public TSocket {
    public:
        typedef std::shared_ptr<TDatagramSocket> TPtr;

        TDatagramSocket(SOCKET fd);

        static TPtr Make(int domain);

        ssize_t SendTo(const void* msg, size_t len, const TAddress& toAddr) const;
        ssize_t RecvFrom(void* buf, size_t len, TAddress& fromAddr) const;
    };

}
