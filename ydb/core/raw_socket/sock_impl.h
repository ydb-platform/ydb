#pragma once

#include <ydb/library/actors/interconnect/poller_actor.h>
#include "sock_config.h"
#include "sock64.h"
#include "sock_ssl.h"

namespace NKikimr::NRawSocket {

struct TEndpointInfo {
    TSslHelpers::TSslHolder<SSL_CTX> SecureContext;
};

class TSocketDescriptor : public NActors::TSharedDescriptor, public TNetworkConfig {
    std::unique_ptr<TNetworkConfig::TSocketType> Socket;
    std::shared_ptr<TEndpointInfo> Endpoint;
    TSpinLock Lock;

public:
    TSocketDescriptor(TSocketType&& s, std::shared_ptr<TEndpointInfo> endpoint)
        : Socket(std::make_unique<TNetworkConfig::TSocketType>(std::move(s)))
        , Endpoint(endpoint)
    {}

    int Listen(int backlog) {
        return Socket->Listen(backlog);
    }

    TIntrusivePtr<TSocketDescriptor> Accept(TSocketAddressType& addr) {
        std::optional<TNetworkConfig::TSocketType> s = Socket->Accept(addr);
        if (!s) {
            return {};
        }
        return new TSocketDescriptor(std::move(s).value(), Endpoint);
    }

    void SetNonBlock() {
        try {
            ::SetNonBlock(*Socket, true);
        }
        catch (const yexception&) {
        }
    }

    ssize_t Send(const void* data, size_t size) {
        return Socket->Send(data, size);
    }

    ssize_t Receive(void* data, size_t size) {
        return Socket->Recv(data, size);
    }

    void RequestPoller(NActors::TPollerToken::TPtr& pollerToken) {
        Socket->RequestPoller(pollerToken);
    }

    int UpgradeToSecure() {
        std::unique_ptr<TNetworkConfig::TSecureSocketType> socket = std::make_unique<TNetworkConfig::TSecureSocketType>(std::move(*Socket));
        int res = socket->SecureAccept(Endpoint->SecureContext.get());
        TGuard lock(Lock);
        Socket.reset(socket.release());
        return res;
    }

    int TryUpgradeToSecure() {
        for (;;) {
            int res = UpgradeToSecure();
            if (res >= 0) {
                return 0;
            } else if (-res == EINTR) {
                continue;
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                return 0;
            } else {
                return res;
            }
        }
    }


    void Shutdown() {
        ::shutdown(*Socket, SHUT_RDWR);
    }

    SOCKET GetRawSocket() const {
        TGuard lock(Lock);
        return *Socket;
    }

    int GetDescriptor() override {
        return GetRawSocket();
    }

    bool IsSslSupported() const {
        return Endpoint->SecureContext != nullptr;
    }
};

class TSocketBuffer : public TBuffer, public TNetworkConfig {
public:
    TSocketBuffer()
        : TBuffer(BUFFER_SIZE)
    {}

    bool EnsureEnoughSpaceAvailable(size_t need) {
        size_t avail = Avail();
        if (avail < need) {
            Reserve(Capacity() + need);
            return true;
        }
        return true;
    }

    // non-destructive variant of AsString
    TString AsString() const {
        return TString(Data(), Size());
    }
};

template<class T = TSocketDescriptor> // for tests
class TBufferedWriter {
public:
    TBufferedWriter(T* socket, size_t size)
        : Socket(socket)
        , BufferSize(size) {
    }

    void write(const char* src, size_t length) {
        size_t left = length;
        size_t offset = 0;

        do {
            TBuffer& buffer = GetOrCreateFrontBuffer();
            if (buffer.Avail() < left) {
                size_t avail = buffer.Avail();
                buffer.Append(src + offset, avail);
                offset += avail;
                left -= avail;
                BuffersDeque.push_front(TBuffer(BufferSize));
            } else {
                buffer.Append(src + offset, left);
                break;
            }
        } while (left > 0);
    }

    [[nodiscard]] ssize_t flush() {
        size_t totalWritten = 0;

        while (!BuffersDeque.empty()) {
            TBuffer& buffer = BuffersDeque.back();
            ssize_t left = buffer.Size() - CurrentBufferOffset;
            while (left > 0) {
                ssize_t res = Send(buffer.Data() + CurrentBufferOffset, left);
                if (res < 0) {
                    return res;
                } else if (res == left) {
                    totalWritten += res;
                    CurrentBufferOffset = 0;
                    BuffersDeque.pop_back();
                    break;
                } else {
                    left -= res;
                    CurrentBufferOffset += res;
                    totalWritten += res;
                }
            }
        }

        return totalWritten;
    }

    TBuffer& GetFrontBuffer() {
        return GetOrCreateFrontBuffer();
    }

    bool Empty() const {
        return BuffersDeque.empty();
    }

    const TDeque<TBuffer>& GetBuffersDeque() const {
        return BuffersDeque;
    }

private:
    T* Socket;
    size_t BufferSize;
    TDeque<TBuffer> BuffersDeque = {};
    size_t CurrentBufferOffset = 0;

    ssize_t Send(const char* data, size_t length) {
        ssize_t res = Socket->Send(data, length);
        
        return res;
    }

    TBuffer& GetOrCreateFrontBuffer() {
        if (BuffersDeque.empty()) {
            BuffersDeque.push_front(TBuffer(BufferSize));
        }
        return BuffersDeque.front();
    }
};

} // namespace NKikimr::NRawSocket

