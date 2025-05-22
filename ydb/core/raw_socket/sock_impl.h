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

class TBufferedWriter {
public:
    TBufferedWriter(TSocketDescriptor* socket, size_t size)
        : Socket(socket)
        , Buffer(size)
        , BufferSize(size) {
    }

    void write(const char* src, size_t length) {
        size_t possible = std::min(length, Buffer.Avail());
        if (possible > 0) {
            Buffer.Append(src, possible);
        }
        if (0 == Buffer.Avail() && Socket) {
            flush();
        }
        size_t left = length - possible;
        if (left >= BufferSize) {
            if (Chunks.empty()) {
                // optimization for reduce memory copy
                ssize_t res = Socket->Send(src + possible, left);
                if (res > 0) {
                    left -= res;
                    possible += res;
                }
            }
            if (left > 0) {
                Buffer.Reserve(left);
                Buffer.Append(src + possible, left);
                flush();
            }
        } else if (left > 0) {
            Buffer.Append(src + possible, left);
        }
    }

    ssize_t flush() {
        if (!Buffer.Empty()) {
            Chunks.emplace_back(std::move(Buffer));
            Buffer.Reserve(BufferSize);
        }
        while(!Chunks.empty()) {
            auto& chunk = Chunks.front();
            ssize_t res = Socket->Send(chunk.Data(), chunk.Size());
            if (res > 0) {
                if (static_cast<size_t>(res) == chunk.Size()) {
                    Chunks.pop_front();
                } else {
                    chunk.Shift(res);
                }
            } else if (-res == EINTR) {
                continue;
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                return 0;
            } else {
                return res;
            }
        }

        return 0;
    }

    const char* Data() {
        return Buffer.Data();
    }

    const TBuffer& GetBuffer() {
        return Buffer;
    }

    size_t Size() {
        return Buffer.Size();
    }

    bool Empty() {
        return Buffer.Empty() && Chunks.empty();
    }

private:
    TSocketDescriptor* Socket;
    TBuffer Buffer;
    size_t BufferSize;

    struct Chunk {
        Chunk(TBuffer&& buffer)
            : Buffer(std::move(buffer))
            , Position(0) {
        }

        TBuffer Buffer;
        size_t Position;

        const char* Data() { return Buffer.Data() + Position; }
        size_t Size() { return Buffer.Size() - Position; }
        void Shift(size_t size) { Position += size; }
    };
    std::deque<Chunk> Chunks;

};

} // namespace NKikimr::NRawSocket
