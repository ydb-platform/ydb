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
        , Buffer(size) {
    }

    /**
    * Writes data to the socket buffer.
    *
    * This method writes the specified number of bytes from the source buffer to the internal buffer.
    * If the internal buffer becomes full, it flushes the buffer to the socket. The process repeats until all data is written.
    *
    * @param src A pointer to the source buffer containing the data to be written.
    * @param length The number of bytes to write from the source buffer.
    * @return The total number of bytes written to the socket. If an error occurs during writing, a negative value is returned.
    */
    [[nodiscard]] ssize_t write(const char* src, size_t length) {
        size_t left = length;
        size_t offset = 0;
        ssize_t totalWritten = 0;
        do {
            if (Buffer.Avail() < left) { // time to flush
                // flush the remains from buffer, than write straight to socket if we have a lot data
                if (!Empty()) {
                    ssize_t flushRes = flush();
                    if (flushRes < 0) {
                        // less than zero means error
                        return flushRes;
                    } else {
                        totalWritten += flushRes;
                    }
                }
                // if we have a lot data, skip copying it to buffer, just send ot straight to socket
                if (left > Buffer.Capacity()) {
                    // we send only small batch to socket, cause we know for sure that it will be written to socket without error
                    // there was a bug when we wrote to socket one big batch and OS closed the connection in case message was bigger than 6mb and SSL was enabled
                    size_t bytesToSend = std::min(left, MAX_SOCKET_BATCH_SIZE);
                    ssize_t sendRes = Send(src + offset, bytesToSend);
                    if (sendRes <= 0) {
                        // less than zero means error
                        // exactly zero is also interpreted as error
                        return sendRes;
                    } else {
                        left -= sendRes;
                        offset += sendRes;
                        totalWritten += sendRes;
                    }
                } else {
                    Buffer.Append(src + offset, left);
                    left = 0;   
                }
            } else {
                Buffer.Append(src + offset, left);
                left = 0;
            }
        } while (left > 0);

        return totalWritten;
    }

    [[nodiscard]] ssize_t flush() {
        if (Empty()) {
            return 0;
        }
        ssize_t res = Send(Data(), Size());
        if (res > 0) {
            Buffer.Clear();
        }
        return res;
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
        return Buffer.Empty();
    }

private:
    static constexpr ui32 MAX_RETRY_ATTEMPTS = 3;
    static constexpr size_t MAX_SOCKET_BATCH_SIZE = 1_MB;
    TSocketDescriptor* Socket;
    TBuffer Buffer;

    ssize_t Send(const char* data, size_t length) {
        ui32 retryAttemtpts = MAX_RETRY_ATTEMPTS;
        while (true) {
            ssize_t res = Socket->Send(data, length);
            // retry 
            if ((-res == EAGAIN || -res == EWOULDBLOCK || -res == EINTR) && retryAttemtpts--) {
                continue;
            }
            
            return res;
        }
        
        Y_UNREACHABLE();
    }
};

} // namespace NKikimr::NRawSocket

