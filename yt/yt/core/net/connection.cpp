#include "connection.h"
#include "packet_connection.h"
#include "private.h"

#include <yt/yt/core/concurrency/pollable_detail.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/net/socket.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/network/pollerimpl.h>

#include <errno.h>

#ifdef _linux_
    #include <sys/ioctl.h>
    #include <sys/signalfd.h>
#endif

#ifdef _win_
    #include <util/network/socket.h>
    #include <util/network/pair.h>

    #include <winsock2.h>

    #include <sys/uio.h>
    #include <fcntl.h>

    #define SHUT_RD SD_RECEIVE
    #define SHUT_WR SD_SEND
    #define SHUT_RDWR SD_BOTH

    #define EWOULDBLOCK WSAEWOULDBLOCK
#endif

namespace NYT::NNet {

using namespace NConcurrency;

#ifdef _unix_
    using TIOVecBasePtr = void*;
#else
    using TIOVecBasePtr = char*;
#endif

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = NetLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

int GetLastNetworkError()
{
#ifdef _win_
    return WSAGetLastError();
#else
    return errno;
#endif
}

ssize_t ReadFromFD(TFileDescriptor fd, char* buffer, size_t length)
{
#ifdef _win_
    return ::recv(
        fd,
        buffer,
        length,
        /*flags*/ 0);
#else
    return HandleEintr(
        ::read,
        fd,
        buffer,
        length);
#endif
}

ssize_t WriteToFD(TFileDescriptor fd, const char* buffer, size_t length)
{
#ifdef _win_
    return ::send(
        fd,
        buffer,
        length,
        /*flags*/ 0);
#else
    return HandleEintr(
        ::write,
        fd,
        buffer,
        length);
#endif
}

TError MakeSystemError(TFormatString<> message)
{
    return TError(message)
        << TError::FromSystem();
}

[[maybe_unused]] TErrorOr<int> CheckPipeBytesLeftToRead(TFileDescriptor fd) noexcept
{
#ifdef _linux_
    int bytesLeft = 0;

    {
        int ret = ::ioctl(fd, FIONREAD, &bytesLeft);
        if (ret == -1) {
            return MakeSystemError("ioctl failed");
        }
    }

    return bytesLeft;
#else
    Y_UNUSED(fd);
    return TError("Unsupported platform");
#endif
}

template <class TDerived>
class TWriteConnectionBase
    : public virtual IConnectionWriter
{
public:
    TFuture<void> Write(const TSharedRef& data) override
    {
        return ToDerived()->GetImpl()->Write(data);
    }

    TFuture<void> Close() override
    {
        return ToDerived()->GetImpl()->Close();
    }

    TFuture<void> WriteV(const TSharedRefArray& data) override
    {
        return ToDerived()->GetImpl()->WriteV(data);
    }

    TFuture<void> CloseWrite() override
    {
        return ToDerived()->GetImpl()->CloseWrite();
    }

    TFuture<void> Abort() override
    {
        return ToDerived()->GetImpl()->Abort(TError(NNet::EErrorCode::Aborted, "Connection aborted"));
    }

    int GetHandle() const override
    {
        return ToDerived()->GetImpl()->GetHandle();
    }

    i64 GetWriteByteCount() const override
    {
        return ToDerived()->GetImpl()->GetWriteByteCount();
    }

    void SetWriteDeadline(std::optional<TInstant> deadline) override
    {
        ToDerived()->GetImpl()->SetWriteDeadline(deadline);
    }

    TConnectionStatistics GetWriteStatistics() const override
    {
        return ToDerived()->GetImpl()->GetWriteStatistics();
    }

protected:
    TDerived* ToDerived()
    {
        return static_cast<TDerived*>(this);
    }

    const TDerived* ToDerived() const
    {
        return static_cast<const TDerived*>(this);
    }
};

template <class TDerived>
class TReadConnectionBase
    : public virtual IConnectionReader
{
public:
    TFuture<size_t> Read(const TSharedMutableRef& data) override
    {
        return ToDerived()->GetImpl()->Read(data);
    }

    TFuture<void> CloseRead() override
    {
        return ToDerived()->GetImpl()->CloseRead();
    }

    TFuture<void> Abort() override
    {
        return ToDerived()->GetImpl()->Abort(TError(NNet::EErrorCode::Aborted, "Connection aborted"));
    }

    int GetHandle() const override
    {
        return ToDerived()->GetImpl()->GetHandle();
    }

    i64 GetReadByteCount() const override
    {
        return ToDerived()->GetImpl()->GetReadByteCount();
    }

    void SetReadDeadline(std::optional<TInstant> deadline) override
    {
        ToDerived()->GetImpl()->SetReadDeadline(deadline);
    }

    TConnectionStatistics GetReadStatistics() const override
    {
        return ToDerived()->GetImpl()->GetReadStatistics();
    }
protected:
    TDerived* ToDerived()
    {
        return static_cast<TDerived*>(this);
    }

    const TDerived* ToDerived() const
    {
        return static_cast<const TDerived*>(this);
    }
};

template <class TDerived>
class TReadWriteConnectionBase
    : public TWriteConnectionBase<TDerived>
    , public TReadConnectionBase<TDerived>
    , public IConnection
{
    using TWriteConnectionBase<TDerived>::ToDerived;

public:
    TConnectionId GetId() const override
    {
        return ToDerived()->GetImpl()->GetId();
    }

    const TNetworkAddress& GetLocalAddress() const override
    {
        return ToDerived()->GetImpl()->GetLocalAddress();
    }

    const TNetworkAddress& GetRemoteAddress() const override
    {
        return ToDerived()->GetImpl()->GetRemoteAddress();
    }

    bool IsIdle() const override
    {
        return ToDerived()->GetImpl()->IsIdle();
    }

    bool IsReusable() const override
    {
        return ToDerived()->GetImpl()->IsReusable();
    }

    bool SetNoDelay() override
    {
        return ToDerived()->GetImpl()->SetNoDelay();
    }

    bool SetKeepAlive() override
    {
        return ToDerived()->GetImpl()->SetKeepAlive();
    }

    TFuture<void> Abort() override
    {
        return ToDerived()->GetImpl()->Abort(TError(NNet::EErrorCode::Aborted, "Connection aborted"));
    }

    void SubscribePeerDisconnect(TCallback<void()> cb) override
    {
        return ToDerived()->GetImpl()->SubscribePeerDisconnect(std::move(cb));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFDConnectionImpl)
DECLARE_REFCOUNTED_CLASS(TDeliveryFencedWriteConnectionImpl)

////////////////////////////////////////////////////////////////////////////////

struct TIOResult
{
    bool Retry;
    size_t ByteCount;
};

struct IIOOperation
{
    virtual ~IIOOperation() = default;

    virtual TErrorOr<TIOResult> PerformIO(TFileDescriptor fd) = 0;

    virtual void Abort(const TError& error) = 0;

    virtual void SetResult() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReadOperation
    : public IIOOperation
{
public:
    explicit TReadOperation(TSharedMutableRef buffer)
        : Buffer_(std::move(buffer))
    { }

    TErrorOr<TIOResult> PerformIO(TFileDescriptor fd) override
    {
        size_t bytesRead = 0;
        while (Position_ < Buffer_.Size()) {
            ssize_t size = ReadFromFD(
                fd,
                Buffer_.Begin() + Position_,
                Buffer_.Size() - Position_);
            if (size == -1) {
                if (GetLastNetworkError() == EWOULDBLOCK || bytesRead > 0) {
                    return TIOResult{.Retry = Position_ == 0, .ByteCount = bytesRead};
                }

                return MakeSystemError("Read failed");
            }
            if (size == 0) {
                break;
            }

            bytesRead += size;
            Position_ += size;
        }
        return TIOResult{.Retry = false, .ByteCount = bytesRead};
    }

    void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    void SetResult() override
    {
        ResultPromise_.Set(Position_);
    }

    TFuture<size_t> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

private:
    const TSharedMutableRef Buffer_;
    const TPromise<size_t> ResultPromise_ = NewPromise<size_t>();

    size_t Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TReceiveFromOperation
    : public IIOOperation
{
public:
    explicit TReceiveFromOperation(TSharedMutableRef buffer)
        : Buffer_(std::move(buffer))
    { }

    TErrorOr<TIOResult> PerformIO(TFileDescriptor fd) override
    {
        ssize_t size = HandleEintr(
            ::recvfrom,
            fd,
            Buffer_.Begin(),
            Buffer_.Size(),
            /*flags*/ 0,
            RemoteAddress_.GetSockAddr(),
            RemoteAddress_.GetLengthPtr());

        if (size == -1) {
            if (GetLastNetworkError() == EWOULDBLOCK) {
                return TIOResult{.Retry = true, .ByteCount = 0};
            }

            return MakeSystemError("Read failed");
        }

        Position_ += size;

        return TIOResult{.Retry = false, .ByteCount = static_cast<size_t>(size)};
    }

    void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    void SetResult() override
    {
        ResultPromise_.Set(std::pair(Position_, RemoteAddress_));
    }

    TFuture<std::pair<size_t, TNetworkAddress>> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

private:
    const TSharedMutableRef Buffer_;
    const TPromise<std::pair<size_t, TNetworkAddress>> ResultPromise_ = NewPromise<std::pair<size_t, TNetworkAddress>>();

    size_t Position_ = 0;
    TNetworkAddress RemoteAddress_;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteOperation
    : public IIOOperation
{
public:
    explicit TWriteOperation(TSharedRef buffer)
        : Buffer_(std::move(buffer))
    { }

    TErrorOr<TIOResult> PerformIO(TFileDescriptor fd) override
    {
        size_t bytesWritten = 0;
        while (Position_ < Buffer_.Size()) {
            ssize_t size = WriteToFD(
                fd,
                Buffer_.Begin() + Position_,
                Buffer_.Size() - Position_);
            if (size == -1) {
                if (GetLastNetworkError() == EWOULDBLOCK) {
                    return TIOResult{.Retry = true, .ByteCount = bytesWritten};
                }
                return MakeSystemError("Write failed");
            }

            YT_VERIFY(size > 0);
            bytesWritten += size;
            Position_ += size;
        }
        return TIOResult{.Retry = false, .ByteCount = bytesWritten};
    }

    void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    void SetResult() override
    {
        ResultPromise_.Set();
    }

    TFuture<void> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

protected:
    bool IsWriteComplete(const TErrorOr<TIOResult>& result)
    {
        return result.IsOK() && !result.Value().Retry;
    }

private:
    const TSharedRef Buffer_;
    const TPromise<void> ResultPromise_ = NewPromise<void>();

    size_t Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_
class TDeliveryFencedWriteOperationOld
    : public TWriteOperation
{
public:
    TDeliveryFencedWriteOperationOld(TSharedRef buffer, std::string pipePath)
        : TWriteOperation(std::move(buffer))
        , PipePath_(std::move(pipePath))
    { }

    TErrorOr<TIOResult> PerformIO(TFileDescriptor fd) override
    {
        auto result = TWriteOperation::PerformIO(fd);
        if (IsWriteComplete(result)) {
            int flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
            int fd = HandleEintr(::open, PipePath_.data(), flags);
            if (fd == -1) {
                return MakeSystemError("Failed to open file descriptor");
            }

            auto bytesLeftOrError = CheckPipeBytesLeftToRead(fd);

            YT_VERIFY(TryClose(fd, /*ignoreBadFD*/ false));

            if (!bytesLeftOrError.IsOK()) {
                YT_LOG_ERROR(bytesLeftOrError, "Delivery fenced write failed");
                return bytesLeftOrError;
            } else {
                YT_LOG_DEBUG("Delivery fenced write pipe check finished (BytesLeft: %v)", bytesLeftOrError.Value());
            }

            result.Value().Retry = (bytesLeftOrError.Value() != 0);
        } else {
            YT_LOG_DEBUG("Delivery fenced write to pipe step finished (Result: %v)", result);
        }

        return result;
    }

private:
    const std::string PipePath_;
};

class TDeliveryFencedWriteOperation
    : public TWriteOperation
{
public:
    TDeliveryFencedWriteOperation(TSharedRef buffer, TFileDescriptor writeFd, TFileDescriptor readFd)
        : TWriteOperation(std::move(buffer))
        , WriteFD_(writeFd)
        , ReadFD_(readFd)
    { }

    TErrorOr<TIOResult> PerformIO(TFileDescriptor fd) override
    {
        if (!std::exchange(FirstWritingAttempt_, false)) {
            auto errorOrIsSignalConsumed = ConsumeSignalFdEventsAndCheckItContainsOurFD(fd);

            if (!errorOrIsSignalConsumed.IsOK()) {
                return errorOrIsSignalConsumed;
            }

            if (!errorOrIsSignalConsumed.Value()) {
                return TIOResult{
                    .Retry = true,
                    .ByteCount = 0,
                };
            }
        }

        auto result = TWriteOperation::PerformIO(WriteFD_);
        if (IsWriteComplete(result)) {
            auto bytesLeftOrError = CheckPipeBytesLeftToRead(ReadFD_);

            if (!bytesLeftOrError.IsOK()) {
                YT_LOG_ERROR(bytesLeftOrError, "Delivery fenced write failed");
                return bytesLeftOrError;
            } else {
                YT_LOG_DEBUG("Delivery fenced write pipe check finished (BytesLeft: %v)", bytesLeftOrError.Value());
            }

            result.Value().Retry = (bytesLeftOrError.Value() != 0);
        } else {
            YT_LOG_DEBUG("Delivery fenced write to pipe step finished (Result: %v)", result);
        }

        return result;
    }

private:
    const TFileDescriptor WriteFD_;
    const TFileDescriptor ReadFD_;

    bool FirstWritingAttempt_ = true;

    TErrorOr<bool> ConsumeSignalFdEventsAndCheckItContainsOurFD(TFileDescriptor fd)
    {
        bool signalForWriteFDConsumed = false;
        while (true) {
            struct signalfd_siginfo fdsi;
            ssize_t s = HandleEintr(::read, fd, &fdsi, sizeof(fdsi));
            if (s <= 0) {
                break;
            }
            if (s != sizeof(fdsi)) {
                return MakeSystemError("Invalid signalfd_siginfo size");
            }

            if (fdsi.ssi_signo != static_cast<uint32_t>(DeliveryFencedWriteSignal)) {
                continue;
            }

            if (fdsi.ssi_fd != WriteFD_) {
                continue;
            }

            signalForWriteFDConsumed = true;
        }

        return signalForWriteFDConsumed;
    }
};

#endif // _linux_

////////////////////////////////////////////////////////////////////////////////

class TWriteVOperation
    : public IIOOperation
{
public:
    explicit TWriteVOperation(TSharedRefArray buffers)
        : Buffers_(std::move(buffers))
    { }

    TErrorOr<TIOResult> PerformIO(TFileDescriptor fd) override
    {
        size_t bytesWritten = 0;
        while (Index_ < Buffers_.Size()) {
            constexpr int MaxEntries = 128;
            iovec ioVectors[MaxEntries];

            ioVectors[0].iov_base = reinterpret_cast<TIOVecBasePtr>(const_cast<char*>(Buffers_[Index_].Begin() + Position_));
            ioVectors[0].iov_len = Buffers_[Index_].Size() - Position_;

            size_t ioVectorsCount = 1;
            for (; ioVectorsCount < MaxEntries && ioVectorsCount + Index_ < Buffers_.Size(); ++ioVectorsCount) {
                const auto& ref = Buffers_[Index_ + ioVectorsCount];

                ioVectors[ioVectorsCount].iov_base = reinterpret_cast<TIOVecBasePtr>(const_cast<char*>(ref.Begin()));
                ioVectors[ioVectorsCount].iov_len = ref.Size();
            }

            ssize_t size = HandleEintr(::writev, fd, ioVectors, ioVectorsCount);

            if (size == -1) {
                if (GetLastNetworkError() == EWOULDBLOCK) {
                    return TIOResult{.Retry = true, .ByteCount = bytesWritten};
                }

                return MakeSystemError("Write failed");
            }

            YT_VERIFY(size > 0);
            bytesWritten += size;
            Position_ += size;

            while (Index_ != Buffers_.Size() && Position_ >= Buffers_[Index_].Size()) {
                Position_ -= Buffers_[Index_].Size();
                Index_++;
            }
        }
        return TIOResult{.Retry = false, .ByteCount = bytesWritten};
    }

    void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    void SetResult() override
    {
        ResultPromise_.Set();
    }

    TFuture<void> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

private:
    const TSharedRefArray Buffers_;
    const TPromise<void> ResultPromise_ = NewPromise<void>();

    size_t Index_ = 0;
    size_t Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TShutdownOperation
    : public IIOOperation
{
public:
    explicit TShutdownOperation(bool shutdownRead)
        : ShutdownRead_(shutdownRead)
    { }

    TErrorOr<TIOResult> PerformIO(TFileDescriptor fd) override
    {
        int res = HandleEintr(::shutdown, fd, ShutdownRead_ ? SHUT_RD : SHUT_WR);
        if (res == -1) {
            return MakeSystemError("Shutdown failed");
        }
        return TIOResult{.Retry = false, .ByteCount = 0};
    }

    void Abort(const TError& error) override
    {
        ResultPromise_.Set(error);
    }

    void SetResult() override
    {
        ResultPromise_.Set();
    }

    TFuture<void> ToFuture() const
    {
        return ResultPromise_.ToFuture();
    }

private:
    const bool ShutdownRead_;
    const TPromise<void> ResultPromise_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

// TODO(pogorelov): Make separate clases for pipe and socket connections.
class TFDConnectionImpl
    : public TPollableBase
{
    struct TIODirection;
public:
    static TFDConnectionImplPtr Create(
        TFileDescriptor fd,
        IPollerPtr poller,
        std::string filePath,
        // COMPAT(pogorelov)
        bool useDeliveryFence)
    {
    #ifndef _linux_
        THROW_ERROR_EXCEPTION_IF(useDeliveryFence, "Delivery fenced write is not supported on this platform");
    #endif // _linux_
        auto epollControl = EPollControl::Read | EPollControl::Write | EPollControl::EdgeTriggered;
        auto readEpollControl = EPollControl::Read;
        auto writeEpollControl = EPollControl::Write;
        auto impl = New<TFDConnectionImpl>(
            fd,
            epollControl,
            readEpollControl,
            writeEpollControl,
            std::move(poller),
            std::move(filePath),
            useDeliveryFence);
        impl->Init();
        return impl;
    }

    static TFDConnectionImplPtr Create(
        TFileDescriptor fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        IPollerPtr poller)
    {
        auto epollControl = EPollControl::Read | EPollControl::Write | EPollControl::EdgeTriggered | EPollControl::ReadHup;
        auto readEpollControl = EPollControl::Read;
        auto writeEpollControl = EPollControl::Write;
        auto impl = New<TFDConnectionImpl>(
            fd,
            epollControl,
            readEpollControl,
            writeEpollControl,
            localAddress,
            remoteAddress,
            std::move(poller));
        impl->Init();

        return impl;
    }

    const std::string& GetLoggingTag() const override
    {
        return LoggingTag_;
    }

    void OnEvent(EPollControl control) override
    {
        DoIO(GetPtr(WriteDirection_), Any(control & WriteEpollControl_));
        DoIO(GetPtr(ReadDirection_), Any(control & ReadEpollControl_));

        if (Any(control & EPollControl::ReadHup)) {
            OnPeerDisconnected();
        }
    }

    void OnShutdown() override
    {
        // Poller guarantees that OnShutdown is never executed concurrently with OnEvent()
        {
            auto guard = Guard(Lock_);

            YT_VERIFY(!ReadDirection_.Running);
            YT_VERIFY(!WriteDirection_.Running);

            auto error = AnnotateError(TError("Connection is shut down"));
            if (WriteError_.IsOK()) {
                WriteError_ = error;
            }
            if (ReadError_.IsOK()) {
                ReadError_ = error;
            }

            ShutdownRequested_ = true;

            TDelayedExecutor::CancelAndClear(WriteTimeoutCookie_);
            TDelayedExecutor::CancelAndClear(ReadTimeoutCookie_);

            if (SynchronousIOCount_ > 0) {
                return;
            }
            ReadDirection_.OnShutdown();
            WriteDirection_.OnShutdown();
        }

        if (ReadDirection_.Operation) {
            ReadDirection_.Operation->Abort(ReadError_);
            ReadDirection_.Operation.reset();
        }
        if (WriteDirection_.Operation) {
            WriteDirection_.Operation->Abort(WriteError_);
            WriteDirection_.Operation.reset();
        }

        Poller_->Unarm(FD_, this);
        YT_VERIFY(TryClose(FD_, /*ignoreBadFD*/ false));
        FD_ = -1;

        OnPeerDisconnected();
        ShutdownPromise_.Set();
    }

    TFuture<size_t> Read(const TSharedMutableRef& data)
    {
        auto read = std::make_unique<TReadOperation>(data);
        auto future = read->ToFuture();
        StartIO(GetPtr(ReadDirection_), std::move(read));
        return future;
    }

    TFuture<std::pair<size_t, TNetworkAddress>> ReceiveFrom(const TSharedMutableRef& buffer)
    {
        auto receive = std::make_unique<TReceiveFromOperation>(buffer);
        auto future = receive->ToFuture();
        StartIO(GetPtr(ReadDirection_), std::move(receive));
        return future;
    }

    void SendTo(const TSharedRef& buffer, const TNetworkAddress& address)
    {
        auto guard = TSynchronousIOGuard(this);
        auto res = HandleEintr(
            ::sendto,
            FD_,
            buffer.Begin(),
            buffer.Size(),
            0, // flags
            address.GetSockAddr(),
            address.GetLength());
        if (res == -1) {
            THROW_ERROR_EXCEPTION(MakeSystemError("Write failed"));
        }
    }

    bool SetNoDelay()
    {
        auto guard = TSynchronousIOGuard(this);
        return TrySetSocketNoDelay(FD_);
    }

    bool SetKeepAlive()
    {
        auto guard = TSynchronousIOGuard(this);
        return TrySetSocketKeepAlive(FD_);
    }

    TFuture<void> Write(const TSharedRef& data)
    {
    #ifdef _linux_
        auto writeOperation = UseDeliveryFence_
            ? std::make_unique<TDeliveryFencedWriteOperationOld>(data, PipePath_)
            : std::make_unique<TWriteOperation>(data);
    #else // _linux_
        auto writeOperation = std::make_unique<TWriteOperation>(data);
        YT_VERIFY(!UseDeliveryFence_);
    #endif // _linux_

        auto future = writeOperation->ToFuture();

        DoWrite(std::move(writeOperation));

        return future;
    }

    // forcefullyConsiderPending used to perform first io without waiting event on epoll.
    // See TDeliveryFencedWriteConnection.
    void DoWrite(std::unique_ptr<IIOOperation> operation, bool forcefullyConsiderPending = false)
    {
        StartIO(GetPtr(WriteDirection_), std::move(operation), forcefullyConsiderPending);
    }

    TFuture<void> WriteV(const TSharedRefArray& data)
    {
        auto writeV = std::make_unique<TWriteVOperation>(data);
        auto future = writeV->ToFuture();
        StartIO(GetPtr(WriteDirection_), std::move(writeV));
        return future;
    }

    TFuture<void> Close()
    {
        YT_LOG_DEBUG("Closing connection");
        return AbortIO(TError("Connection closed"));
    }

    bool IsIdle()
    {
        auto guard = Guard(Lock_);
        return
            ReadError_.IsOK() &&
            WriteError_.IsOK() &&
            !WriteDirection_.Operation &&
            !ReadDirection_.Operation &&
            SynchronousIOCount_ == 0 &&
            !PeerDisconnectedList_.IsFired();
    }

    bool IsReusable()
    {
        return IsIdle();
    }

    TFuture<void> Abort(TError error)
    {
        YT_LOG_DEBUG(error, "Aborting connection");
        return AbortIO(std::move(error));
    }

    TFuture<void> CloseRead()
    {
        auto shutdownRead = std::make_unique<TShutdownOperation>(true);
        auto future = shutdownRead->ToFuture();
        StartIO(GetPtr(ReadDirection_), std::move(shutdownRead));
        return future;
    }

    TFuture<void> CloseWrite()
    {
        auto shutdownWrite = std::make_unique<TShutdownOperation>(false);
        auto future = shutdownWrite->ToFuture();
        StartIO(GetPtr(WriteDirection_), std::move(shutdownWrite));
        return future;
    }

    TConnectionId GetId() const
    {
        return Id_;
    }

    const TNetworkAddress& GetLocalAddress() const
    {
        return LocalAddress_;
    }

    const TNetworkAddress& GetRemoteAddress() const
    {
        return RemoteAddress_;
    }

    TFileDescriptor GetHandle() const
    {
        return FD_;
    }

    i64 GetReadByteCount() const
    {
        return ReadDirection_.BytesTransferred;
    }

    i64 GetWriteByteCount() const
    {
        return WriteDirection_.BytesTransferred;
    }

    TConnectionStatistics GetReadStatistics() const
    {
        auto guard = Guard(Lock_);
        return ReadDirection_.GetStatistics();
    }

    TConnectionStatistics GetWriteStatistics() const
    {
        auto guard = Guard(Lock_);
        return WriteDirection_.GetStatistics();
    }

    void SetReadDeadline(std::optional<TInstant> deadline)
    {
        auto guard = Guard(Lock_);

        if (ShutdownRequested_) {
            return;
        }

        TDelayedExecutor::CancelAndClear(ReadTimeoutCookie_);

        if (deadline) {
            ReadTimeoutCookie_ = TDelayedExecutor::Submit(AbortFromReadTimeout_, *deadline);
        }
    }

    void SetWriteDeadline(std::optional<TInstant> deadline)
    {
        auto guard = Guard(Lock_);

        if (ShutdownRequested_) {
            return;
        }

        TDelayedExecutor::CancelAndClear(WriteTimeoutCookie_);

        if (deadline) {
            WriteTimeoutCookie_ = TDelayedExecutor::Submit(AbortFromWriteTimeout_, *deadline);
        }
    }

    void SubscribePeerDisconnect(TCallback<void()> callback)
    {
        PeerDisconnectedList_.Subscribe(std::move(callback));
    }

protected:
    const TConnectionId Id_ = TConnectionId::Create();
    const std::string Endpoint_;
    const std::string LoggingTag_;
    const NLogging::TLogger Logger;
    TFileDescriptor FD_ = -1;
    int SynchronousIOCount_ = 0;

    TFDConnectionImpl(
        TFileDescriptor fd,
        EPollControl FDEpollControl,
        EPollControl readEpollControl,
        EPollControl writeEpollControl,
        IPollerPtr poller,
        std::string filePath,
        // COMPAT(pogorelov)
        bool useDeliveryFence)
        : Endpoint_(Format("File{%v}", filePath))
        , LoggingTag_(MakeLoggingTag(Id_, Endpoint_))
        , Logger(NetLogger().WithRawTag(LoggingTag_))
        , FD_(fd)
        , FDEpollControl_(FDEpollControl)
        , ReadEpollControl_(readEpollControl)
        , WriteEpollControl_(writeEpollControl)
        , Poller_(std::move(poller))
        , UseDeliveryFence_(useDeliveryFence)
        , PipePath_(std::move(filePath))
    { }

    TFDConnectionImpl(
        TFileDescriptor fd,
        EPollControl epollControl,
        EPollControl readEpollControl,
        EPollControl writeEpollControl,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        IPollerPtr poller)
        : Endpoint_(Format("FD{%v<->%v}", localAddress, remoteAddress))
        , LoggingTag_(MakeLoggingTag(Id_, Endpoint_))
        , Logger(NetLogger().WithRawTag(LoggingTag_))
        , FD_(fd)
        , FDEpollControl_(epollControl)
        , ReadEpollControl_(readEpollControl)
        , WriteEpollControl_(writeEpollControl)
        , LocalAddress_(localAddress)
        , RemoteAddress_(remoteAddress)
        , Poller_(std::move(poller))
    { }

    ~TFDConnectionImpl()
    {
        YT_LOG_DEBUG("Connection destroyed");
    }

    void Arm(EPollControl additionalFlags = {})
    {
        Poller_->Arm(FD_, this, FDEpollControl_ | additionalFlags);
    }

    bool TryRegister()
    {
        return Poller_->TryRegister(MakeStrong(this));
    }

private:
    const EPollControl FDEpollControl_;
    const EPollControl ReadEpollControl_;
    const EPollControl WriteEpollControl_;
    const TNetworkAddress LocalAddress_;
    const TNetworkAddress RemoteAddress_;
    const IPollerPtr Poller_;

    // COMPAT(pogorelov)
    // If set to true via ctor argument |useDeliveryFence| will use
    // DeliverFencedWriteOperations instead of WriteOperations,
    // which future is set only after data from pipe has been read.
    const bool UseDeliveryFence_ = false;
    const std::string PipePath_;

    DECLARE_NEW_FRIEND()

    class TSynchronousIOGuard
    {
    public:
        explicit TSynchronousIOGuard(TFDConnectionImplPtr owner)
            : Owner_(std::move(owner))
        {
            auto guard = Guard(Owner_->Lock_);
            Owner_->WriteError_.ThrowOnError();
            Owner_->ReadError_.ThrowOnError();
            ++Owner_->SynchronousIOCount_;
        }

        ~TSynchronousIOGuard()
        {
            if (Owner_) {
                auto guard = Guard(Owner_->Lock_);
                YT_VERIFY(Owner_->SynchronousIOCount_ > 0);
                if (--Owner_->SynchronousIOCount_ == 0 &&
                    Owner_->ShutdownRequested_)
                {
                    guard.Release();
                    Owner_->OnShutdown();
                }
            }
        }

        TSynchronousIOGuard(const TSynchronousIOGuard&) = delete;
        TSynchronousIOGuard(TSynchronousIOGuard&&) = default;

        TSynchronousIOGuard& operator=(const TSynchronousIOGuard&) = delete;
        TSynchronousIOGuard& operator=(TSynchronousIOGuard&&) = default;

    private:
        const TFDConnectionImplPtr Owner_;
    };

    enum class EDirection
    {
        Read,
        Write
    };

    struct TIODirection
    {
        explicit TIODirection(EDirection direction)
            : Direction(direction)
        { }

        std::unique_ptr<IIOOperation> Operation;
        std::atomic<i64> BytesTransferred = 0;
        TDuration IdleDuration;
        TDuration BusyDuration;
        TCpuInstant StartTime = GetCpuInstant();
        std::optional<TCpuInstant> EndTime;
        EDirection Direction;
        bool Pending = false;
        bool Running = false;

        void StartBusyTimer()
        {
            auto now = GetCpuInstant();
            IdleDuration += CpuDurationToDuration(now - StartTime);
            StartTime = now;
        }

        void StopBusyTimer()
        {
            auto now = GetCpuInstant();
            BusyDuration += CpuDurationToDuration(now - StartTime);
            StartTime = now;
        }

        void OnShutdown()
        {
            EndTime = GetCpuInstant();
        }

        TConnectionStatistics GetStatistics() const
        {
            TConnectionStatistics statistics{IdleDuration, BusyDuration};
            auto lastEventTime = EndTime.value_or(GetCpuInstant());
            (Operation ? statistics.BusyDuration : statistics.IdleDuration) += CpuDurationToDuration(lastEventTime - StartTime);
            return statistics;
        }
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TIODirection ReadDirection_{EDirection::Read};
    TIODirection WriteDirection_{EDirection::Write};
    bool ShutdownRequested_ = false;
    TError WriteError_;
    TError ReadError_;
    const TPromise<void> ShutdownPromise_ = NewPromise<void>();

    TSingleShotCallbackList<void()> PeerDisconnectedList_;

    TClosure AbortFromReadTimeout_;
    TClosure AbortFromWriteTimeout_;

    TDelayedExecutorCookie ReadTimeoutCookie_;
    TDelayedExecutorCookie WriteTimeoutCookie_;

    static std::string MakeLoggingTag(TConnectionId id, const std::string& endpoint)
    {
       return Format(
            "ConnectionId: %v, Endpoint: %v",
            id,
            endpoint);
    }

    TError AnnotateError(TError error) const
    {
        return std::move(error)
            << TErrorAttribute("connection_id", Id_)
            << TErrorAttribute("connection_endpoint", Endpoint_);
    }

    void Init()
    {
        YT_LOG_DEBUG("Connection created");

        AbortFromReadTimeout_ = BIND(&TFDConnectionImpl::AbortFromReadTimeout, MakeWeak(this));
        AbortFromWriteTimeout_ = BIND(&TFDConnectionImpl::AbortFromWriteTimeout, MakeWeak(this));

        if (!TryRegister()) {
            ReadError_ = WriteError_ = AnnotateError(TError("Cannot register connection pollable"));
            return;
        }

        Arm();
    }

    TError GetCurrentError(EDirection direction)
    {
        switch (direction) {
            case EDirection::Read:
                return ReadError_;
            case EDirection::Write:
                // We want to read if there were write errors before, but we don't want to write if there were read errors,
                // because it looks useless.
                if (!WriteError_.IsOK()) {
                    return WriteError_;
                }
                return ReadError_;
        }
    }

    void StartIO(TNonNullPtr<TIODirection> direction, std::unique_ptr<IIOOperation> operation, bool forcefullyConsiderPending = false)
    {
        TError error;
        bool needRetry = false;

        {
            auto guard = Guard(Lock_);

            error = GetCurrentError(direction->Direction);
            if (error.IsOK()) {
                if (direction->Operation) {
                    THROW_ERROR(AnnotateError(TError("Another IO operation is in progress")));
                }

                YT_VERIFY(!direction->Running);
                direction->Operation = std::move(operation);
                direction->StartBusyTimer();

                direction->Pending |= forcefullyConsiderPending;

                // Start operation only if this direction already has pending
                // event otherwise reading from FIFO before opening by writer
                // will return EOF immediately.
                needRetry = direction->Pending;
            }
        }

        if (!error.IsOK()) {
            operation->Abort(std::move(error));
            return;
        }

        if (needRetry) {
            Poller_->Retry(this);
        }
    }

    void DoIO(TNonNullPtr<TIODirection> direction, bool event)
    {
        {
            auto guard = Guard(Lock_);

            if (!event && !direction->Pending) {
                return;
            }

            auto error = GetCurrentError(direction->Direction);
            if (!error.IsOK()) {
                return;
            }

            if (!direction->Operation || direction->Running) {
                direction->Pending |= event;
                return;
            }

            direction->Pending = false;
            direction->Running = true;
        }

        auto result = direction->Operation->PerformIO(FD_);
        if (result.IsOK()) {
            direction->BytesTransferred += result.Value().ByteCount;
        } else {
            result = AnnotateError(std::move(result));
        }

        bool needUnregister = false;
        bool needRetry = false;
        bool needRearm = false;
        std::unique_ptr<IIOOperation> operation;
        {
            auto guard = Guard(Lock_);
            direction->Running = false;

            auto error = GetCurrentError(direction->Direction);

            if (!result.IsOK()) {
                // IO finished with error.
                operation = std::move(direction->Operation);
                auto& directionError = direction->Direction == EDirection::Read ? ReadError_ : WriteError_;
                if (directionError.IsOK()) {
                    directionError = result;
                    if (direction->Direction == EDirection::Read) {
                        Poller_->Unarm(FD_, this);
                        needUnregister = true;
                    }
                }
                direction->StopBusyTimer();
            } else if (!error.IsOK()) {
                // IO was aborted.
                operation = std::move(direction->Operation);
                // Avoid aborting completed IO.
                if (result.Value().Retry) {
                    result = error;
                }
                direction->Pending = true;
                direction->StopBusyTimer();
            } else if (result.Value().Retry) {
                // IO not completed. Retry if have pending backlog.
                // If dont have pending backlog, just subscribe for further notifications.
                if (direction->Pending) {
                    needRetry = true;
                } else {
                    needRearm = true;
                }
            } else {
                // IO finished successfully.
                operation = std::move(direction->Operation);
                // TODO not set pending if no backlog after short read/write
                direction->Pending = true;
                direction->StopBusyTimer();
            }

            if (needRearm) {
                YT_VERIFY(!needRetry && !needUnregister);
                Arm(EPollControl::BacklogEmpty);
            }
        }

        if (!result.IsOK()) {
            operation->Abort(std::move(result));
        } else if (!result.Value().Retry) {
            operation->SetResult();
        } else if (needRetry) {
            Poller_->Retry(this);
        }

        if (needUnregister) {
            YT_UNUSED_FUTURE(Poller_->Unregister(this));
        }
    }

    TFuture<void> AbortIO(TError error)
    {
        auto annotatedError = AnnotateError(std::move(error));

        auto guard = Guard(Lock_);
        // In case of read errors we have called Unarm and Unregister already.
        bool needUnarmAndUnregister = ReadError_.IsOK();
        if (WriteError_.IsOK()) {
            WriteError_ = annotatedError;
        }
        if (ReadError_.IsOK()) {
            ReadError_ = annotatedError;
        }
        if (needUnarmAndUnregister) {
            Poller_->Unarm(FD_, this);
            guard.Release();
            YT_UNUSED_FUTURE(Poller_->Unregister(this));
        }
        return ShutdownPromise_.ToFuture();
    }

    void AbortFromReadTimeout()
    {
        YT_UNUSED_FUTURE(Abort(TError(NYT::EErrorCode::Timeout, "Read timeout")));
    }

    void AbortFromWriteTimeout()
    {
        YT_UNUSED_FUTURE(Abort(TError(NYT::EErrorCode::Timeout, "Write timeout")));
    }

    void OnPeerDisconnected()
    {
        if (PeerDisconnectedList_.Fire()) {
            YT_LOG_DEBUG("Peer disconnected");
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TFDConnectionImpl)

#ifdef _linux_
class TDeliveryFencedWriteConnectionImpl
    : public TFDConnectionImpl
{
public:
    static TDeliveryFencedWriteConnectionImplPtr Create(
        IPollerPtr poller,
        std::string pipePath,
        std::optional<int> capacity)
    {
        TFileDescriptorGuard signalFD = CreateSignalFD();
        TDeliveryFencedWriteConnectionImplPtr impl;
        impl = New<TDeliveryFencedWriteConnectionImpl>(std::move(poller), std::move(pipePath), signalFD.Get(), capacity);
        impl->Init();

        signalFD.Release();
        return impl;
    }

    TFuture<void> Write(const TSharedRef& data)
    {
        auto writeOperation = std::make_unique<TDeliveryFencedWriteOperation>(data, WriteFD_, ReadFD_);

        auto future = writeOperation->ToFuture();

        DoWrite(std::move(writeOperation), /*forcefullyConsiderPending*/ true);

        return future;
    }

private:
    const std::string PipePath_;
    const std::optional<int> PipeCapacity_;

    TFileDescriptor WriteFD_ = -1;
    TFileDescriptor ReadFD_ = -1;

    TDeliveryFencedWriteConnectionImpl(
        IPollerPtr poller,
        std::string pipePath,
        TFileDescriptor signalFD,
        std::optional<int> capacity)
        : TFDConnectionImpl(
            signalFD,
            /*FDEpollControl*/ EPollControl::Read | EPollControl::EdgeTriggered,
            /*readEpollControll*/ EPollControl::None,
            // Yes, we read to write :)
            /*writeEpollControl*/ EPollControl::Read,
            std::move(poller),
            pipePath,
            // NB(pogorelov): DeliveryFence is old compat logic, so we turn it off here.
            /*useDeliveryFence*/ false)
        , PipePath_(std::move(pipePath))
        , PipeCapacity_(capacity)
    {
        YT_LOG_DEBUG("Delivery fenced connection created");
    }

    ~TDeliveryFencedWriteConnectionImpl()
    {
        YT_LOG_DEBUG("Delivery fenced connection destroyed");
    }

    DECLARE_NEW_FRIEND();

    static TFileDescriptor CreateSignalFD()
    {
        sigset_t mask;
        if (sigemptyset(&mask) == -1) {
            ThrowError("empty sig set");
        }
        if (sigaddset(&mask, DeliveryFencedWriteSignal) == -1) {
            ThrowError(Format("add %v RT signal to sig set", DeliveryFencedWriteSignal - SIGRTMIN));
        }

        auto fd = HandleEintr(::signalfd, -1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
        if (fd == -1) {
            ThrowError("open signalfd");
        }

        return fd;
    }

    void Init()
    {
        TFileDescriptorGuard readFdGuard = HandleEintr(::open, PipePath_.data(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);
        if (readFdGuard.Get() == -1) {
            ThrowError("open pipe for reading");
        }

        TFileDescriptorGuard writeFdGuard = HandleEintr(::open, PipePath_.data(), O_WRONLY | O_CLOEXEC);
        if (writeFdGuard.Get() == -1) {
            ThrowError("open pipe for writing");
        }

        auto flags = fcntl(writeFdGuard.Get(), F_GETFL);
        if (flags == -1) {
            ThrowError("get pipe writing fd flags");
        }
        if (fcntl(writeFdGuard.Get(), F_SETFL, flags | O_NONBLOCK | FASYNC) == -1) {
            ThrowError("set pipe writing fd flags");
        }
        if (fcntl(writeFdGuard.Get(), F_SETOWN, getpid()) == -1) {
            ThrowError("set pipe owner");
        }
        if (fcntl(writeFdGuard.Get(), F_SETSIG, DeliveryFencedWriteSignal) == -1) {
            ThrowError("set custom pipe signal");
        }
        if (PipeCapacity_) {
            SafeSetPipeCapacity(writeFdGuard.Get(), *PipeCapacity_);
        }

        if (!TryRegister()) {
            ThrowError("register connection in poller");
        }

        try {
            Arm();
        } catch (const std::exception& ex) {
            ThrowError("arm connection", ex);
        } catch (...) {
            ThrowError("arm connection");
        }

        YT_LOG_DEBUG("Delivery fenced connection initialized");

        ReadFD_ = readFdGuard.Release();
        WriteFD_ = writeFdGuard.Release();
    }

    void OnShutdown() final
    {
        TFDConnectionImpl::OnShutdown();

        YT_VERIFY(SynchronousIOCount_ == 0);

        YT_VERIFY(TryClose(WriteFD_, /*ignoreBadFD*/ false));
        YT_VERIFY(TryClose(ReadFD_, /*ignoreBadFD*/ false));
    }

    [[noreturn]] static void ThrowError(std::string_view action, TError innerError = TError())
    {
        auto error = TError("Failed to %v for delivery fenced connection", action);
        if (!innerError.IsOK()) {
            error <<= std::move(innerError);
        } else {
            error <<= TError::FromSystem();
        }
        THROW_ERROR(std::move(error));
    }
};

DEFINE_REFCOUNTED_TYPE(TDeliveryFencedWriteConnectionImpl)

#endif // _linux_

////////////////////////////////////////////////////////////////////////////////

// TODO(pogorelov): Make separate clases for pipe and socket connections.
// The sole purpose of this class is to call Abort on Impl in dtor.
// Since object of TFDConnection is created, you should not care about fd.
// But in case of exception you should close fd by yourself.
class TFDConnection
    : public TReadWriteConnectionBase<TFDConnection>
{
public:
    TFDConnection(
        TFileDescriptor fd,
        IPollerPtr poller,
        TRefCountedPtr pipeHolder,
        std::string pipePath = "",
        // COMPAT(pogorelov)
        bool useDeliveryFence = false)
        : Impl_(TFDConnectionImpl::Create(fd, std::move(poller), std::move(pipePath), useDeliveryFence))
        , PipeHolder_(std::move(pipeHolder))
    { }

    TFDConnection(
        TFileDescriptor fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        IPollerPtr poller)
        : Impl_(TFDConnectionImpl::Create(fd, localAddress, remoteAddress, std::move(poller)))
    { }

    TFDConnection(
        TRefCountedPtr pipeHolder,
        TFDConnectionImplPtr impl)
        : Impl_(std::move(impl))
        , PipeHolder_(std::move(pipeHolder))
    { }

    ~TFDConnection()
    {
        YT_UNUSED_FUTURE(Impl_->Abort(TError("Connection is abandoned")));
    }
    const TFDConnectionImplPtr& GetImpl() const
    {
        return Impl_;
    }

private:
    const TFDConnectionImplPtr Impl_;
    const TRefCountedPtr PipeHolder_;
};

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

[[noreturn]] void ThrowUnimplemented(std::string_view method)
{
    THROW_ERROR_EXCEPTION("%v is not supported", method);
}

// NOTE(pogorelov): Using 2 TDeliveryFencedWriteConnection concurrently is not supported.
// Desired behavior:
// Write should complete only after all data has been written to the pipe and fully read by the reader.
// Yes, this is not what pipes were originally designed for.
// However, we now have a large amount of user code relying on our API (which currently only reads from stdin).
// We want to be able to adjust the supplying data portion size without modifying the API, hence this solution.
//
// Implementation:
// To achieve this behavior, we use:
//  - signalfd (for signal-based notifications)
//  - FASYNC mode (for asynchronous pipe I/O)
//  - FIONREAD (to check remaining data in the pipe)
//
// Note about FASYNC Behavior:
// Starting from kernel 5.15, the writer receives a signal (SIGIO) on every read operation from the pipe.
// In earlier kernel versions, this behavior has been changed several times, but it appears to be finalized in 5.15 and later:
// https://web.git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit?id=fe67f4dd8daa252eb9aa7acb61555f3cc3c1ce4c
//
// Initialization Steps:
// 1) Configure the pipe for signals:
//      - Set the process as the FIFO owner (F_SETOWN).
//      - Enable FASYNC on the write-side file descriptor (F_SETFL) to receive I/O event signals.
//      - Use a custom real-time signal (instead of SIGIO) to not occupy SIGIO.
//      - We use real-time signal as a reserve for the future
//        (when RT signal is used, kernel stores the queue with signal and its metadata such as fd it triggered by).
// 2) Block the custom signal for the entire process (sigprocmask).
// 3) Create a signalfd to read signals via a file descriptor.
// 4) Arm epoll for the signal FD:
//      Monitor for EPOLLIN (even though this is the writer side—we’re waiting to read signals).
//
// Write Steps:
// 1) Write data to the pipe:
//      Write until all data is sent or EWOULDBLOCK is encountered.
//      (Unlike common writer, we don’t immediately sleep on epoll.
//      Opening the pipe for reading wakes the writer, but FASYNC does not send SIGIO in this case.)
// 2) Check pipe status with FIONREAD:
//      If the pipe is empty, writing is complete.
// 3) Wait for signals via epoll.
// 4) Process signals from signalfd:
//      Read all the signals and repeat from Step 1.
class TDeliveryFencedWriteConnection
    : public TWriteConnectionBase<TDeliveryFencedWriteConnection>
{
public:
    TDeliveryFencedWriteConnection(
        TRefCountedPtr pipeHolder,
        IPollerPtr poller,
        std::string pipePath,
        std::optional<int> capacity)
        : Impl_(TDeliveryFencedWriteConnectionImpl::Create(std::move(poller), std::move(pipePath), capacity))
        , PipeHolder_(std::move(pipeHolder))
    {
        YT_VERIFY(!HasActiveConnection.exchange(true));
    }

    ~TDeliveryFencedWriteConnection()
    {
        YT_VERIFY(HasActiveConnection.exchange(false));
        YT_UNUSED_FUTURE(Impl_->Abort(TError("Connection is abandoned")));
    }
    TFuture<void> WriteV(const TSharedRefArray& /*data*/) final
    {
        ThrowUnimplemented("WriteV");
    }

    TFuture<void> CloseWrite() final
    {
        ThrowUnimplemented("CloseWrite");;
    }

    void SetWriteDeadline(std::optional<TInstant> /*deadline*/)
    {
        ThrowUnimplemented("SetWriteDeadline");
    }

    TFuture<void> Write(const TSharedRef& data) final
    {
        return static_cast<TDeliveryFencedWriteConnectionImpl&>(*Impl_).Write(data);
    }

    const TDeliveryFencedWriteConnectionImplPtr& GetImpl() const
    {
        return Impl_;
    }

private:
    const TDeliveryFencedWriteConnectionImplPtr Impl_;
    const TRefCountedPtr PipeHolder_;

    static inline std::atomic<bool> HasActiveConnection = false;
};

#endif // _linux_

////////////////////////////////////////////////////////////////////////////////

namespace {

TFileDescriptor CreateWriteFDForConnection(
    const std::string& pipePath,
    std::optional<int> capacity,
    // COMPAT(pogorelov)
    bool useDeliveryFence)
{
#ifdef _unix_
    int flags = O_WRONLY | O_CLOEXEC;
    TFileDescriptorGuard fd = HandleEintr(::open, pipePath.c_str(), flags);
    if (fd.Get() == -1) {
        THROW_ERROR_EXCEPTION(MakeSystemError("Failed to open named pipe"))
            << TErrorAttribute("path", pipePath);
    }

    try {
        if (capacity) {
            SafeSetPipeCapacity(fd.Get(), *capacity);
        }

        if (useDeliveryFence) {
            SafeEnableEmptyPipeEpollEvent(fd.Get());
        }

        SafeMakeNonblocking(fd.Get());
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(
            TError(ex) << TError::FromSystem(),
            "Failed to open pipe for writing (UseDeliveryFence: %v, Capacity: %v)",
            useDeliveryFence,
            capacity);
        throw;
    } catch (...) {
        YT_LOG_WARNING(
            "Failed to open pipe for writing (MaybeRelevantError: %v, UseDeliveryFence: %v, Capacity: %v)",
            TError::FromSystem(),
            useDeliveryFence,
            capacity);
        throw;
    }
    return fd.Release();
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::pair<IConnectionPtr, IConnectionPtr> CreateConnectionPair(IPollerPtr poller)
{
    SOCKET fds[2];

#ifdef _unix_
    int flags = SOCK_STREAM;

    #ifdef _linux_
        flags |= SOCK_NONBLOCK | SOCK_CLOEXEC;
    #endif

    if (HandleEintr(::socketpair, AF_LOCAL, flags, 0, fds) == -1) {
        THROW_ERROR_EXCEPTION(MakeSystemError("Failed to create socket pair"));
    }
#else
    if (SocketPair(fds, /*overlapped*/ false, /*cloexec*/ true) == SOCKET_ERROR) {
        THROW_ERROR_EXCEPTION(MakeSystemError("Failed to create socket pair"));
    }

    SetNonBlock(fds[0]);
    SetNonBlock(fds[1]);
#endif
    TFileDescriptorGuard fd0(fds[0]);
    TFileDescriptorGuard fd1(fds[1]);

    auto address0 = GetSocketName(fds[0]);
    auto address1 = GetSocketName(fds[1]);

    auto first = New<TFDConnection>(fds[0], address0, address1, poller);
    fd0.Release();
    auto second = New<TFDConnection>(fds[1], address1, address0, std::move(poller));
    fd1.Release();
    return std::pair(std::move(first), std::move(second));
}

IConnectionPtr CreateConnectionFromFD(
    TFileDescriptor fd,
    const TNetworkAddress& localAddress,
    const TNetworkAddress& remoteAddress,
    IPollerPtr poller)
{
    return New<TFDConnection>(fd, localAddress, remoteAddress, std::move(poller));
}

IConnectionReaderPtr CreateInputConnectionFromFD(
    TFileDescriptor fd,
    const std::string& /*pipePath*/,
    IPollerPtr poller,
    const TRefCountedPtr& pipeHolder)
{
    return New<TFDConnection>(fd, std::move(poller), pipeHolder);
}

IConnectionReaderPtr CreateInputConnectionFromPath(
    std::string pipePath,
    IPollerPtr poller,
    TRefCountedPtr pipeHolder)
{
#ifdef _unix_
    int flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
    TFileDescriptorGuard fd = HandleEintr(::open, pipePath.c_str(), flags);
    if (fd.Get() == -1) {
        THROW_ERROR_EXCEPTION(MakeSystemError("Failed to open named pipe"))
            << TErrorAttribute("path", pipePath);
    }

    auto connection = New<TFDConnection>(fd.Get(), std::move(poller), std::move(pipeHolder), std::move(pipePath));
    fd.Release();
    return connection;
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

IConnectionWriterPtr CreateOutputConnectionFromPath(
    std::string pipePath,
    IPollerPtr poller,
    TRefCountedPtr pipeHolder,
    std::optional<int> capacity,
    EDeliveryFencedMode deliveryFencedMode)
{
    if (deliveryFencedMode == EDeliveryFencedMode::New) {
    #ifdef _linux_
        return New<TDeliveryFencedWriteConnection>(
            std::move(pipeHolder),
            std::move(poller),
            std::move(pipePath),
            capacity);
    #else // _linux_
        THROW_ERROR_EXCEPTION("Delivery fenced write is not supported on this platform");
    #endif // _linux_
    }

    bool useDeliveryFence = deliveryFencedMode == EDeliveryFencedMode::Old;

    TFileDescriptorGuard fd = CreateWriteFDForConnection(pipePath, capacity, useDeliveryFence);
    auto connection = New<TFDConnection>(
        fd.Get(),
        std::move(poller),
        std::move(pipeHolder),
        std::move(pipePath),
        useDeliveryFence);
    fd.Release();

    return connection;
}

////////////////////////////////////////////////////////////////////////////////

class TPacketConnection
    : public IPacketConnection
{
public:
    TPacketConnection(
        TFileDescriptor fd,
        const TNetworkAddress& localAddress,
        IPollerPtr poller)
        : Impl_(TFDConnectionImpl::Create(fd, localAddress, TNetworkAddress{}, std::move(poller)))
    { }

    ~TPacketConnection()
    {
        YT_UNUSED_FUTURE(Abort());
    }

    TFuture<std::pair<size_t, TNetworkAddress>> ReceiveFrom(
        const TSharedMutableRef& buffer) override
    {
        return Impl_->ReceiveFrom(buffer);
    }

    void SendTo(const TSharedRef& buffer, const TNetworkAddress& address) override
    {
        Impl_->SendTo(buffer, address);
    }

    TFuture<void> Abort() override
    {
        return Impl_->Abort(TError("Connection is abandoned"));
    }

private:
    TFDConnectionImplPtr Impl_;
};

IPacketConnectionPtr CreatePacketConnection(
    const TNetworkAddress& at,
    NConcurrency::IPollerPtr poller)
{
    TFileDescriptorGuard fd = CreateUdpSocket();
    try {
        SetReuseAddrFlag(fd.Get());
        BindSocket(fd.Get(), at);
    } catch (...) {
        SafeClose(fd.Get(), false);
        throw;
    }

    auto connection = New<TPacketConnection>(fd.Get(), at, std::move(poller));
    fd.Release();
    return connection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
