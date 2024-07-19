#include "connection.h"
#include "packet_connection.h"
#include "private.h"

#include <yt/yt/core/concurrency/pollable_detail.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/net/socket.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <util/network/pollerimpl.h>

#include <errno.h>

#ifdef _linux_
    #include <sys/ioctl.h>
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
// using namespace NProfiling;

#ifdef _unix_
    using TIOVecBasePtr = void*;
#else
    using TIOVecBasePtr = char*;
#endif

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

enum class EPipeReadStatus
{
    PipeEmpty,
    PipeNotEmpty,
    NotSupportedError,
};

EPipeReadStatus CheckPipeReadStatus(const TString& pipePath)
{
#ifdef _linux_
    int bytesLeft = 0;

    {
        int flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
        int fd = HandleEintr(::open, pipePath.c_str(), flags);

        int ret = ::ioctl(fd, FIONREAD, &bytesLeft);
        if (ret == -1 && errno == EINVAL) {
            // Some linux platforms do not support
            // FIONREAD call. In such cases we
            // expect EINVAL error.
            return EPipeReadStatus::NotSupportedError;
        }

        SafeClose(fd, /*ignoreBadFD*/ false);
    }

    return bytesLeft == 0
        ? EPipeReadStatus::PipeEmpty
        : EPipeReadStatus::PipeNotEmpty;
#else
    Y_UNUSED(pipePath);
    return EPipeReadStatus::NotSupportedError;
#endif
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFDConnectionImpl)

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
    explicit TReadOperation(const TSharedMutableRef& buffer)
        : Buffer_(buffer)
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

                return TError("Read failed")
                    << TError::FromSystem();
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
    TSharedMutableRef Buffer_;
    size_t Position_ = 0;

    TPromise<size_t> ResultPromise_ = NewPromise<size_t>();
};

////////////////////////////////////////////////////////////////////////////////

class TReceiveFromOperation
    : public IIOOperation
{
public:
    explicit TReceiveFromOperation(const TSharedMutableRef& buffer)
        : Buffer_(buffer)
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

            return TError("Read failed")
                << TError::FromSystem();
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
    TSharedMutableRef Buffer_;
    size_t Position_ = 0;
    TNetworkAddress RemoteAddress_;

    TPromise<std::pair<size_t, TNetworkAddress>> ResultPromise_ = NewPromise<std::pair<size_t, TNetworkAddress>>();
};

////////////////////////////////////////////////////////////////////////////////

class TWriteOperation
    : public IIOOperation
{
public:
    explicit TWriteOperation(const TSharedRef& buffer)
        : Buffer_(buffer)
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
                return TError("Write failed")
                    << TError::FromSystem();
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

private:
    TSharedRef Buffer_;
    size_t Position_ = 0;

    TPromise<void> ResultPromise_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

class TDeliveryFencedWriteOperation
    : public TWriteOperation
{
public:
    TDeliveryFencedWriteOperation(const TSharedRef& buffer, TString pipePath)
        : TWriteOperation(buffer)
        , PipePath_(std::move(pipePath))
    { }

    TErrorOr<TIOResult> PerformIO(TFileDescriptor fd) override
    {
        auto result = TWriteOperation::PerformIO(fd);
        if (IsWriteComplete(result)) {
            auto pipeReadStatus = CheckPipeReadStatus(PipePath_);
            if (pipeReadStatus == EPipeReadStatus::NotSupportedError) {
                return TError("Delivery fenced write failed: FIONDREAD is not supported on your platform")
                    << TError::FromSystem();
            }

            result.Value().Retry = (pipeReadStatus != EPipeReadStatus::PipeEmpty);
        }

        return result;
    }

private:
    const TString PipePath_;

    bool IsWriteComplete(const TErrorOr<TIOResult>& result)
    {
        return result.IsOK() && !result.Value().Retry;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TWriteVOperation
    : public IIOOperation
{
public:
    explicit TWriteVOperation(const TSharedRefArray& buffers)
        : Buffers_(buffers)
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

                return TError("Write failed")
                    << TError::FromSystem();
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
    TSharedRefArray Buffers_;
    size_t Index_ = 0;
    size_t Position_ = 0;

    TPromise<void> ResultPromise_ = NewPromise<void>();
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
            return TError("Shutdown failed")
                << TError::FromSystem();
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
    TPromise<void> ResultPromise_ = NewPromise<void>();
};

////////////////////////////////////////////////////////////////////////////////

class TFDConnectionImpl
    : public TPollableBase
{
public:
    static TFDConnectionImplPtr Create(
        TFileDescriptor fd,
        TString filePath,
        IPollerPtr poller,
        bool useDeliveryFence)
    {
        auto impl = New<TFDConnectionImpl>(fd, std::move(filePath), std::move(poller), useDeliveryFence);
        impl->Init();
        return impl;
    }

    static TFDConnectionImplPtr Create(
        TFileDescriptor fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        IPollerPtr poller)
    {
        auto impl = New<TFDConnectionImpl>(fd, localAddress, remoteAddress, std::move(poller));
        impl->Init();
        return impl;
    }

    const TString& GetLoggingTag() const override
    {
        return LoggingTag_;
    }

    void OnEvent(EPollControl control) override
    {
        DoIO(&WriteDirection_, Any(control & EPollControl::Write));
        DoIO(&ReadDirection_, Any(control & EPollControl::Read));

        if (Any(control & EPollControl::ReadHup)) {
            NotifyPeerDisconnected();
        }
    }

    void OnShutdown() override
    {
        // Poller guarantees that OnShutdown is never executed concurrently with OnEvent()
        {
            auto guard = Guard(Lock_);

            YT_VERIFY(!ReadDirection_.Running);
            YT_VERIFY(!WriteDirection_.Running);

            auto shutdownError = TError("Connection is shut down");
            if (WriteError_.IsOK()) {
                WriteError_ = shutdownError;
            }
            if (ReadError_.IsOK()) {
                ReadError_ = shutdownError;
            }

            ShutdownRequested_ = true;

            TDelayedExecutor::CancelAndClear(WriteTimeoutCookie_);
            TDelayedExecutor::CancelAndClear(ReadTimeoutCookie_);

            if (SynchronousIOCount_ > 0) {
                return;
            }
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
        YT_VERIFY(TryClose(FD_, false));
        FD_ = -1;

        NotifyPeerDisconnected();
        ReadDirection_.OnShutdown();
        WriteDirection_.OnShutdown();

        ShutdownPromise_.Set();
    }

    TFuture<size_t> Read(const TSharedMutableRef& data)
    {
        auto read = std::make_unique<TReadOperation>(data);
        auto future = read->ToFuture();
        StartIO(&ReadDirection_, std::move(read));
        return future;
    }

    TFuture<std::pair<size_t, TNetworkAddress>> ReceiveFrom(const TSharedMutableRef& buffer)
    {
        auto receive = std::make_unique<TReceiveFromOperation>(buffer);
        auto future = receive->ToFuture();
        StartIO(&ReadDirection_, std::move(receive));
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
            THROW_ERROR_EXCEPTION("Write failed")
                << TError::FromSystem();
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
        if (UseDeliveryFence_) {
            return DoDeliveryFencedWrite(data);
        }

        return DoWrite(data);
    }

    TFuture<void> WriteV(const TSharedRefArray& data)
    {
        auto writeV = std::make_unique<TWriteVOperation>(data);
        auto future = writeV->ToFuture();
        StartIO(&WriteDirection_, std::move(writeV));
        return future;
    }

    TFuture<void> Close()
    {
        auto error = TError("Connection closed")
            << TErrorAttribute("connection", Name_);
        return AbortIO(error);
    }

    bool IsIdle()
    {
        auto guard = Guard(Lock_);
        return
            ReadError_.IsOK() &&
            WriteError_.IsOK() &&
            !WriteDirection_.Operation &&
            !ReadDirection_.Operation &&
            SynchronousIOCount_ == 0;
    }

    TFuture<void> Abort(const TError& error)
    {
        return AbortIO(error);
    }

    TFuture<void> CloseRead()
    {
        auto shutdownRead = std::make_unique<TShutdownOperation>(true);
        auto future = shutdownRead->ToFuture();
        StartIO(&ReadDirection_, std::move(shutdownRead));
        return future;
    }

    TFuture<void> CloseWrite()
    {
        auto shutdownWrite = std::make_unique<TShutdownOperation>(false);
        auto future = shutdownWrite->ToFuture();
        StartIO(&WriteDirection_, std::move(shutdownWrite));
        return future;
    }

    const TNetworkAddress& LocalAddress() const
    {
        return LocalAddress_;
    }

    const TNetworkAddress& RemoteAddress() const
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

    void SubscribePeerDisconnect(TCallback<void()> cb)
    {
        {
            auto guard = Guard(Lock_);
            if (!PeerDisconnected_) {
                OnPeerDisconnected_.push_back(std::move(cb));
                return;
            }
        }

        cb();
    }

private:
    const TString Name_;
    const TString LoggingTag_;
    const TNetworkAddress LocalAddress_;
    const TNetworkAddress RemoteAddress_;
    TFileDescriptor FD_ = -1;
    const IPollerPtr Poller_;

    // If set to true via ctor argument
    // |useDeliveryFence| will use
    // DeliverFencedWriteOperations
    // instead of WriteOperations,
    // which future is set only
    // after data from pipe has been read.
    const bool UseDeliveryFence_ = false;
    const TString PipePath_;


    TFDConnectionImpl(
        TFileDescriptor fd,
        TString filePath,
        const IPollerPtr& poller,
        bool useDeliveryFence)
        : Name_(Format("File{%v}", filePath))
        , FD_(fd)
        , Poller_(std::move(poller))
        , UseDeliveryFence_(useDeliveryFence)
        , PipePath_(std::move(filePath))
    { }

    TFDConnectionImpl(
        TFileDescriptor fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        IPollerPtr poller)
        : Name_(Format("FD{%v<->%v}", localAddress, remoteAddress))
        , LoggingTag_(Format("ConnectionId: %v", Name_))
        , LocalAddress_(localAddress)
        , RemoteAddress_(remoteAddress)
        , FD_(fd)
        , Poller_(std::move(poller))
    { }

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
    int SynchronousIOCount_ = 0;
    TError WriteError_;
    TError ReadError_;
    const TPromise<void> ShutdownPromise_ = NewPromise<void>();

    bool PeerDisconnected_ = false;
    std::vector<TCallback<void()>> OnPeerDisconnected_;

    TClosure AbortFromReadTimeout_;
    TClosure AbortFromWriteTimeout_;

    TDelayedExecutorCookie ReadTimeoutCookie_;
    TDelayedExecutorCookie WriteTimeoutCookie_;

    TFuture<void> DoWrite(const TSharedRef& data)
    {
        auto write = std::make_unique<TWriteOperation>(data);
        auto future = write->ToFuture();
        StartIO(&WriteDirection_, std::move(write));
        return future;
    }

    TFuture<void> DoDeliveryFencedWrite(const TSharedRef& data)
    {
        auto syncWrite = std::make_unique<TDeliveryFencedWriteOperation>(data, PipePath_);
        auto future = syncWrite->ToFuture();
        StartIO(&WriteDirection_, std::move(syncWrite));
        return future;
    }

    void Init()
    {
        AbortFromReadTimeout_ = BIND(&TFDConnectionImpl::AbortFromReadTimeout, MakeWeak(this));
        AbortFromWriteTimeout_ = BIND(&TFDConnectionImpl::AbortFromWriteTimeout, MakeWeak(this));

        if (!Poller_->TryRegister(this)) {
            WriteError_ = TError("Cannot register connection pollable");
            ReadError_ = WriteError_;
            return;
        }

        Arm();
    }

    void Arm(EPollControl additionalFlags = {})
    {
        auto control = EPollControl::Read | EPollControl::Write | EPollControl::EdgeTriggered | EPollControl::ReadHup;
        Poller_->Arm(FD_, this, control | additionalFlags);
    }

    TError GetCurrentError(EDirection direction)
    {
        switch (direction) {
            case EDirection::Read:
                return ReadError_;
            case EDirection::Write: {
                // We want to read if there were write errors before, but we don't want to write if there were read errors,
                // because it looks useless.
                auto error = WriteError_;
                if (error.IsOK() && !ReadError_.IsOK()) {
                    error = ReadError_;
                }
                return error;
            }
        }
    }

    void StartIO(TIODirection* direction, std::unique_ptr<IIOOperation> operation)
    {
        TError error;
        bool needRetry = false;

        {
            auto guard = Guard(Lock_);

            error = GetCurrentError(direction->Direction);

            if (error.IsOK()) {
                if (direction->Operation) {
                    THROW_ERROR_EXCEPTION("Another IO operation is in progress")
                        << TErrorAttribute("connection", Name_);
                }

                YT_VERIFY(!direction->Running);
                direction->Operation = std::move(operation);
                direction->StartBusyTimer();
                // Start operation only if this direction already has pending
                // event otherwise reading from FIFO before opening by writer
                // will return EOF immediately.
                needRetry = direction->Pending;
            }
        }

        if (!error.IsOK()) {
            operation->Abort(error);
            return;
        }

        if (needRetry) {
            Poller_->Retry(this);
        }
    }

    void DoIO(TIODirection* direction, bool event)
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
            result = result << TErrorAttribute("connection", Name_);
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
            operation->Abort(result);
        } else if (!result.Value().Retry) {
            operation->SetResult();
        } else if (needRetry) {
            Poller_->Retry(this);
        }

        if (needUnregister) {
            YT_UNUSED_FUTURE(Poller_->Unregister(this));
        }
    }

    TFuture<void> AbortIO(const TError& error)
    {
        auto guard = Guard(Lock_);
        // In case of read errors we have called Unarm and Unregister already.
        bool needUnarmAndUnregister = ReadError_.IsOK();
        if (WriteError_.IsOK()) {
            WriteError_ = error;
        }
        if (ReadError_.IsOK()) {
            ReadError_ = error;
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
        YT_UNUSED_FUTURE(Abort(TError("Read timeout")));
    }

    void AbortFromWriteTimeout()
    {
        YT_UNUSED_FUTURE(Abort(TError("Write timeout")));
    }

    void NotifyPeerDisconnected()
    {
        std::vector<TCallback<void()>> callbacks;
        {
            auto guard = Guard(Lock_);
            PeerDisconnected_ = true;
            callbacks = std::move(OnPeerDisconnected_);
        }
        for (const auto& cb : callbacks) {
            cb();
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TFDConnectionImpl)

////////////////////////////////////////////////////////////////////////////////

// The sole purpose of this class is to call Abort on Impl in dtor.
class TFDConnection
    : public IConnection
{
public:
    TFDConnection(
        TFileDescriptor fd,
        TString pipePath,
        IPollerPtr poller,
        TRefCountedPtr pipeHolder = nullptr,
        bool useDeliveryFence = false)
        : Impl_(TFDConnectionImpl::Create(fd, std::move(pipePath), std::move(poller), useDeliveryFence))
        , PipeHolder_(std::move(pipeHolder))
    { }

    TFDConnection(
        TFileDescriptor fd,
        const TNetworkAddress& localAddress,
        const TNetworkAddress& remoteAddress,
        IPollerPtr poller)
        : Impl_(TFDConnectionImpl::Create(fd, localAddress, remoteAddress, std::move(poller)))
    { }

    ~TFDConnection()
    {
        YT_UNUSED_FUTURE(Impl_->Abort(TError("Connection is abandoned")));
    }

    const TNetworkAddress& LocalAddress() const override
    {
        return Impl_->LocalAddress();
    }

    const TNetworkAddress& RemoteAddress() const override
    {
        return Impl_->RemoteAddress();
    }

    int GetHandle() const override
    {
        return Impl_->GetHandle();
    }

    TFuture<size_t> Read(const TSharedMutableRef& data) override
    {
        return Impl_->Read(data);
    }

    TFuture<void> Write(const TSharedRef& data) override
    {
        return Impl_->Write(data);
    }

    TFuture<void> WriteV(const TSharedRefArray& data) override
    {
        return Impl_->WriteV(data);
    }

    TFuture<void> Close() override
    {
        return Impl_->Close();
    }

    bool IsIdle() const override
    {
        return Impl_->IsIdle();
    }

    TFuture<void> Abort() override
    {
        return Impl_->Abort(TError(EErrorCode::Aborted, "Connection aborted"));
    }

    TFuture<void> CloseRead() override
    {
        return Impl_->CloseRead();
    }

    TFuture<void> CloseWrite() override
    {
        return Impl_->CloseWrite();
    }

    i64 GetReadByteCount() const override
    {
        return Impl_->GetReadByteCount();
    }

    i64 GetWriteByteCount() const override
    {
        return Impl_->GetWriteByteCount();
    }

    TConnectionStatistics GetReadStatistics() const override
    {
        return Impl_->GetReadStatistics();
    }

    TConnectionStatistics GetWriteStatistics() const override
    {
        return Impl_->GetWriteStatistics();
    }

    void SetReadDeadline(std::optional<TInstant> deadline) override
    {
        Impl_->SetReadDeadline(deadline);
    }

    void SetWriteDeadline(std::optional<TInstant> deadline) override
    {
        Impl_->SetWriteDeadline(deadline);
    }

    bool SetNoDelay() override
    {
        return Impl_->SetNoDelay();
    }

    bool SetKeepAlive() override
    {
        return Impl_->SetKeepAlive();
    }

    void SubscribePeerDisconnect(TCallback<void()> cb) override
    {
        return Impl_->SubscribePeerDisconnect(std::move(cb));
    }

private:
    const TFDConnectionImplPtr Impl_;
    TRefCountedPtr PipeHolder_;
};

////////////////////////////////////////////////////////////////////////////////

namespace {

TFileDescriptor CreateWriteFDForConnection(
    const TString& pipePath,
    std::optional<int> capacity)
{
#ifdef _unix_
    int flags = O_WRONLY | O_CLOEXEC;
    int fd = HandleEintr(::open, pipePath.c_str(), flags);
    if (fd == -1) {
        THROW_ERROR_EXCEPTION("Failed to open named pipe")
            << TError::FromSystem()
            << TErrorAttribute("path", pipePath);
    }

    try {
        if (capacity) {
            SafeSetPipeCapacity(fd, *capacity);
        }

        SafeMakeNonblocking(fd);
    } catch (...) {
        SafeClose(fd, false);
        throw;
    }
    return fd;
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
        THROW_ERROR_EXCEPTION("Failed to create socket pair")
            << TError::FromSystem();
    }
#else
    if (SocketPair(fds, /*overlapped*/ false, /*cloexec*/ true) == SOCKET_ERROR) {
        THROW_ERROR_EXCEPTION("Failed to create socket pair")
            << TError::FromSystem();
    }

    SetNonBlock(fds[0]);
    SetNonBlock(fds[1]);
#endif

    try {
        auto address0 = GetSocketName(fds[0]);
        auto address1 = GetSocketName(fds[1]);

        auto first = New<TFDConnection>(fds[0], address0, address1, poller);
        auto second = New<TFDConnection>(fds[1], address1, address0, std::move(poller));
        return std::pair(std::move(first), std::move(second));
    } catch (...) {
        YT_VERIFY(TryClose(fds[0], false));
        YT_VERIFY(TryClose(fds[1], false));
        throw;
    }
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
    TString pipePath,
    IPollerPtr poller,
    const TRefCountedPtr& pipeHolder)
{
    return New<TFDConnection>(fd, std::move(pipePath), std::move(poller), pipeHolder);
}

IConnectionReaderPtr CreateInputConnectionFromPath(
    TString pipePath,
    IPollerPtr poller,
    const TRefCountedPtr& pipeHolder)
{
#ifdef _unix_
    int flags = O_RDONLY | O_CLOEXEC | O_NONBLOCK;
    int fd = HandleEintr(::open, pipePath.c_str(), flags);
    if (fd == -1) {
        THROW_ERROR_EXCEPTION("Failed to open named pipe")
            << TError::FromSystem()
            << TErrorAttribute("path", pipePath);
    }

    return New<TFDConnection>(fd, std::move(pipePath), std::move(poller), pipeHolder);
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

IConnectionWriterPtr CreateOutputConnectionFromPath(
    TString pipePath,
    IPollerPtr poller,
    const TRefCountedPtr& pipeHolder,
    std::optional<int> capacity,
    bool useDeliveryFence)
{
    return New<TFDConnection>(
        CreateWriteFDForConnection(pipePath, capacity),
        std::move(pipePath),
        std::move(poller),
        pipeHolder,
        useDeliveryFence);
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
    auto fd = CreateUdpSocket();
    try {
        SetReuseAddrFlag(fd);
        BindSocket(fd, at);
    } catch (...) {
        SafeClose(fd, false);
        throw;
    }

    return New<TPacketConnection>(fd, at, std::move(poller));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
