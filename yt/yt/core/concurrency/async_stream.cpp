#include "async_stream.h"
#include "scheduler.h"

#include <util/stream/buffered.h>

#include <yt/yt/core/misc/serialize.h>

#include <queue>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSyncInputStreamAdapter
    : public IInputStream
{
public:
    TSyncInputStreamAdapter(
        IAsyncInputStreamPtr underlyingStream,
        EWaitForStrategy strategy)
        : UnderlyingStream_(std::move(underlyingStream))
        , Strategy_(strategy)
    { }

private:
    const IAsyncInputStreamPtr UnderlyingStream_;
    const EWaitForStrategy Strategy_;

    size_t DoRead(void* buffer, size_t length) override
    {
        if (length == 0) {
            return 0;
        }

        // When using WaitFor, we protect ourselves from TFiberCancelledException by
        // introducing our own read buffer and additional data copying. In case of
        // Get, there are no means of cancellation, so reading directly to the destination
        // buffer is just fine.
        TSharedMutableRef readBuffer;
        if (Strategy_ == EWaitForStrategy::WaitFor) {
            struct TSyncInputStreamAdapterIntermediateBufferTag { };
            readBuffer = TSharedMutableRef::Allocate<TSyncInputStreamAdapterIntermediateBufferTag>(length);
        } else {
            readBuffer = TSharedMutableRef(buffer, length, /*holder*/ nullptr);
        }

        auto bytesRead = WaitForWithStrategy(UnderlyingStream_->Read(readBuffer), Strategy_)
            .ValueOrThrow();

        if (Strategy_ == EWaitForStrategy::WaitFor) {
            memcpy(buffer, readBuffer.Begin(), bytesRead);
        }

        return bytesRead;
    }
};

std::unique_ptr<IInputStream> CreateSyncAdapter(
    IAsyncInputStreamPtr underlyingStream,
    EWaitForStrategy strategy)
{
    YT_VERIFY(underlyingStream);
    return std::make_unique<TSyncInputStreamAdapter>(
        std::move(underlyingStream),
        strategy);
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncInputStreamAdapter
    : public IAsyncInputStream
{
public:
    TAsyncInputStreamAdapter(
        IInputStream* underlyingStream,
        IInvokerPtr invoker)
        : UnderlyingStream_(underlyingStream)
        , Invoker_(std::move(invoker))
    { }

    TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        return
            BIND(&TAsyncInputStreamAdapter::DoRead, MakeStrong(this), buffer)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    size_t DoRead(const TSharedMutableRef& buffer) const
    {
        return UnderlyingStream_->Read(buffer.Begin(), buffer.Size());
    }

private:
    IInputStream* const UnderlyingStream_;
    const IInvokerPtr Invoker_;
};

IAsyncInputStreamPtr CreateAsyncAdapter(
    IInputStream* underlyingStream,
    IInvokerPtr invoker)
{
    YT_VERIFY(underlyingStream);
    return New<TAsyncInputStreamAdapter>(underlyingStream, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

class TSyncBufferedOutputStreamAdapter
    : public IZeroCopyOutput
{
public:
    TSyncBufferedOutputStreamAdapter(
        IAsyncOutputStreamPtr underlyingStream,
        EWaitForStrategy strategy,
        size_t bufferCapacity)
        : UnderlyingStream_(std::move(underlyingStream))
        , Strategy_(strategy)
        , BufferCapacity_(bufferCapacity)
    {
        Reset();
    }

    virtual ~TSyncBufferedOutputStreamAdapter()
    {
        try {
            Finish();
        } catch (...) {
        }
    }

private:
    const IAsyncOutputStreamPtr UnderlyingStream_;
    const EWaitForStrategy Strategy_;
    const size_t BufferCapacity_;
    size_t CurrentBufferSize_;
    TSharedMutableRef Buffer_;

    struct TBufferTag
    { };

    void Reset()
    {
        CurrentBufferSize_ = 0;
        Buffer_ = TSharedMutableRef::Allocate<TBufferTag>(BufferCapacity_);
    }

    void* WriteToBuffer(const void* data, size_t length)
    {
        YT_ASSERT(length <= GetBufferSpaceLeft());
        char* ptr = Buffer_.Begin() + CurrentBufferSize_;
        ::memcpy(Buffer_.Begin() + CurrentBufferSize_, data, length);
        CurrentBufferSize_ += length;
        return ptr;
    }

    void WriteToStream(const void* data, size_t length)
    {
        auto sharedBuffer = TSharedRef::MakeCopy<TBufferTag>(TRef(data, length));
        auto future = UnderlyingStream_->Write(std::move(sharedBuffer));
        WaitForWithStrategy(std::move(future), Strategy_)
            .ThrowOnError();
    }

    size_t GetBufferSpaceLeft() const
    {
        return BufferCapacity_ - CurrentBufferSize_;
    }

    size_t GetBufferSize() const
    {
        return CurrentBufferSize_;
    }

protected:
    size_t DoNext(void** ptr) override
    {
        if (GetBufferSpaceLeft() == 0) {
            DoFlush();
        }

        auto size = GetBufferSpaceLeft();
        *ptr = Buffer_.Begin() + CurrentBufferSize_;
        CurrentBufferSize_ += size;

        return size;
    }

    void DoUndo(size_t size) override
    {
        YT_VERIFY(CurrentBufferSize_ >= size);
        CurrentBufferSize_ -= size;
    }

    void DoWrite(const void* buffer, size_t length) override
    {
        if (length > GetBufferSpaceLeft()) {
            DoFlush();
        }
        if (length <= GetBufferSpaceLeft()) {
            WriteToBuffer(buffer, length);
        } else {
            WriteToStream(buffer, length);
        }
    }

    void DoFlush() override
    {
        if (CurrentBufferSize_ == 0) {
            return;
        }
        auto writeFuture = UnderlyingStream_->Write(Buffer_.Slice(0, CurrentBufferSize_));
        WaitForWithStrategy(std::move(writeFuture), Strategy_)
            .ThrowOnError();
        Reset();
    }
};

std::unique_ptr<IZeroCopyOutput> CreateBufferedSyncAdapter(
    IAsyncOutputStreamPtr underlyingStream,
    EWaitForStrategy strategy,
    size_t bufferSize)
{
    YT_VERIFY(underlyingStream);
    return std::make_unique<TSyncBufferedOutputStreamAdapter>(
        std::move(underlyingStream),
        strategy,
        bufferSize);
}

////////////////////////////////////////////////////////////////////////////////

class TFlushableAsyncOutputStreamAdapter
    : public IFlushableAsyncOutputStream
{
public:
    TFlushableAsyncOutputStreamAdapter(
        IOutputStream* underlyingStream,
        IInvokerPtr invoker)
        : UnderlyingStream_(underlyingStream)
        , Invoker_(std::move(invoker))
    { }

    TFuture<void> Write(const TSharedRef& buffer) override
    {
        return BIND(&TFlushableAsyncOutputStreamAdapter::DoWrite, MakeStrong(this), buffer)
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> Flush() override
    {
        return BIND(&TFlushableAsyncOutputStreamAdapter::DoFlush, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    TFuture<void> Close() override
    {
        return BIND(&TFlushableAsyncOutputStreamAdapter::DoFinish, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    void DoWrite(const TSharedRef& buffer) const
    {
        UnderlyingStream_->Write(buffer.Begin(), buffer.Size());
    }

    void DoFlush() const
    {
        UnderlyingStream_->Flush();
    }

    void DoFinish() const
    {
        UnderlyingStream_->Finish();
    }

private:
    IOutputStream* const UnderlyingStream_;
    const IInvokerPtr Invoker_;
};

IFlushableAsyncOutputStreamPtr CreateAsyncAdapter(
    IOutputStream* underlyingStream,
    IInvokerPtr invoker)
{
    YT_VERIFY(underlyingStream);
    return New<TFlushableAsyncOutputStreamAdapter>(underlyingStream, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef IAsyncZeroCopyInputStream::ReadAll()
{
    struct TTag
    { };

    std::vector<TSharedRef> chunks;

    // TODO(prime@): Add hard limit on body size.
    while (true) {
        auto chunk = WaitFor(Read())
            .ValueOrThrow();

        if (chunk.Empty()) {
            break;
        }

        chunks.emplace_back(TSharedRef::MakeCopy<TTag>(chunk));
    }

    return MergeRefsToRef<TTag>(std::move(chunks));
}

////////////////////////////////////////////////////////////////////////////////

class TZeroCopyInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TZeroCopyInputStreamAdapter(
        IAsyncInputStreamPtr underlyingStream,
        size_t blockSize)
        : UnderlyingStream_(std::move(underlyingStream))
        , BlockSize_(blockSize)
    { }

    TFuture<TSharedRef> Read() override
    {
        struct TZeroCopyInputStreamAdapterBlockTag
        { };

        auto promise = NewPromise<TSharedRef>();
        auto block = TSharedMutableRef::Allocate<TZeroCopyInputStreamAdapterBlockTag>(BlockSize_, {.InitializeStorage = false});

        DoRead(promise, std::move(block), 0);

        return promise;
    }

private:
    const IAsyncInputStreamPtr UnderlyingStream_;
    const size_t BlockSize_;


    void DoRead(
        TPromise<TSharedRef> promise,
        TSharedMutableRef block,
        size_t offset)
    {
        if (block.Size() == offset) {
            promise.Set(std::move(block));
            return;
        }

        UnderlyingStream_->Read(block.Slice(offset, block.Size())).Subscribe(
            BIND(
                &TZeroCopyInputStreamAdapter::OnRead,
                MakeStrong(this),
                std::move(promise),
                std::move(block),
                offset).Via(GetSyncInvoker()));
    }

    void OnRead(
        TPromise<TSharedRef> promise,
        TSharedMutableRef block,
        size_t offset,
        const TErrorOr<size_t>& result)
    {
        if (!result.IsOK()) {
            promise.Set(TError(result));
            return;
        }

        auto bytes = result.Value();

        if (bytes == 0) {
            promise.Set(offset == 0 ? TSharedRef() : block.Slice(0, offset));
            return;
        }

        DoRead(std::move(promise), std::move(block), offset + bytes);
    }
};

IAsyncZeroCopyInputStreamPtr CreateZeroCopyAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t blockSize)
{
    YT_VERIFY(underlyingStream);
    return New<TZeroCopyInputStreamAdapter>(std::move(underlyingStream), blockSize);
}

////////////////////////////////////////////////////////////////////////////////

class TCopyingInputStreamAdapter
    : public IAsyncInputStream
{
public:
    explicit TCopyingInputStreamAdapter(IAsyncZeroCopyInputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YT_VERIFY(UnderlyingStream_);
    }

    TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        if (CurrentBlock_) {
            // NB(psushin): no swapping here, it's a _copying_ adapter!
            // Also, #buffer may be constructed via FromNonOwningRef.
            return MakeFuture<size_t>(DoCopy(buffer));
        } else {
            return UnderlyingStream_->Read().Apply(
                BIND(&TCopyingInputStreamAdapter::OnRead, MakeStrong(this), buffer));
        }
    }

private:
    const IAsyncZeroCopyInputStreamPtr UnderlyingStream_;

    TSharedRef CurrentBlock_;
    i64 CurrentOffset_ = 0;


    size_t DoCopy(const TMutableRef& buffer)
    {
        size_t remaining = CurrentBlock_.Size() - CurrentOffset_;
        size_t bytes = std::min(buffer.Size(), remaining);
        ::memcpy(buffer.Begin(), CurrentBlock_.Begin() + CurrentOffset_, bytes);
        CurrentOffset_ += bytes;
        if (CurrentOffset_ == std::ssize(CurrentBlock_)) {
            CurrentBlock_.Reset();
            CurrentOffset_ = 0;
        }
        return bytes;
    }

    size_t OnRead(const TSharedMutableRef& buffer, const TSharedRef& block)
    {
        CurrentBlock_ = block;
        return DoCopy(buffer);
    }

};

IAsyncInputStreamPtr CreateCopyingAdapter(IAsyncZeroCopyInputStreamPtr underlyingStream)
{
    return New<TCopyingInputStreamAdapter>(std::move(underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

class TZeroCopyOutputStreamAdapter
    : public IAsyncZeroCopyOutputStream
{
public:
    explicit TZeroCopyOutputStreamAdapter(IAsyncOutputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YT_VERIFY(UnderlyingStream_);
    }

    TFuture<void> Write(const TSharedRef& data) override
    {
        YT_ASSERT(data);
        return Push(data);
    }

    TFuture<void> Close() override
    {
        return Push(TSharedRef());
    }

private:
    const IAsyncOutputStreamPtr UnderlyingStream_;

    struct TEntry
    {
        // If `Block' is null it means close was requested.
        TSharedRef Block;
        TPromise<void> Promise;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::queue<TEntry> Queue_;
    TError Error_;
    bool Closed_ = false;

    TFuture<void> Push(const TSharedRef& data)
    {
        TPromise<void> promise;
        bool needInvoke;
        {
            auto guard = Guard(SpinLock_);
            YT_VERIFY(!Closed_);
            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }
            promise = NewPromise<void>();
            Queue_.push(TEntry{data, promise});
            needInvoke = (Queue_.size() == 1);
            Closed_ = !data;
        }
        if (needInvoke) {
            TFuture<void> invokeResult;
            if (data) {
                invokeResult = UnderlyingStream_->Write(data);
            } else {
                invokeResult = UnderlyingStream_->Close();
            }
            invokeResult.Subscribe(
                BIND(&TZeroCopyOutputStreamAdapter::OnWritten, MakeStrong(this)));
        }
        return promise;
    }

    void OnWritten(const TError& error)
    {
        TSharedRef data;
        bool hasData = NotifyAndFetchNext(error, &data);
        while (hasData) {
            if (error.IsOK()) {
                TFuture<void> result;
                if (data) {
                    result = UnderlyingStream_->Write(data);
                } else {
                    result = UnderlyingStream_->Close();
                }
                auto mayWriteResult = result.TryGet();
                if (!mayWriteResult || !mayWriteResult->IsOK()) {
                    result.Subscribe(
                        BIND(&TZeroCopyOutputStreamAdapter::OnWritten, MakeStrong(this)));
                    break;
                }
            }

            hasData = NotifyAndFetchNext(error, &data);
        }
    }

    // Set current entry promise to error and tries to fetch next entry data.
    // Return `false' if there no next entry.
    // Otherwise return `true' and fill data with next entry Block.
    bool NotifyAndFetchNext(const TError& error, TSharedRef* data)
    {
        TPromise<void> promise;
        bool hasData = false;
        {
            auto guard = Guard(SpinLock_);
            auto& entry = Queue_.front();
            promise = std::move(entry.Promise);
            if (!error.IsOK() && Error_.IsOK()) {
                Error_ = error;
            }
            Queue_.pop();
            hasData = !Queue_.empty();
            if (hasData) {
                *data = Queue_.front().Block;
            }
        }
        promise.Set(error);
        return hasData;
    }
};

IAsyncZeroCopyOutputStreamPtr CreateZeroCopyAdapter(IAsyncOutputStreamPtr underlyingStream)
{
    return New<TZeroCopyOutputStreamAdapter>(std::move(underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

class TCopyingOutputStreamAdapter
    : public IAsyncOutputStream
{
public:
    explicit TCopyingOutputStreamAdapter(IAsyncZeroCopyOutputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YT_VERIFY(UnderlyingStream_);
    }

    TFuture<void> Write(const TSharedRef& buffer) override
    {
        struct TCopyingOutputStreamAdapterBlockTag { };
        auto block = TSharedMutableRef::Allocate<TCopyingOutputStreamAdapterBlockTag>(buffer.Size(), {.InitializeStorage = false});
        ::memcpy(block.Begin(), buffer.Begin(), buffer.Size());
        return UnderlyingStream_->Write(block);
    }

    TFuture<void> Close() override
    {
        return UnderlyingStream_->Close();
    }

private:
    const IAsyncZeroCopyOutputStreamPtr UnderlyingStream_;
};

IAsyncOutputStreamPtr CreateCopyingAdapter(IAsyncZeroCopyOutputStreamPtr underlyingStream)
{
    return New<TCopyingOutputStreamAdapter>(underlyingStream);
}

////////////////////////////////////////////////////////////////////////////////

class TPrefetchingInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TPrefetchingInputStreamAdapter(
        IAsyncZeroCopyInputStreamPtr underlyingStream,
        size_t windowSize)
        : UnderlyingStream_(underlyingStream)
        , WindowSize_(windowSize)
    {
        YT_VERIFY(UnderlyingStream_);
        YT_VERIFY(WindowSize_ > 0);
    }

    TFuture<TSharedRef> Read() override
    {
        auto guard = Guard(SpinLock_);
        if (!Error_.IsOK()) {
            return MakeFuture<TSharedRef>(Error_);
        }
        if (PrefetchedBlocks_.empty()) {
            return Prefetch(&guard).Apply(
                BIND(&TPrefetchingInputStreamAdapter::OnPrefetched, MakeStrong(this)));
        }
        return MakeFuture<TSharedRef>(PopBlock(&guard));
    }

private:
    const IAsyncZeroCopyInputStreamPtr UnderlyingStream_;
    const size_t WindowSize_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TError Error_;
    std::queue<TSharedRef> PrefetchedBlocks_;
    size_t PrefetchedSize_ = 0;
    TFuture<void> OutstandingResult_;


    TFuture<void> Prefetch(TGuard<NThreading::TSpinLock>* guard)
    {
        if (OutstandingResult_) {
            return OutstandingResult_;
        }
        auto promise = NewPromise<void>();
        OutstandingResult_ = promise;
        guard->Release();
        UnderlyingStream_->Read().Subscribe(BIND(
            &TPrefetchingInputStreamAdapter::OnRead,
            MakeStrong(this),
            promise));
        return promise;
    }

    void OnRead(TPromise<void> promise, const TErrorOr<TSharedRef>& result)
    {
        {
            auto guard = Guard(SpinLock_);
            PushBlock(&guard, result);
        }
        promise.Set(result);
    }

    TSharedRef OnPrefetched()
    {
        auto guard = Guard(SpinLock_);
        return PopBlock(&guard);
    }

    void PushBlock(TGuard<NThreading::TSpinLock>* guard, const TErrorOr<TSharedRef>& result)
    {
        YT_ASSERT(OutstandingResult_);
        OutstandingResult_.Reset();
        if (!result.IsOK()) {
            Error_ = TError(result);
            return;
        }
        const auto& block = result.Value();
        PrefetchedBlocks_.push(block);
        PrefetchedSize_ += block.Size();
        if (block && PrefetchedSize_ < WindowSize_) {
            YT_UNUSED_FUTURE(Prefetch(guard));
        }
    }

    TSharedRef PopBlock(TGuard<NThreading::TSpinLock>* guard)
    {
        YT_ASSERT(!PrefetchedBlocks_.empty());
        auto block = PrefetchedBlocks_.front();
        PrefetchedBlocks_.pop();
        PrefetchedSize_ -= block.Size();
        if (!OutstandingResult_ && PrefetchedSize_ < WindowSize_) {
            YT_UNUSED_FUTURE(Prefetch(guard));
        }
        return block;
    }

};

IAsyncZeroCopyInputStreamPtr CreatePrefetchingAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream,
    size_t windowSize)
{
    return New<TPrefetchingInputStreamAdapter>(std::move(underlyingStream), windowSize);
}

////////////////////////////////////////////////////////////////////////////////

struct TBufferingInputStreamAdapterBufferTag { };

class TBufferingInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TBufferingInputStreamAdapter(
        IAsyncInputStreamPtr underlyingStream,
        size_t windowSize)
        : UnderlyingStream_(underlyingStream)
        , WindowSize_(windowSize)
    {
        YT_VERIFY(UnderlyingStream_);
        YT_VERIFY(WindowSize_ > 0);

        Buffer_ = TSharedMutableRef::Allocate<TBufferingInputStreamAdapterBufferTag>(WindowSize_, {.InitializeStorage = false});
    }

    TFuture<TSharedRef> Read() override
    {
        auto guard = Guard(SpinLock_);
        if (PrefetchedSize_ == 0) {
            if (EndOfStream_) {
                return MakeFuture<TSharedRef>(TSharedRef());
            }
            if (!Error_.IsOK()) {
                return MakeFuture<TSharedRef>(Error_);
            }
            return Prefetch(&guard).Apply(
                BIND(&TBufferingInputStreamAdapter::OnPrefetched, MakeStrong(this)));
        }
        return MakeFuture<TSharedRef>(CopyPrefetched(&guard));
    }

private:
    const IAsyncInputStreamPtr UnderlyingStream_;
    const size_t WindowSize_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TError Error_;
    TSharedMutableRef Prefetched_;
    TSharedMutableRef Buffer_;
    size_t PrefetchedSize_ = 0;
    bool EndOfStream_ = false;
    TFuture<void> OutstandingResult_;

    TFuture<void> Prefetch(TGuard<NThreading::TSpinLock>* guard)
    {
        if (OutstandingResult_) {
            return OutstandingResult_;
        }
        auto promise = NewPromise<void>();
        OutstandingResult_ = promise;
        guard->Release();
        UnderlyingStream_->Read(Buffer_.Slice(0, WindowSize_ - PrefetchedSize_)).Subscribe(BIND(
            &TBufferingInputStreamAdapter::OnRead,
            MakeStrong(this),
            promise));
        return promise;
    }

    void OnRead(TPromise<void> promise, const TErrorOr<size_t>& result)
    {
        {
            auto guard = Guard(SpinLock_);
            AppendPrefetched(&guard, result);
        }
        promise.Set(result);
    }

    TSharedRef OnPrefetched()
    {
        auto guard = Guard(SpinLock_);
        YT_ASSERT(PrefetchedSize_ != 0);
        return CopyPrefetched(&guard);
    }

    void AppendPrefetched(TGuard<NThreading::TSpinLock>* guard, const TErrorOr<size_t>& result)
    {
        YT_ASSERT(OutstandingResult_);
        OutstandingResult_.Reset();
        if (!result.IsOK()) {
            Error_ = TError(result);
            return;
        } else if (result.Value() == 0) {
            EndOfStream_ = true;
            return;
        }
        size_t bytes = result.Value();
        if (bytes != 0) {
            if (PrefetchedSize_ == 0) {
                Prefetched_ = Buffer_;
                Buffer_ = TSharedMutableRef::Allocate<TBufferingInputStreamAdapterBufferTag>(WindowSize_, {.InitializeStorage = false});
            } else {
                ::memcpy(Prefetched_.Begin() + PrefetchedSize_, Buffer_.Begin(), bytes);
            }
            PrefetchedSize_ += bytes;

        }
        // Stop reading on the end of stream or full buffer.
        if (bytes != 0 && PrefetchedSize_ < WindowSize_) {
            YT_UNUSED_FUTURE(Prefetch(guard));
        }
    }

    TSharedRef CopyPrefetched(TGuard<NThreading::TSpinLock>* guard)
    {
        YT_ASSERT(PrefetchedSize_ != 0);
        auto block = Prefetched_.Slice(0, PrefetchedSize_);
        Prefetched_ = TSharedMutableRef();
        PrefetchedSize_ = 0;
        if (!OutstandingResult_) {
            YT_UNUSED_FUTURE(Prefetch(guard));
        }
        return block;
    }

};

IAsyncZeroCopyInputStreamPtr CreateBufferingAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t windowSize)
{
    return New<TBufferingInputStreamAdapter>(std::move(underlyingStream), windowSize);
}

////////////////////////////////////////////////////////////////////////////////

class TExpiringInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TExpiringInputStreamAdapter(
        IAsyncZeroCopyInputStreamPtr underlyingStream,
        TDuration timeout)
        : UnderlyingStream_(underlyingStream)
        , Timeout_(timeout)
    {
        YT_VERIFY(UnderlyingStream_);
        YT_VERIFY(Timeout_ > TDuration::Zero());
    }

    TFuture<TSharedRef> Read() override
    {
        auto guard = Guard(SpinLock_);

        if (PendingBlock_) {
            auto block = std::move(PendingBlock_);
            PendingBlock_.reset();

            return MakeFuture<TSharedRef>(*block);
        }

        auto promise = NewPromise<TSharedRef>();
        Cookie_ = TDelayedExecutor::Submit(
            BIND(&TExpiringInputStreamAdapter::OnTimeout, MakeWeak(this), promise), Timeout_);

        YT_ASSERT(!Promise_);
        Promise_ = promise;

        if (!Fetching_) {
            Fetching_ = true;
            guard.Release();

            UnderlyingStream_->Read().Subscribe(
                BIND(&TExpiringInputStreamAdapter::OnRead, MakeWeak(this)));
        }
        return promise;
    }

private:
    const IAsyncZeroCopyInputStreamPtr UnderlyingStream_;
    const TDuration Timeout_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    bool Fetching_ = false;
    std::optional<TErrorOr<TSharedRef>> PendingBlock_;
    TPromise<TSharedRef> Promise_;
    TDelayedExecutorCookie Cookie_;

    void OnRead(const TErrorOr<TSharedRef>& value)
    {
        TPromise<TSharedRef> promise;
        auto guard = Guard(SpinLock_);
        Fetching_ = false;
        if (Promise_) {
            swap(Promise_, promise);
            TDelayedExecutor::CancelAndClear(Cookie_);
            guard.Release();
            promise.Set(value);
        } else {
            PendingBlock_ = value;
        }

    }

    void OnTimeout(TPromise<TSharedRef> promise, bool aborted)
    {
        bool timedOut = false;
        {
            auto guard = Guard(SpinLock_);
            if (promise == Promise_) {
                Promise_ = TPromise<TSharedRef>();
                timedOut = true;
            }
        }

        if (timedOut) {
            TError error;
            if (aborted) {
                error = TError(NYT::EErrorCode::Canceled, "Operation aborted");
            } else {
                error = TError(NYT::EErrorCode::Timeout, "Operation timed out")
                    << TErrorAttribute("timeout", Timeout_);
            }
            promise.Set(error);
        }
    }
};

IAsyncZeroCopyInputStreamPtr CreateExpiringAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream,
    TDuration timeout)
{
    return New<TExpiringInputStreamAdapter>(underlyingStream, timeout);
}

////////////////////////////////////////////////////////////////////////////////

class TConcurrentInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TConcurrentInputStreamAdapter(
        IAsyncZeroCopyInputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YT_VERIFY(UnderlyingStream_);
    }

    TFuture<TSharedRef> Read() override
    {
        auto guard = Guard(SpinLock_);

        if (PendingBlock_) {
            auto block = std::move(PendingBlock_);
            PendingBlock_.reset();

            return MakeFuture<TSharedRef>(*block);
        }

        auto newPromise = NewPromise<TSharedRef>();
        auto oldPromise = newPromise;
        swap(oldPromise, Promise_);

        if (!Fetching_) {
            Fetching_ = true;
            guard.Release();

            UnderlyingStream_->Read().Subscribe(
                BIND(&TConcurrentInputStreamAdapter::OnRead, MakeWeak(this)));
        }
        // Always set the pending promise from previous Read.
        if (oldPromise) {
            guard.Release();
            oldPromise.TrySet(TError(NYT::EErrorCode::Canceled, "Read canceled"));
        }
        return newPromise;
    }

private:
    const IAsyncZeroCopyInputStreamPtr UnderlyingStream_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    bool Fetching_ = false;
    std::optional<TErrorOr<TSharedRef>> PendingBlock_;
    TPromise<TSharedRef> Promise_;

    void OnRead(const TErrorOr<TSharedRef>& value)
    {
        TPromise<TSharedRef> promise;
        {
            auto guard = Guard(SpinLock_);
            Fetching_ = false;
            YT_ASSERT(Promise_);
            swap(promise, Promise_);
            if (promise.IsSet()) {
                YT_ASSERT(!PendingBlock_);
                PendingBlock_ = value;
                return;
            }
        }
        promise.Set(value);
    }
};

IAsyncZeroCopyInputStreamPtr CreateConcurrentAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream)
{
    return New<TConcurrentInputStreamAdapter>(std::move(underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

void PipeInputToOutput(
    const IAsyncZeroCopyInputStreamPtr& input,
    const IAsyncOutputStreamPtr& output)
{
    while (true) {
        auto asyncBlock = input->Read();
        auto block = WaitFor(asyncBlock)
            .ValueOrThrow();
        if (!block || block.Empty()) {
            break;
        }
        WaitFor(output->Write(block))
            .ThrowOnError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> ExpectEndOfStream(
    const IAsyncZeroCopyInputStreamPtr& input)
{
    YT_VERIFY(input);
    return input->Read().Apply(BIND([] (const TSharedRef& ref) {
        if (ref) {
            THROW_ERROR_EXCEPTION("Expected end-of-stream, received a non-null ref of size %v",
                ref.Size());
        }
    }));
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
