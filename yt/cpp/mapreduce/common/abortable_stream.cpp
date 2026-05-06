#include "abortable_stream.h"

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/yt/logging/logger.h>
#include <library/cpp/yt/memory/ref.h>

#include <util/system/spinlock.h>

namespace NYT::NDetail {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TAbortableInputStreamAdapter
    : public IAbortableInputStream
{
public:
    explicit TAbortableInputStreamAdapter(IAsyncInputStreamPtr underlyingStream)
        : UnderlyingStream_(std::move(underlyingStream))
    { }

    void Abort() override
    {
        auto guard = Guard(Lock_);
        IsAborted_ = true;
        CurrentFuture_.Cancel(TError("Stream was aborted"));
    }

    bool IsAborted() const override
    {
        auto guard = Guard(Lock_);
        return IsAborted_;
    }

private:
    const IAsyncInputStreamPtr UnderlyingStream_;

    TAdaptiveLock Lock_;
    bool IsAborted_ = false;
    TFuture<size_t> CurrentFuture_;

    size_t DoRead(void* buffer, size_t length) override
    {
        if (length == 0) {
            return 0;
        }

        struct TAbortableInputStreamBufferTag { };
        auto readBuffer = TSharedMutableRef::Allocate<TAbortableInputStreamBufferTag>(length);

        auto future = UnderlyingStream_->Read(readBuffer);
        {
            auto guard = Guard(Lock_);
            CurrentFuture_ = future;
            if (IsAborted_) {
                future.Cancel(TError("Stream was aborted"));
            }
        }

        auto result = WaitFor(future);

        {
            auto guard = Guard(Lock_);
            if (IsAborted_) {
                ythrow TInputStreamAbortedError() << "Stream was aborted";
            }
        }

        auto bytesRead = result.ValueOrThrow();

        memcpy(buffer, readBuffer.Begin(), bytesRead);

        return bytesRead;
    }
};

std::unique_ptr<IAbortableInputStream> CreateAbortableInputStreamAdapter(
    IAsyncInputStreamPtr underlyingStream)
{
    YT_VERIFY(underlyingStream);
    return std::make_unique<TAbortableInputStreamAdapter>(
        std::move(underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

class TAbortableStreamFallback
    : public IAbortableInputStream
{
public:
    explicit TAbortableStreamFallback(IInputStream* underlyingStream)
        : UnderlyingStream_(underlyingStream)
    { }

    void Abort() override
    {
        YT_LOG_WARNING("Abort for this stream type is not supported");
    }

private:
    IInputStream* const UnderlyingStream_;

    size_t DoRead(void* buffer, size_t length) override
    {
        return UnderlyingStream_->Read(buffer, length);
    }
};

std::unique_ptr<IAbortableInputStream> CreateAbortableInputStreamAdapterFallback(
    IInputStream* underlyingStream)
{
    YT_VERIFY(underlyingStream);
    return std::make_unique<TAbortableStreamFallback>(underlyingStream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
