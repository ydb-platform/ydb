#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Provides an asynchronous interface for reading from a stream.
struct IAsyncInputStream
    : public virtual TRefCounted
{
    //! Starts reading another block of data.
    /*!
     *  Call #Read and provide a buffer to start reading.
     *  One must not call #Read again before the previous call is complete.
     *  Returns number of bytes read or an error.
     */
    [[nodiscard]] virtual TFuture<size_t> Read(const TSharedMutableRef& buffer) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncInputStream)

//! Creates a synchronous adapter from a given asynchronous stream.
/*!
 *  NB: in order to ensure memory safety with WaitFor strategy, data is read to an
 *  intermediate shared buffer and then copied to the destination buffer.
 *  Do not use this wrapper in throughput-critical code, prefer using
 *  async or async zero-copy input stream interface instead.
 */
std::unique_ptr<IInputStream> CreateSyncAdapter(
    IAsyncInputStreamPtr underlyingStream,
    EWaitForStrategy strategy = EWaitForStrategy::WaitFor);

//! Creates an asynchronous adapter from a given synchronous stream.
/*!
 *  Caller may provide an invoker for all calls to the underlying stream.
 *  This way one can ensure that current thread will not block in calls
 *  to the adapter.
 */
IAsyncInputStreamPtr CreateAsyncAdapter(
    IInputStream* underlyingStream,
    IInvokerPtr invoker = GetSyncInvoker());

////////////////////////////////////////////////////////////////////////////////

//! Provides an asynchronous interface for writing to a stream.
struct IAsyncOutputStream
    : public virtual TRefCounted
{
    //! Starts writing another block of data.
    /*!
     *  Call #Write to issue a write request.
     *  Buffer passed to #Write must remain valid until the returned future is set.
     *  One must not call #Write again before the previous call is complete.
     *
     *  Implementations must not rely on the content of #buffer to remain immutable
     *  between calls to #Write; e.g. clients are allowed to reuse a single (mutable)
     *  buffer between these calls.
     */
    [[nodiscard]] virtual TFuture<void> Write(const TSharedRef& buffer) = 0;

    //! Finalizes stream.
    /*! Call #Close to complete writes.
     *  #Close shouldn't be called before previous #Write call is complete.
     *  #Write/#Close mustn't be called after #Close was called.
     */
    [[nodiscard]] virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncOutputStream)

struct IFlushableAsyncOutputStream
    : public IAsyncOutputStream
{
    //! Starts flushing the stream.
    /*! Call #Flush to complete preceding writes.
     *  #Flush shouldn't be called before previous #Write call is complete.
     *  #Flush mustn't be called after #Close was called.
     */
    [[nodiscard]] virtual TFuture<void> Flush() = 0;
};

DEFINE_REFCOUNTED_TYPE(IFlushableAsyncOutputStream)

//! Creates a synchronous buffering adapter from a given asynchronous stream.
/*!
 *  Not thread safe.
 */
std::unique_ptr<IZeroCopyOutput> CreateBufferedSyncAdapter(
    IAsyncOutputStreamPtr underlyingStream,
    EWaitForStrategy strategy = EWaitForStrategy::WaitFor,
    size_t bufferSize = 8_KB);

//! Creates an asynchronous adapter from a given synchronous stream.
/*!
 *  Caller may provide an invoker for all calls to the underlying stream.
 *  This way one can ensure that current thread will not block in calls
 *  to the adapter.
 */
IFlushableAsyncOutputStreamPtr CreateAsyncAdapter(
    IOutputStream* underlyingStream,
    IInvokerPtr invoker = GetSyncInvoker());

////////////////////////////////////////////////////////////////////////////////

//! Similar to IAsyncInputStream but is essentially zero-copy, i.e.
//! produces a sequence of memory blocks with shared ownership.
struct IAsyncZeroCopyInputStream
    : public virtual TRefCounted
{
    //! Requests another block of data.
    /*!
     *  Returns the data or an error.
     *  If a null TSharedRef is returned then end-of-stream is reached.
     *  One must not call #Read again before the previous call is complete.
     *  A sane implementation must guarantee that it returns blocks of sensible size.
     */
    [[nodiscard]] virtual TFuture<TSharedRef> Read() = 0;

    // Extension methods

    //! Reads all content from the stream by iteratively calling #Read until the stream is exhausted.
    /*!
     *  \note
     *  May (and typically will) cause fiber context switch.
     */
    TSharedRef ReadAll();
};

DEFINE_REFCOUNTED_TYPE(IAsyncZeroCopyInputStream)

//! Creates a zero-copy adapter from a given asynchronous stream.
IAsyncZeroCopyInputStreamPtr CreateZeroCopyAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t blockSize = 64_KB);

//! Creates a copying adapter from a given asynchronous zero-copy stream.
IAsyncInputStreamPtr CreateCopyingAdapter(IAsyncZeroCopyInputStreamPtr underlyingStream);

////////////////////////////////////////////////////////////////////////////////

//! Similar to IAsyncOutputStream but is essentially zero-copy, i.e.
//! consumes a sequence of memory blocks with shared ownership.
struct IAsyncZeroCopyOutputStream
    : public virtual TRefCounted
{
    //! Enqueues another block of data.
    /*!
     *  Returns an error, if any.
     *  In contrast to IAsyncOutputStream, one may call #Write again before
     *  the previous call is complete. The returned future, however, provides
     *  means to implement backpressure.
     *
     *  NB: this shared ref should become unique ref.
     */
    [[nodiscard]] virtual TFuture<void> Write(const TSharedRef& data) = 0;

    //! Indicates that the stream is closed.
    /*!
     *  No #Write calls are possible after #Close.
     */
    [[nodiscard]] virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncZeroCopyOutputStream)

//! Creates a zero-copy adapter from a given asynchronous stream.
IAsyncZeroCopyOutputStreamPtr CreateZeroCopyAdapter(IAsyncOutputStreamPtr underlyingStream);

//! Creates a copying adapter from a given asynchronous zero-copy stream.
IAsyncOutputStreamPtr CreateCopyingAdapter(IAsyncZeroCopyOutputStreamPtr underlyingStream);

//! Creates an adapter that prefetches data in background.
/*!
 *  The adapter tries to maintain up to #windowSize bytes of data by
 *  retrieving blocks from #underlyingStream in background.
 */
IAsyncZeroCopyInputStreamPtr CreatePrefetchingAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream,
    size_t windowSize);

////////////////////////////////////////////////////////////////////////////////

//! Creates an adapter that prefetches data into the buffer on bytes scale.
/*!
 *  The adapder differs from PrefetchingAdapter:
 *  - it is based on IAsyncInputStreamPtr underlying stream;
 *  - it works with bytes instead of blocks, provided by zero-copy adapters.
 */
IAsyncZeroCopyInputStreamPtr CreateBufferingAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t windowSize);

////////////////////////////////////////////////////////////////////////////////

//! Creates an adapter that returns an error if no data is read within timeout.
NConcurrency::IAsyncZeroCopyInputStreamPtr CreateExpiringAdapter(
    NConcurrency::IAsyncZeroCopyInputStreamPtr underlyingStream,
    TDuration timeout);

////////////////////////////////////////////////////////////////////////////////

//! Creates an adapter that can process concurrent Read() requests.
IAsyncZeroCopyInputStreamPtr CreateConcurrentAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream);

////////////////////////////////////////////////////////////////////////////////

// NB(levysotsky): Doesn't close the output stream.
void PipeInputToOutput(
    const IAsyncZeroCopyInputStreamPtr& input,
    const IAsyncOutputStreamPtr& output);

////////////////////////////////////////////////////////////////////////////////

TFuture<void> ExpectEndOfStream(
    const IAsyncZeroCopyInputStreamPtr& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
