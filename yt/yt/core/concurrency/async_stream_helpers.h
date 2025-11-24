#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/invoker_util.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Creates a synchronous adapter from a given asynchronous stream.
/*!
 *  NB: In order to ensure memory safety with WaitFor strategy, data is read to an
 *  intermediate shared buffer and then copied to the destination buffer.
 *  Do not use this wrapper in throughput-critical code, prefer using
 *  async or async zero-copy input stream interface instead.
 */
std::unique_ptr<IInputStream> CreateSyncAdapter(
    IAsyncInputStreamPtr underlyingStream,
    EWaitForStrategy strategy = EWaitForStrategy::WaitFor);

//! Creates a synchronous adapter from a given asynchronous zero-copy stream.
std::unique_ptr<IZeroCopyInput> CreateSyncAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream,
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

//! Creates a zero-copy adapter from a given asynchronous stream.
IAsyncZeroCopyInputStreamPtr CreateZeroCopyAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t blockSize = 64_KB);

//! Creates a copying adapter from a given asynchronous zero-copy stream.
IAsyncInputStreamPtr CreateCopyingAdapter(IAsyncZeroCopyInputStreamPtr underlyingStream);

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

//! Creates an adapter that prefetches data into the buffer on bytes scale.
/*!
 *  The adapder differs from PrefetchingAdapter:
 *  - it is based on IAsyncInputStreamPtr underlying stream;
 *  - it works with bytes instead of blocks, provided by zero-copy adapters.
 */
IAsyncZeroCopyInputStreamPtr CreateBufferingAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t windowSize);

//! Creates an adapter that returns an error if no data is read within timeout.
NConcurrency::IAsyncZeroCopyInputStreamPtr CreateExpiringAdapter(
    NConcurrency::IAsyncZeroCopyInputStreamPtr underlyingStream,
    TDuration timeout);

//! Creates an adapter that can process concurrent Read() requests.
IAsyncZeroCopyInputStreamPtr CreateConcurrentAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream);

//! Transfers bytes from input to output.
//! Does not close the output.
void PipeInputToOutput(
    const IAsyncZeroCopyInputStreamPtr& input,
    const IAsyncOutputStreamPtr& output);
void PipeInputToOutput(
    const IAsyncZeroCopyInputStreamPtr& input,
    const IAsyncZeroCopyOutputStreamPtr& output);

//! Drains the input.
void DrainInput(const IAsyncZeroCopyInputStreamPtr& input);

//! Checks that the input has reached the end.
TFuture<void> CheckEndOfStream(
    const IAsyncZeroCopyInputStreamPtr& input);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
