#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref.h>

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

////////////////////////////////////////////////////////////////////////////////

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

    //! Reads all content from the stream.
    /*!
     *  Default implementation iteratively calls #Read until the stream is exhausted.
     *
     *  \note
     *  May (and typically will) cause fiber context switch.
     */
    virtual TSharedRef ReadAll();
};

DEFINE_REFCOUNTED_TYPE(IAsyncZeroCopyInputStream)

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
     *  NB: This shared ref should become unique ref.
     */
    [[nodiscard]] virtual TFuture<void> Write(const TSharedRef& data) = 0;

    //! Indicates that the stream is closed.
    /*!
     *  No #Write calls are possible after #Close.
     */
    [[nodiscard]] virtual TFuture<void> Close() = 0;
};

DEFINE_REFCOUNTED_TYPE(IAsyncZeroCopyOutputStream)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
