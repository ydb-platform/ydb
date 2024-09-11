#pragma once

#include "channel.h"

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/sliding_window.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/compression/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! For empty and null attachments returns 1; for others returns the actual size.
size_t GetStreamingAttachmentSize(TRef attachment);

////////////////////////////////////////////////////////////////////////////////

class TAttachmentsInputStream
    : public NConcurrency::IAsyncZeroCopyInputStream
{
public:
    TAttachmentsInputStream(
        TClosure readCallback,
        IInvokerPtr compressionInvoker,
        std::optional<TDuration> timeout = {});

    TFuture<TSharedRef> Read() override;

    void EnqueuePayload(const TStreamingPayload& payload);
    void Abort(const TError& error, bool fireAborted = true);
    void AbortUnlessClosed(const TError& error, bool fireAborted = true);
    TStreamingFeedback GetFeedback() const;

    DEFINE_SIGNAL(void(), Aborted);

private:
    const TClosure ReadCallback_;
    const IInvokerPtr CompressionInvoker_;
    const std::optional<TDuration> Timeout_;

    struct TWindowPacket
    {
        TStreamingPayload Payload;
        std::vector<TSharedRef> DecompressedAttachments;
    };

    struct TQueueEntry
    {
        TSharedRef Attachment;
        size_t CompressedSize;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TSlidingWindow<TWindowPacket> Window_;
    TRingQueue<TQueueEntry> Queue_;
    TError Error_;
    TPromise<TSharedRef> Promise_;
    NConcurrency::TDelayedExecutorCookie TimeoutCookie_;

    std::atomic<ssize_t> ReadPosition_ = 0;
    bool Closed_ = false;

    void DecompressAndEnqueuePayload(const TStreamingPayload& payload);
    void DoEnqueuePayload(
        const TStreamingPayload& payload,
        const std::vector<TSharedRef>& decompressedAttachments);
    void DoAbort(
        TGuard<NThreading::TSpinLock>& guard,
        const TError& error,
        bool fireAborted = true);
    void OnTimeout();
};

DEFINE_REFCOUNTED_TYPE(TAttachmentsInputStream)

////////////////////////////////////////////////////////////////////////////////

class TAttachmentsOutputStream
    : public NConcurrency::IAsyncZeroCopyOutputStream
{
public:
    TAttachmentsOutputStream(
        NCompression::ECodec codec,
        IInvokerPtr compressionInvoker,
        TClosure pullCallback,
        ssize_t windowSize,
        std::optional<TDuration> timeout = {});

    TFuture<void> Write(const TSharedRef& data) override;
    TFuture<void> Close() override;

    void Abort(const TError& error);
    void AbortUnlessClosed(const TError& error, bool fireAborted = true);
    void HandleFeedback(const TStreamingFeedback& feedback);
    std::optional<TStreamingPayload> TryPull();

    DEFINE_SIGNAL(void(), Aborted);

private:
    const NCompression::ECodec Codec_;
    const IInvokerPtr CompressionInvoker_;
    const TClosure PullCallback_;
    const ssize_t WindowSize_;
    const std::optional<TDuration> Timeout_;

    struct TWindowPacket
    {
        TSharedRef Data;
        TPromise<void> Promise;
        NConcurrency::TDelayedExecutorCookie TimeoutCookie;
    };

    struct TConfirmationEntry
    {
        ssize_t Position;
        TPromise<void> Promise;
        NConcurrency::TDelayedExecutorCookie TimeoutCookie;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::atomic<size_t> CompressionSequenceNumber_ = {0};
    TSlidingWindow<TWindowPacket> Window_;
    TError Error_;
    TRingQueue<TSharedRef> DataQueue_;
    TRingQueue<TConfirmationEntry> ConfirmationQueue_;
    TPromise<void> ClosePromise_;
    NConcurrency::TDelayedExecutorCookie CloseTimeoutCookie_;
    bool Closed_ = false;
    ssize_t WritePosition_ = 0;
    ssize_t SentPosition_ = 0;
    ssize_t ReadPosition_ = 0;
    int PayloadSequenceNumber_ = 0;

    void OnWindowPacketsReady(
        TMutableRange<TWindowPacket> packets,
        TGuard<NThreading::TSpinLock>& guard);
    void MaybeInvokePullCallback(TGuard<NThreading::TSpinLock>& guard);
    bool CanPullMore(bool first) const;
    void DoAbort(
        TGuard<NThreading::TSpinLock>& guard,
        const TError& error,
        bool fireAborted = true);
    void OnTimeout();
};

DEFINE_REFCOUNTED_TYPE(TAttachmentsOutputStream)

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TRpcClientInputStream
    : public NConcurrency::IAsyncZeroCopyInputStream
{
public:
    TRpcClientInputStream(
        IClientRequestPtr request,
        TFuture<void> invokeResult);

    TFuture<TSharedRef> Read() override;

    ~TRpcClientInputStream();

private:
    const IClientRequestPtr Request_;
    const NConcurrency::IAsyncZeroCopyInputStreamPtr Underlying_;
    const TFuture<void> InvokeResult_;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWriterFeedback,
    (Handshake)
    (Success)
);

TError CheckWriterFeedback(
    const TSharedRef& ref,
    EWriterFeedback expectedFeedback);

TFuture<void> ExpectWriterFeedback(
    const NConcurrency::IAsyncZeroCopyInputStreamPtr& input,
    EWriterFeedback expectedFeedback);

TFuture<void> ExpectHandshake(
    const NConcurrency::IAsyncZeroCopyInputStreamPtr& input,
    bool feedbackEnabled);

////////////////////////////////////////////////////////////////////////////////

class TRpcClientOutputStream
    : public NConcurrency::IAsyncZeroCopyOutputStream
{
public:
    TRpcClientOutputStream(
        IClientRequestPtr request,
        TFuture<void> invokeResult,
        bool feedbackEnabled = false);

    TFuture<void> Write(const TSharedRef& data) override;
    TFuture<void> Close() override;

    ~TRpcClientOutputStream();

private:
    const IClientRequestPtr Request_;

    NConcurrency::IAsyncZeroCopyOutputStreamPtr Underlying_;
    TFuture<void> InvokeResult_;
    const TPromise<void> CloseResult_ = NewPromise<void>();

    NConcurrency::IAsyncZeroCopyInputStreamPtr FeedbackStream_;
    bool FeedbackEnabled_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TRingQueue<TPromise<void>> ConfirmationQueue_;
    TError Error_;

    void AbortOnError(const TError& error);
    void OnFeedback(const TErrorOr<TSharedRef>& refOrError);
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStreamFromInvokedRequest(
    IClientRequestPtr request,
    TFuture<void> invokeResult,
    bool feedbackEnabled = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Creates an input stream adapter from an uninvoked client request.
template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateRpcClientInputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request);

//! Creates an output stream adapter from an uninvoked client request.
template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    bool feedbackEnabled = false);

//! This variant expects the server to send one TSharedRef of meta information,
//! then close its stream.
template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    TCallback<void(TSharedRef)> metaHandler);

////////////////////////////////////////////////////////////////////////////////

//! Handles an incoming streaming request that uses the #CreateRpcClientInputStream
//! function.
void HandleInputStreamingRequest(
    const IServiceContextPtr& context,
    const std::function<TSharedRef()>& blockGenerator);

void HandleInputStreamingRequest(
    const IServiceContextPtr& context,
    const NConcurrency::IAsyncZeroCopyInputStreamPtr& input);

//! Handles an incoming streaming request that uses the #CreateRpcClientOutputStream
//! function with the same #feedbackEnabled value.
void HandleOutputStreamingRequest(
    const IServiceContextPtr& context,
    const std::function<void(TSharedRef)>& blockHandler,
    const std::function<void()>& finalizer,
    bool feedbackEnabled = false);

void HandleOutputStreamingRequest(
    const IServiceContextPtr& context,
    const NConcurrency::IAsyncZeroCopyOutputStreamPtr& output,
    bool feedbackEnabled = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define STREAM_INL_H_
#include "stream-inl.h"
#undef STREAM_INL_H_

