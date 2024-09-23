#include "stream.h"
#include "client.h"
#include "service_detail.h"

#include <yt/yt/core/compression/codec.h>

namespace NYT::NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr ssize_t MaxWindowSize = 16384;

////////////////////////////////////////////////////////////////////////////////

size_t GetStreamingAttachmentSize(TRef attachment)
{
    if (!attachment || attachment.Size() == 0) {
        return 1;
    } else {
        return attachment.Size();
    }
}

////////////////////////////////////////////////////////////////////////////////

TAttachmentsInputStream::TAttachmentsInputStream(
    TClosure readCallback,
    IInvokerPtr compressionInvoker,
    std::optional<TDuration> timeout)
    : ReadCallback_(std::move(readCallback))
    , CompressionInvoker_(std::move(compressionInvoker))
    , Timeout_(timeout)
    , Window_(MaxWindowSize)
{ }

TFuture<TSharedRef> TAttachmentsInputStream::Read()
{
    auto guard = Guard(Lock_);

    // Failure here indicates an attempt to read past EOSs.
    if (Closed_) {
        return MakeFuture<TSharedRef>(TError("Stream is already closed"));
    }

    if (!Error_.IsOK()) {
        return MakeFuture<TSharedRef>(Error_);
    }

    // Failure here indicates that another Read request is already in progress.
    YT_VERIFY(!Promise_);

    if (Queue_.empty()) {
        Promise_ = NewPromise<TSharedRef>();
        if (Timeout_) {
            TimeoutCookie_ = TDelayedExecutor::Submit(
                BIND(&TAttachmentsInputStream::OnTimeout, MakeWeak(this)),
                *Timeout_);
        }
        return Promise_.ToFuture();
    } else {
        auto entry = std::move(Queue_.front());
        Queue_.pop();
        ReadPosition_ += entry.CompressedSize;
        if (!entry.Attachment) {
            YT_VERIFY(!Closed_);
            Closed_ = true;
        }
        guard.Release();
        ReadCallback_();
        return MakeFuture(entry.Attachment);
    }
}

void TAttachmentsInputStream::EnqueuePayload(const TStreamingPayload& payload)
{
    if (payload.Codec == NCompression::ECodec::None) {
        DoEnqueuePayload(payload, payload.Attachments);
    } else {
        CompressionInvoker_->Invoke(
            BIND(&TAttachmentsInputStream::DecompressAndEnqueuePayload, MakeWeak(this), payload));
    }
}

void TAttachmentsInputStream::DecompressAndEnqueuePayload(const TStreamingPayload& payload)
{
    try {
        std::vector<TSharedRef> decompressedAttachments;
        decompressedAttachments.reserve(payload.Attachments.size());
        auto* codec = NCompression::GetCodec(payload.Codec);
        for (const auto& attachment : payload.Attachments) {
            TSharedRef decompressedAttachment;
            if (attachment) {
                decompressedAttachment = codec->Decompress(attachment);
            }
            decompressedAttachments.push_back(std::move(decompressedAttachment));
        }
        DoEnqueuePayload(payload, decompressedAttachments);
    } catch (const std::exception& ex) {
        Abort(ex, false);
    }
}

void TAttachmentsInputStream::DoEnqueuePayload(
    const TStreamingPayload& payload,
    const std::vector<TSharedRef>& decompressedAttachments)
{
    auto guard = Guard(Lock_);

    if (!Error_.IsOK()) {
        return;
    }

    Window_.AddPacket(
        payload.SequenceNumber,
        {
            payload,
            decompressedAttachments,
        },
        [&] (auto&& packet) {
            for (size_t index = 0; index < packet.Payload.Attachments.size(); ++index) {
                Queue_.push({
                    packet.DecompressedAttachments[index],
                    GetStreamingAttachmentSize(packet.Payload.Attachments[index])
                });
            }
        });

    if (Promise_ && !Queue_.empty()) {
        auto entry = std::move(Queue_.front());
        Queue_.pop();

        auto promise = std::move(Promise_);

        ReadPosition_ += entry.CompressedSize;

        if (!entry.Attachment) {
            YT_VERIFY(!Closed_);
            Closed_ = true;
        }

        TDelayedExecutor::CancelAndClear(TimeoutCookie_);

        guard.Release();

        promise.Set(std::move(entry.Attachment));
        ReadCallback_();
    }
}

void TAttachmentsInputStream::Abort(const TError& error, bool fireAborted)
{
    auto guard = Guard(Lock_);
    DoAbort(guard, error, fireAborted);
}

void TAttachmentsInputStream::AbortUnlessClosed(const TError& error, bool fireAborted)
{
    auto guard = Guard(Lock_);

    if (Closed_) {
        return;
    }

    static const auto FinishedError = TError("Request finished");
    DoAbort(
        guard,
        error.IsOK() ? FinishedError : error,
        fireAborted);
}

void TAttachmentsInputStream::DoAbort(TGuard<NThreading::TSpinLock>& guard, const TError& error, bool fireAborted)
{
    if (!Error_.IsOK()) {
        return;
    }

    Error_ = error;

    auto promise = Promise_;

    guard.Release();

    if (promise) {
        promise.Set(error);
    }

    if (fireAborted) {
        Aborted_.Fire();
    }
}

void TAttachmentsInputStream::OnTimeout()
{
    Abort(TError(NYT::EErrorCode::Timeout, "Attachments stream read timed out")
        << TErrorAttribute("timeout", *Timeout_));
}

TStreamingFeedback TAttachmentsInputStream::GetFeedback() const
{
    return TStreamingFeedback{
        ReadPosition_.load()
    };
}

////////////////////////////////////////////////////////////////////////////////

TAttachmentsOutputStream::TAttachmentsOutputStream(
    NCompression::ECodec codec,
    IInvokerPtr compressisonInvoker,
    TClosure pullCallback,
    ssize_t windowSize,
    std::optional<TDuration> timeout)
    : Codec_(codec)
    , CompressionInvoker_(std::move(compressisonInvoker))
    , PullCallback_(std::move(pullCallback))
    , WindowSize_(windowSize)
    , Timeout_(timeout)
    , Window_(std::numeric_limits<ssize_t>::max())
{ }

TFuture<void> TAttachmentsOutputStream::Write(const TSharedRef& data)
{
    YT_VERIFY(data);
    auto promise = NewPromise<void>();
    TDelayedExecutorCookie timeoutCookie;
    if (Timeout_) {
        timeoutCookie = TDelayedExecutor::Submit(
            BIND(&TAttachmentsOutputStream::OnTimeout, MakeWeak(this)),
            *Timeout_);
    }
    if (Codec_ == NCompression::ECodec::None) {
        auto guard = Guard(Lock_);
        TWindowPacket packet{
            data,
            promise,
            std::move(timeoutCookie)
        };
        OnWindowPacketsReady(TMutableRange(&packet, 1), guard);
    } else {
        auto sequenceNumber = CompressionSequenceNumber_++;
        CompressionInvoker_->Invoke(BIND([=, this, this_ = MakeStrong(this)] {
            auto* codec = NCompression::GetCodec(Codec_);
            auto compressedData = codec->Compress(data);
            auto guard = Guard(Lock_);
            std::vector<TWindowPacket> packets;
            Window_.AddPacket(
                sequenceNumber,
                {compressedData, promise, std::move(timeoutCookie)},
                [&] (auto&& packet) {
                    packets.push_back(std::move(packet));
                });
            OnWindowPacketsReady(packets, guard);
        }));
    }
    return promise.ToFuture();
}

void TAttachmentsOutputStream::OnWindowPacketsReady(TMutableRange<TWindowPacket> packets, TGuard<NThreading::TSpinLock>& guard)
{
    if (ClosePromise_) {
        guard.Release();
        TError error("Stream is already closed");
        for (auto& packet : packets) {
            TDelayedExecutor::CancelAndClear(packet.TimeoutCookie);
            packet.Promise.Set(error);
        }
        return;
    }

    if (!Error_.IsOK()) {
        guard.Release();
        for (auto& packet : packets) {
            TDelayedExecutor::CancelAndClear(packet.TimeoutCookie);
            packet.Promise.Set(Error_);
        }
        return;
    }

    std::vector<TPromise<void>> promisesToSet;
    for (auto& packet : packets) {
        WritePosition_ += GetStreamingAttachmentSize(packet.Data);
        DataQueue_.push(std::move(packet.Data));

        if (WritePosition_ - ReadPosition_ <= WindowSize_) {
            TDelayedExecutor::CancelAndClear(packet.TimeoutCookie);
            promisesToSet.push_back(std::move(packet.Promise));
            ConfirmationQueue_.push({
                .Position = WritePosition_
            });
        } else {
            ConfirmationQueue_.push({
                .Position = WritePosition_,
                .Promise = std::move(packet.Promise),
                .TimeoutCookie = std::move(packet.TimeoutCookie)
            });
        }
    }

    MaybeInvokePullCallback(guard);

    guard.Release();

    for (const auto& promise : promisesToSet) {
        promise.Set();
    }
}

TFuture<void> TAttachmentsOutputStream::Close()
{
    auto guard = Guard(Lock_);

    if (!Error_.IsOK()) {
        return MakeFuture(Error_);
    }

    if (ClosePromise_) {
        return ClosePromise_.ToFuture();
    }

    auto promise = ClosePromise_ = NewPromise<void>();
    if (Timeout_) {
        CloseTimeoutCookie_ = TDelayedExecutor::Submit(
            BIND(&TAttachmentsOutputStream::OnTimeout, MakeWeak(this)),
            *Timeout_);
    }

    TSharedRef nullAttachment;
    DataQueue_.push(nullAttachment);
    WritePosition_ += GetStreamingAttachmentSize(nullAttachment);

    ConfirmationQueue_.push({
        .Position = WritePosition_
    });

    MaybeInvokePullCallback(guard);

    return promise.ToFuture();
}

void TAttachmentsOutputStream::Abort(const TError& error)
{
    auto guard = Guard(Lock_);

    DoAbort(guard, error);
}

void TAttachmentsOutputStream::AbortUnlessClosed(const TError& error, bool fireAborted)
{
    auto guard = Guard(Lock_);

    if (Closed_) {
        return;
    }

    DoAbort(
        guard,
        error.IsOK() ? TError("Request is already completed") : error,
        fireAborted);
}

void TAttachmentsOutputStream::DoAbort(TGuard<NThreading::TSpinLock>& guard, const TError& error, bool fireAborted)
{
    if (!Error_.IsOK()) {
        return;
    }

    Error_ = error;

    std::vector<TPromise<void>> promises;
    promises.reserve(ConfirmationQueue_.size());
    while (!ConfirmationQueue_.empty()) {
        auto& entry = ConfirmationQueue_.front();
        TDelayedExecutor::CancelAndClear(entry.TimeoutCookie);
        promises.push_back(std::move(entry.Promise));
        ConfirmationQueue_.pop();
    }

    if (ClosePromise_) {
        promises.push_back(ClosePromise_);
        TDelayedExecutor::CancelAndClear(CloseTimeoutCookie_);
    }

    guard.Release();

    for (const auto& promise : promises) {
        if (promise) {
            // Avoid double-setting ClosePromise_.
            promise.TrySet(error);
        }
    }

    if (fireAborted) {
        Aborted_.Fire();
    }
}

void TAttachmentsOutputStream::OnTimeout()
{
    Abort(TError(NYT::EErrorCode::Timeout, "Attachments stream write timed out")
        << TErrorAttribute("timeout", *Timeout_));
}

void TAttachmentsOutputStream::HandleFeedback(const TStreamingFeedback& feedback)
{
    auto guard = Guard(Lock_);

    if (!Error_.IsOK()) {
        return;
    }

    if (ReadPosition_ >= feedback.ReadPosition) {
        return;
    }

    if (feedback.ReadPosition > WritePosition_) {
        THROW_ERROR_EXCEPTION("Stream read position exceeds write position: %v > %v",
            feedback.ReadPosition,
            WritePosition_);
    }

    ReadPosition_ = feedback.ReadPosition;

    std::vector<TPromise<void>> promises;
    promises.reserve(ConfirmationQueue_.size());

    while (!ConfirmationQueue_.empty() &&
            ConfirmationQueue_.front().Position <= ReadPosition_ + WindowSize_)
    {
        auto& entry = ConfirmationQueue_.front();
        TDelayedExecutor::CancelAndClear(entry.TimeoutCookie);
        promises.push_back(std::move(entry.Promise));
        ConfirmationQueue_.pop();
    }

    if (ClosePromise_ && ReadPosition_ == WritePosition_) {
        promises.push_back(ClosePromise_);
        TDelayedExecutor::CancelAndClear(CloseTimeoutCookie_);
        Closed_ = true;
    }

    MaybeInvokePullCallback(guard);

    guard.Release();

    for (const auto& promise : promises) {
        if (promise) {
            // Avoid double-setting ClosePromise_.
            promise.TrySet();
        }
    }
}

std::optional<TStreamingPayload> TAttachmentsOutputStream::TryPull()
{
    auto guard = Guard(Lock_);

    if (!Error_.IsOK()) {
        return std::nullopt;
    }

    TStreamingPayload result;
    result.Codec = Codec_;
    while (CanPullMore(result.Attachments.empty())) {
        auto attachment = std::move(DataQueue_.front());
        SentPosition_ += GetStreamingAttachmentSize(attachment);
        result.Attachments.push_back(std::move(attachment));
        DataQueue_.pop();
    }

    if (result.Attachments.empty()) {
        return std::nullopt;
    }

    result.SequenceNumber = PayloadSequenceNumber_++;
    return result;
}

void TAttachmentsOutputStream::MaybeInvokePullCallback(TGuard<NThreading::TSpinLock>& guard)
{
    if (CanPullMore(true)) {
        guard.Release();
        PullCallback_();
    }
}

bool TAttachmentsOutputStream::CanPullMore(bool first) const
{
    if (DataQueue_.empty()) {
        return false;
    }

    if (SentPosition_ - ReadPosition_ + static_cast<ssize_t>(GetStreamingAttachmentSize(DataQueue_.front())) <= WindowSize_) {
        return true;
    }

    if (first && SentPosition_ == ReadPosition_) {
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TRpcClientInputStream::TRpcClientInputStream(
    IClientRequestPtr request,
    TFuture<void> invokeResult)
    : Request_(std::move(request))
    , Underlying_(Request_->GetResponseAttachmentsStream())
    , InvokeResult_(std::move(invokeResult))
{
    YT_VERIFY(Underlying_);
    YT_VERIFY(InvokeResult_);
}

TFuture<TSharedRef> TRpcClientInputStream::Read()
{
    return Underlying_->Read()
        .Apply(BIND([invokeResult = InvokeResult_] (const TSharedRef& ref) mutable
    {
        if (ref) {
            return MakeFuture(ref);
        }

        return invokeResult.Apply(BIND([] {
            return TSharedRef();
        }));
    }));
}

TRpcClientInputStream::~TRpcClientInputStream()
{
    // Here we assume that canceling a completed request is safe.
    InvokeResult_.Cancel(TError("RPC input stream destroyed"));
}

////////////////////////////////////////////////////////////////////////////////

TError CheckWriterFeedback(
    const TSharedRef& ref,
    EWriterFeedback expectedFeedback)
{
    NProto::TWriterFeedback protoFeedback;
    if (!TryDeserializeProto(&protoFeedback, ref)) {
        return TError("Failed to deserialize writer feedback");
    }

    EWriterFeedback actualFeedback;
    if (!TryEnumCast(protoFeedback.feedback(), &actualFeedback)) {
        return TError("Invalid writer feedback value %v",
            static_cast<int>(protoFeedback.feedback()));
    }

    if (actualFeedback != expectedFeedback) {
        return TError("Received a wrong kind of writer feedback: %v instead of %v",
            actualFeedback,
            expectedFeedback);
    }

    return TError();
}

TFuture<void> ExpectWriterFeedback(
    const IAsyncZeroCopyInputStreamPtr& input,
    EWriterFeedback expectedFeedback)
{
    YT_VERIFY(input);
    return input->Read().Apply(BIND([=] (const TSharedRef& ref) {
        return MakeFuture(CheckWriterFeedback(ref, expectedFeedback));
    }));
}

TFuture<void> ExpectHandshake(
    const IAsyncZeroCopyInputStreamPtr& input,
    bool feedbackEnabled)
{
    return feedbackEnabled
        ? ExpectWriterFeedback(input, NDetail::EWriterFeedback::Handshake)
        : ExpectEndOfStream(input);
}

////////////////////////////////////////////////////////////////////////////////

TRpcClientOutputStream::TRpcClientOutputStream(
    IClientRequestPtr request,
    TFuture<void> invokeResult,
    bool feedbackEnabled)
    : Request_(std::move(request))
    , InvokeResult_(std::move(invokeResult))
    , FeedbackEnabled_(feedbackEnabled)
{
    YT_VERIFY(Request_);

    Underlying_ = Request_->GetRequestAttachmentsStream();
    YT_VERIFY(Underlying_);

    FeedbackStream_ = Request_->GetResponseAttachmentsStream();
    YT_VERIFY(FeedbackStream_);

    if (FeedbackEnabled_) {
        FeedbackStream_->Read().Subscribe(
            BIND(&TRpcClientOutputStream::OnFeedback, MakeWeak(this)));
    }
}

TFuture<void> TRpcClientOutputStream::Write(const TSharedRef& data)
{
    if (FeedbackEnabled_) {
        auto promise = NewPromise<void>();
        TFuture<void> writeResult;
        {
            auto guard = Guard(SpinLock_);
            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }

            ConfirmationQueue_.push(promise);
            writeResult = Underlying_->Write(data);
        }

        writeResult.Subscribe(
            BIND(&TRpcClientOutputStream::AbortOnError, MakeWeak(this)));

        return promise.ToFuture();
    } else {
        auto writeResult = Underlying_->Write(data);
        writeResult.Subscribe(
            BIND(&TRpcClientOutputStream::AbortOnError, MakeWeak(this)));
        return writeResult;
    }
}

TFuture<void> TRpcClientOutputStream::Close()
{
    CloseResult_.TrySetFrom(Underlying_->Close());
    return CloseResult_.ToFuture().Apply(BIND([invokeResult = InvokeResult_] {
        return invokeResult;
    }));
}

void TRpcClientOutputStream::AbortOnError(const TError& error)
{
    if (error.IsOK()) {
        return;
    }

    auto guard = Guard(SpinLock_);

    if (!Error_.IsOK()) {
        return;
    }

    Error_ = error;

    std::vector<TPromise<void>> promises;
    promises.reserve(ConfirmationQueue_.size());

    while (!ConfirmationQueue_.empty()) {
        promises.push_back(std::move(ConfirmationQueue_.front()));
        ConfirmationQueue_.pop();
    }

    guard.Release();

    for (const auto& promise : promises) {
        if (promise) {
            promise.Set(error);
        }
    }

    InvokeResult_.Cancel(error);
}

void TRpcClientOutputStream::OnFeedback(const TErrorOr<TSharedRef>& refOrError)
{
    YT_VERIFY(FeedbackEnabled_);

    auto error = TError(refOrError);
    if (error.IsOK()) {
        const auto& ref = refOrError.Value();
        if (!ref) {
            auto guard = Guard(SpinLock_);

            if (ConfirmationQueue_.empty()) {
                guard.Release();
                CloseResult_.TrySetFrom(Underlying_->Close());
                return;
            }
            error = TError(NRpc::EErrorCode::ProtocolError, "Expected a positive writer feedback, received a null ref");
        } else {
            error = CheckWriterFeedback(ref, EWriterFeedback::Success);
        }
    }

    TPromise<void> promise;

    {
        auto guard = Guard(SpinLock_);

        if (!Error_.IsOK()) {
            return;
        }

        if (!error.IsOK()) {
            guard.Release();
            AbortOnError(error);
            return;
        }

        YT_VERIFY(!ConfirmationQueue_.empty());
        promise = std::move(ConfirmationQueue_.front());
        ConfirmationQueue_.pop();
    }

    promise.Set();

    FeedbackStream_->Read().Subscribe(
        BIND(&TRpcClientOutputStream::OnFeedback, MakeWeak(this)));
}

TRpcClientOutputStream::~TRpcClientOutputStream()
{
    InvokeResult_.Cancel(TError("RPC output stream destroyed"));
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef GenerateWriterFeedbackMessage(
    EWriterFeedback feedback)
{
    NProto::TWriterFeedback protoFeedback;
    protoFeedback.set_feedback(static_cast<NProto::EWriterFeedback>(feedback));
    return SerializeProtoToRef(protoFeedback);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStreamFromInvokedRequest(
    IClientRequestPtr request,
    TFuture<void> invokeResult,
    bool feedbackEnabled)
{
    auto handshakeResult = NDetail::ExpectHandshake(
        request->GetResponseAttachmentsStream(),
        feedbackEnabled);

    return handshakeResult.Apply(BIND([
        =,
        request = std::move(request),
        invokeResult = std::move(invokeResult)
    ] {
        return New<NDetail::TRpcClientOutputStream>(
            std::move(request),
            std::move(invokeResult),
            feedbackEnabled);
    })).template As<IAsyncZeroCopyOutputStreamPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void HandleInputStreamingRequest(
    const IServiceContextPtr& context,
    const std::function<TSharedRef()>& blockGenerator)
{
    auto inputStream = context->GetRequestAttachmentsStream();
    YT_VERIFY(inputStream);
    WaitFor(ExpectEndOfStream(inputStream))
        .ThrowOnError();

    auto outputStream = context->GetResponseAttachmentsStream();
    YT_VERIFY(outputStream);

    while (auto block = blockGenerator()) {
        WaitFor(outputStream->Write(block))
            .ThrowOnError();
    }

    WaitFor(outputStream->Close())
        .ThrowOnError();

    context->Reply(TError());
}

void HandleInputStreamingRequest(
    const IServiceContextPtr& context,
    const IAsyncZeroCopyInputStreamPtr& input)
{
    HandleInputStreamingRequest(
        context,
        [&] {
            return WaitFor(input->Read())
                .ValueOrThrow();
        });
}

void HandleOutputStreamingRequest(
    const IServiceContextPtr& context,
    const std::function<void(TSharedRef)>& blockHandler,
    const std::function<void()>& finalizer,
    bool feedbackEnabled)
{
    auto inputStream = context->GetRequestAttachmentsStream();
    YT_VERIFY(inputStream);
    auto outputStream = context->GetResponseAttachmentsStream();
    YT_VERIFY(outputStream);

    auto blockGenerator = [&] {
        return WaitFor(inputStream->Read())
            .ValueOrThrow();
    };

    if (feedbackEnabled) {
        auto handshakeRef = GenerateWriterFeedbackMessage(
            NDetail::EWriterFeedback::Handshake);
        WaitFor(outputStream->Write(handshakeRef))
            .ThrowOnError();

        while (auto block = blockGenerator()) {
            blockHandler(std::move(block));

            auto ackRef = GenerateWriterFeedbackMessage(NDetail::EWriterFeedback::Success);
            WaitFor(outputStream->Write(ackRef))
                .ThrowOnError();
        }

        WaitFor(outputStream->Close())
            .ThrowOnError();
    } else {
        WaitFor(outputStream->Close())
            .ThrowOnError();

        while (auto block = blockGenerator()) {
            blockHandler(std::move(block));
        }
    }

    finalizer();

    context->Reply(TError());
}

void HandleOutputStreamingRequest(
    const IServiceContextPtr& context,
    const IAsyncZeroCopyOutputStreamPtr& output,
    bool feedbackEnabled)
{
    HandleOutputStreamingRequest(
        context,
        [&] (TSharedRef block) {
            WaitFor(output->Write(std::move(block)))
                .ThrowOnError();
        },
        [&] {
            WaitFor(output->Close())
                .ThrowOnError();
        },
        feedbackEnabled);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

