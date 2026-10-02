#include "compression.h"

#include "compression_detail.h"
#include "helpers.h"
#include "http.h"

#include <yt/yt/core/ytree/serialize.h>

#include <yt/yt/core/compression/dictionary_codec.h>

#include <yt/yt/core/concurrency/async_stream_helpers.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/streams/brotli/brotli.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/blockcodecs/stream.h>

#include <util/stream/zlib.h>

#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYT::NHttp {

using namespace NHttp::NDetail;
using namespace NConcurrency;
using namespace NHeaders;

////////////////////////////////////////////////////////////////////////////////

struct TStreamHolder
    : public TSharedRangeHolder
{
    explicit TStreamHolder(IAsyncOutputStreamPtr output)
        : Output(std::move(output))
    { }

    // NB: Cannot provide any reasonable GetTotalByteSize implementation.

    IAsyncOutputStreamPtr Output;
};

DEFINE_REFCOUNTED_TYPE(TStreamHolder)

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TSharedRefOutputStream::Write(const TSharedRef& buffer)
{
    Parts_.push_back(TSharedRef::MakeCopy<TDefaultSharedBlobTag>(buffer));
    return OKFuture;
}

TFuture<void> TSharedRefOutputStream::Flush()
{
    return OKFuture;
}

TFuture<void> TSharedRefOutputStream::Close()
{
    return OKFuture;
}

std::vector<TSharedRef> TSharedRefOutputStream::Finish()
{
    return std::move(Parts_);
}

////////////////////////////////////////////////////////////////////////////////

class TCompressingOutputStream
    : public IFlushableAsyncOutputStream
    , private IOutputStream
{
public:
    TCompressingOutputStream(
        IAsyncOutputStreamPtr underlying,
        TContentEncoding contentEncoding,
        IInvokerPtr compressionInvoker)
        : Underlying_(std::move(underlying))
        , ContentEncoding_(std::move(contentEncoding))
        , CompressionInvoker_(std::move(compressionInvoker))
    { }

    ~TCompressingOutputStream()
    {
        Destroying_ = true;
        Compressor_.reset();
    }

    TFuture<void> Write(const TSharedRef& buffer) override
    {
        return BIND(&TCompressingOutputStream::DoWriteCompressor, MakeStrong(this), buffer)
            .AsyncVia(CompressionInvoker_)
            .Run();
    }

    TFuture<void> Flush() override
    {
        return BIND(&TCompressingOutputStream::DoFlushCompressor, MakeStrong(this))
            .AsyncVia(CompressionInvoker_)
            .Run();
    }

    TFuture<void> Close() override
    {
        return BIND(&TCompressingOutputStream::DoFinishCompressor, MakeStrong(this))
            .AsyncVia(CompressionInvoker_)
            .Run();
    }

private:
    const IAsyncOutputStreamPtr Underlying_;
    const TContentEncoding ContentEncoding_;
    const IInvokerPtr CompressionInvoker_;

    // NB: Arcadia streams got some "interesting" ideas about
    // exception handling and the role of destructors in the C++
    // programming language.
    bool Destroying_ = false;
    bool Finished_ = false;
    std::unique_ptr<IOutputStream> Compressor_;


    void EnsureCompressorCreated()
    {
        if (Compressor_) {
            return;
        }

        if (ContentEncoding_.starts_with("z-")) {
            Compressor_.reset(new NBlockCodecs::TCodedOutput(
                this,
                NBlockCodecs::Codec(ContentEncoding_.substr(2)),
                DefaultCompressionBufferSize));
            return;
        }

        if (ContentEncoding_ == "gzip") {
            Compressor_.reset(new TZLibCompress(this, ZLib::GZip, 4, DefaultCompressionBufferSize));
            return;
        }

        if (ContentEncoding_ == "deflate") {
            Compressor_.reset(new TZLibCompress(this, ZLib::ZLib, 4, DefaultCompressionBufferSize));
            return;
        }

        if (ContentEncoding_ == "br") {
            Compressor_.reset(new TBrotliCompress(this, 3));
            return;
        }

        Compressor_ = TryDetectOptionalCompressors(ContentEncoding_, this);
        if (Compressor_) {
            return;
        }

        THROW_ERROR_EXCEPTION("Unsupported content encoding")
            .With("content_encoding", ToString(ContentEncoding_));
    }

    void DoWriteCompressor(const TSharedRef& buffer)
    {
        if (Finished_) {
            THROW_ERROR_EXCEPTION("Attempting write to closed compression stream");
        }

        EnsureCompressorCreated();
        Compressor_->Write(buffer.Begin(), buffer.Size());
    }

    void DoFlushCompressor()
    {
        EnsureCompressorCreated();
        Compressor_->Flush();
    }

    void DoFinishCompressor()
    {
        if (Finished_) {
            return;
        }
        Finished_ = true;
        EnsureCompressorCreated();
        Compressor_->Finish();
    }

    void DoWrite(const void* buf, size_t len) override
    {
        if (Destroying_) {
            return;
        }

        WaitForFast(Underlying_->Write(TSharedRef(buf, len, New<TStreamHolder>(this))))
            .ThrowOnError();
    }

    void DoFlush() override
    { }

    void DoFinish() override
    {
        if (Destroying_) {
            return;
        }

        WaitForFast(Underlying_->Close())
            .ThrowOnError();
    }
};

DEFINE_REFCOUNTED_TYPE(TCompressingOutputStream)

////////////////////////////////////////////////////////////////////////////////

class TDecompressingInputStream
    : public IAsyncInputStream
    , private IInputStream
{
public:
    TDecompressingInputStream(
        IAsyncZeroCopyInputStreamPtr underlying,
        TContentEncoding contentEncoding,
        IInvokerPtr compressionInvoker)
        : Underlying_(std::move(underlying))
        , ContentEncoding_(std::move(contentEncoding))
        , CompressionInvoker_(std::move(compressionInvoker))
    { }

    TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        return BIND(&TDecompressingInputStream::DoReadDecompressor, MakeStrong(this), buffer)
            .AsyncVia(CompressionInvoker_)
            .Run();
    }

private:
    const IAsyncZeroCopyInputStreamPtr Underlying_;
    const TContentEncoding ContentEncoding_;
    const IInvokerPtr CompressionInvoker_;

    std::unique_ptr<IInputStream> Decompressor_;

    bool CompressedEos_ = false;
    bool DecompressedEos_ = false;
    TSharedRef CompressedBlock_;
    size_t CompressedBlockOffset_ = 0;

    void EnsureDecompressorCreated()
    {
        if (Decompressor_) {
            return;
        }

        if (ContentEncoding_.starts_with("z-")) {
            Decompressor_.reset(new NBlockCodecs::TDecodedInput(
                this,
                NBlockCodecs::Codec(ContentEncoding_.substr(2))));
            return;
        }

        if (ContentEncoding_ == "gzip" || ContentEncoding_ == "deflate") {
            Decompressor_.reset(new TZLibDecompress(this, ZLib::Auto, DefaultCompressionBufferSize));
            return;
        }

        if (ContentEncoding_ == "br") {
            Decompressor_.reset(new TBrotliDecompress(this, DefaultCompressionBufferSize));
            return;
        }

        Decompressor_ = TryDetectOptionalDecompressors(ContentEncoding_, this);
        if (Decompressor_) {
            return;
        }

        THROW_ERROR_EXCEPTION("Unsupported content encoding")
            .With("content_encoding", ContentEncoding_);
    }

    size_t DoReadDecompressor(const TSharedMutableRef& uncompressedBuffer)
    {
        if (DecompressedEos_) {
            return 0;
        }

        EnsureDecompressorCreated();

        size_t offset = 0;
        while (offset < uncompressedBuffer.size()) {
            auto bytesRead = Decompressor_->Read(uncompressedBuffer.begin() + offset, uncompressedBuffer.size() - offset);
            if (bytesRead == 0) {
                DecompressedEos_ = true;
                break;
            }
            offset += bytesRead;
        }
        return offset;
    }

    size_t DoRead(void* buf, size_t len) override
    {
        if (CompressedEos_) {
            return 0;
        }

        size_t offset = 0;
        while (offset < len) {
            if (!CompressedBlock_) {
                CompressedBlockOffset_ = 0;
                CompressedBlock_ = WaitForFast(Underlying_->Read())
                    .ValueOrThrow();
                if (!CompressedBlock_) {
                    CompressedEos_ = true;
                    break;
                }
            }

            auto bytesRead = std::min(len - offset, CompressedBlock_.size() - CompressedBlockOffset_);
            memcpy(static_cast<char*>(buf) + offset, CompressedBlock_.begin() + CompressedBlockOffset_, bytesRead);
            offset += bytesRead;
            CompressedBlockOffset_ += bytesRead;

            if (CompressedBlockOffset_ == CompressedBlock_.size()) {
                CompressedBlock_ = {};
            }
        }
        return offset;
    }
};

DEFINE_REFCOUNTED_TYPE(TDecompressingInputStream)

////////////////////////////////////////////////////////////////////////////////

bool IsContentEncodingSupported(const TContentEncoding& contentEncoding)
{
    if (contentEncoding.starts_with("z-")) {
        try {
            NBlockCodecs::Codec(contentEncoding.substr(2));
            return true;
        } catch (const NBlockCodecs::TNotFound&) {
            return false;
        }
    }

    if (Find(GetInternallySupportedContentEncodings(), contentEncoding) != GetInternallySupportedContentEncodings().end()) {
        return true;
    }

    return false;
}

const std::vector<TContentEncoding>& GetSupportedContentEncodings()
{
    static const auto result = [] {
        auto result = GetInternallySupportedContentEncodings();
        for (auto blockCodec : NBlockCodecs::ListAllCodecs()) {
            result.push_back(std::string("z-") + std::string(blockCodec));
        }
        return result;
    }();
    return result;
}

// NB: Does not implement the spec, but a reasonable approximation: quality values are ignored
// and the first supported encoding listed by the client wins.
TErrorOr<TContentEncoding> GetBestAcceptedContentEncoding(TStringBuf clientAcceptEncodingHeader)
{
    for (const auto& part : StringSplitter(clientAcceptEncodingHeader).Split(',')) {
        TContentEncoding candidate(StripString(part.Token().Before(';')));
        if (candidate == "x-lzop") {
            continue;
        }
        if (IsContentEncodingSupported(candidate)) {
            return candidate;
        }
    }

    return TError("Could not determine feasible content encoding given accept encoding constraints")
        .With("client_accept_encoding", clientAcceptEncodingHeader);
}

IFlushableAsyncOutputStreamPtr CreateCompressingAdapter(
    IAsyncOutputStreamPtr underlying,
    TContentEncoding contentEncoding,
    IInvokerPtr compressionInvoker)
{
    return New<TCompressingOutputStream>(
        std::move(underlying),
        std::move(contentEncoding),
        std::move(compressionInvoker));
}

IAsyncInputStreamPtr CreateDecompressingAdapter(
    IAsyncZeroCopyInputStreamPtr underlying,
    TContentEncoding contentEncoding,
    IInvokerPtr compressionInvoker)
{
    return New<TDecompressingInputStream>(
        std::move(underlying),
        std::move(contentEncoding),
        std::move(compressionInvoker));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TDecodingRequest
    : public IRequest
{
public:
    TDecodingRequest(
        IRequestPtr underlying,
        TContentEncoding contentEncoding,
        IInvokerPtr compressionInvoker)
        : Underlying_(std::move(underlying))
        , Decoder_(CreateZeroCopyAdapter(CreateDecompressingAdapter(
            Underlying_,
            std::move(contentEncoding),
            std::move(compressionInvoker))))
    { }

    TFuture<TSharedRef> Read() override
    {
        return Decoder_->Read();
    }

    std::pair<int, int> GetVersion() override
    {
        return Underlying_->GetVersion();
    }

    EMethod GetMethod() override
    {
        return Underlying_->GetMethod();
    }

    const TUrlRef& GetUrl() override
    {
        return Underlying_->GetUrl();
    }

    const THeadersPtr& GetHeaders() override
    {
        return Underlying_->GetHeaders();
    }

    const NNet::TNetworkAddress& GetRemoteAddress() const override
    {
        return Underlying_->GetRemoteAddress();
    }

    TConnectionId GetConnectionId() const override
    {
        return Underlying_->GetConnectionId();
    }

    TRequestId GetRequestId() const override
    {
        return Underlying_->GetRequestId();
    }

    i64 GetReadByteCount() const override
    {
        return Underlying_->GetReadByteCount();
    }

    TInstant GetStartTime() const override
    {
        return Underlying_->GetStartTime();
    }

    bool IsHttps() const override
    {
        return Underlying_->IsHttps();
    }

    int GetPort() const override
    {
        return Underlying_->GetPort();
    }

private:
    const IRequestPtr Underlying_;
    const IAsyncZeroCopyInputStreamPtr Decoder_;
};

////////////////////////////////////////////////////////////////////////////////

class TEncodingResponseWriter
    : public IResponseWriter
{
public:
    TEncodingResponseWriter(
        IResponseWriterPtr underlying,
        TContentEncoding contentEncoding,
        IInvokerPtr compressionInvoker)
        : Underlying_(std::move(underlying))
        , ContentEncoding_(std::move(contentEncoding))
        , CompressionInvoker_(std::move(compressionInvoker))
    { }

    const THeadersPtr& GetHeaders() override
    {
        return Underlying_->GetHeaders();
    }

    const THeadersPtr& GetTrailers() override
    {
        return Underlying_->GetTrailers();
    }

    bool AreHeadersFlushed() const override
    {
        return Underlying_->AreHeadersFlushed();
    }

    std::optional<EStatusCode> GetStatus() const override
    {
        return Underlying_->GetStatus();
    }

    void SetStatus(EStatusCode status) override
    {
        Underlying_->SetStatus(status);
    }

    void AddConnectionCloseHeader() override
    {
        Underlying_->AddConnectionCloseHeader();
    }

    i64 GetWriteByteCount() const override
    {
        return Underlying_->GetWriteByteCount();
    }

    TFuture<void> Write(const TSharedRef& buffer) override
    {
        return GetBodyStream()->Write(buffer);
    }

    TFuture<void> Flush() override
    {
        return GetBodyStream()->Flush();
    }

    TFuture<void> Close() override
    {
        if (!Encoding_) {
            return Underlying_->Close();
        }
        return BodyStream_->Close().Apply(BIND([underlying = Underlying_] {
            return underlying->Close();
        }));
    }

    TFuture<void> WriteBody(const TSharedRef& smallBody) override
    {
        return WriteBody(TRange(&smallBody, 1));
    }

    TFuture<void> WriteBody(TRange<TSharedRef> bodyParts) override
    {
        if (!BodyStream_ && !ShouldEncode()) {
            return Underlying_->WriteBody(bodyParts);
        }
        std::vector<TSharedRef> parts(bodyParts.begin(), bodyParts.end());
        return BIND([this, this_ = MakeStrong(this), parts = std::move(parts)] {
            for (const auto& part : parts) {
                WaitFor(Write(part))
                    .ThrowOnError();
            }
            WaitFor(Close())
                .ThrowOnError();
        })
            .AsyncVia(CompressionInvoker_)
            .Run();
    }

private:
    const IResponseWriterPtr Underlying_;
    const TContentEncoding ContentEncoding_;
    const IInvokerPtr CompressionInvoker_;

    IFlushableAsyncOutputStreamPtr BodyStream_;
    bool Encoding_ = false;

    bool ShouldEncode() const
    {
        return
            ContentEncoding_ != IdentityContentEncoding &&
            !Underlying_->GetHeaders()->Find(ContentEncodingHeaderName);
    }

    const IFlushableAsyncOutputStreamPtr& GetBodyStream()
    {
        if (BodyStream_) {
            return BodyStream_;
        }
        if (ShouldEncode()) {
            const auto& headers = Underlying_->GetHeaders();
            headers->Set(ContentEncodingHeaderName, ContentEncoding_);
            headers->Add(VaryHeaderName, AcceptEncodingHeaderName);
            BodyStream_ = CreateCompressingAdapter(Underlying_, ContentEncoding_, CompressionInvoker_);
            Encoding_ = true;
        } else {
            BodyStream_ = Underlying_;
        }
        return BodyStream_;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TContentEncodingHttpHandler
    : public IHttpHandler
{
public:
    TContentEncodingHttpHandler(IHttpHandlerPtr underlying, IInvokerPtr compressionInvoker)
        : Underlying_(std::move(underlying))
        , CompressionInvoker_(std::move(compressionInvoker))
    { }

    void HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp) override
    {
        auto decodingReq = req;
        if (const auto* requestContentEncoding = req->GetHeaders()->Find(ContentEncodingHeaderName);
            requestContentEncoding && *requestContentEncoding != IdentityContentEncoding)
        {
            if (!IsContentEncodingSupported(*requestContentEncoding)) {
                FillYTErrorHeaders(rsp, TError("Unsupported content encoding %Qv", *requestContentEncoding));
                rsp->SetStatus(EStatusCode::UnsupportedMediaType);
                WaitFor(rsp->Close())
                    .ThrowOnError();
                return;
            }
            decodingReq = New<TDecodingRequest>(req, *requestContentEncoding, CompressionInvoker_);
        }

        auto responseContentEncoding = IdentityContentEncoding;
        if (const auto* acceptEncoding = req->GetHeaders()->Find(AcceptEncodingHeaderName)) {
            auto contentEncodingOrError = GetBestAcceptedContentEncoding(*acceptEncoding);
            if (contentEncodingOrError.IsOK()) {
                responseContentEncoding = contentEncodingOrError.Value();
            }
        }

        if (responseContentEncoding == IdentityContentEncoding) {
            Underlying_->HandleRequest(decodingReq, rsp);
            return;
        }

        Underlying_->HandleRequest(
            decodingReq,
            New<TEncodingResponseWriter>(rsp, std::move(responseContentEncoding), CompressionInvoker_));
    }

private:
    const IHttpHandlerPtr Underlying_;
    const IInvokerPtr CompressionInvoker_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IHttpHandlerPtr CreateContentEncodingHttpHandler(IHttpHandlerPtr underlying, IInvokerPtr compressionInvoker)
{
    return New<TContentEncodingHttpHandler>(std::move(underlying), std::move(compressionInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
