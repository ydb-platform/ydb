#include "compression.h"

#include "compression_detail.h"

#include <yt/yt/core/ytree/serialize.h>

#include <yt/yt/core/compression/dictionary_codec.h>

#include <library/cpp/streams/brotli/brotli.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/blockcodecs/stream.h>

#include <util/stream/zlib.h>

namespace NYT::NHttp {

using namespace NHttp::NDetail;
using namespace NConcurrency;

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
    return VoidFuture;
}

TFuture<void> TSharedRefOutputStream::Flush()
{
    return VoidFuture;
}

TFuture<void> TSharedRefOutputStream::Close()
{
    return VoidFuture;
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

        if (ContentEncoding_.StartsWith("z-")) {
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

        if (Compressor_ = TryDetectOptionalCompressors(ContentEncoding_, this)) {
            return;
        }

        THROW_ERROR_EXCEPTION("Unsupported content encoding")
            << TErrorAttribute("content_encoding", ToString(ContentEncoding_));
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

        if (ContentEncoding_.StartsWith("z-")) {
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

        if (Decompressor_ = TryDetectOptionalDecompressors(ContentEncoding_, this)) {
            return;
        }

        THROW_ERROR_EXCEPTION("Unsupported content encoding")
            << TErrorAttribute("content_encoding", ContentEncoding_);
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
    if (contentEncoding.StartsWith("z-")) {
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
            result.push_back(TString("z-") + blockCodec);
        }
        return result;
    }();
    return result;
}

// NB: Does not implement the spec, but a reasonable approximation.
TErrorOr<TContentEncoding> GetBestAcceptedContentEncoding(const TString& clientAcceptEncodingHeader)
{
    auto bestPosition = TString::npos;
    std::optional<TContentEncoding> bestEncoding;

    auto checkCandidate = [&] (const TString& candidate, size_t position) {
        if (position != TString::npos && (bestPosition == TString::npos || position < bestPosition)) {
            bestEncoding = candidate;
            bestPosition = position;
        }
    };

    for (const auto& candidate : GetInternallySupportedContentEncodings()) {
        if (candidate == "x-lzop") {
            continue;
        }

        auto position = clientAcceptEncodingHeader.find(candidate);
        checkCandidate(candidate, position);
    }

    for (const auto& blockcodec : NBlockCodecs::ListAllCodecs()) {
        auto candidate = TString("z-") + blockcodec;

        auto position = clientAcceptEncodingHeader.find(candidate);
        checkCandidate(candidate, position);
    }

    if (!bestEncoding) {
        return TError("Could not determine feasible content encoding given accept encoding constraints")
            << TErrorAttribute("client_accept_encoding", clientAcceptEncodingHeader);
    }

    return *bestEncoding;
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

} // namespace NYT::NHttp
