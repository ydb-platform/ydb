#include "compression.h"

#include <yt/yt/core/ytree/serialize.h>

#include <library/cpp/streams/brotli/brotli.h>

#include <library/cpp/blockcodecs/codecs.h>
#include <library/cpp/blockcodecs/stream.h>

#include <util/stream/zlib.h>

namespace NYT::NHttp {

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
    Refs_.push_back(TSharedRef::MakeCopy<TDefaultSharedBlobTag>(buffer));
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

const std::vector<TSharedRef>& TSharedRefOutputStream::GetRefs() const
{
    return Refs_;
}

////////////////////////////////////////////////////////////////////////////////

class TCompressingOutputStream
    : public IFlushableAsyncOutputStream
    , private IOutputStream
{
public:
    TCompressingOutputStream(
        IAsyncOutputStreamPtr underlying,
        TContentEncoding contentEncoding)
        : Underlying_(underlying)
        , ContentEncoding_(contentEncoding)
    { }

    ~TCompressingOutputStream()
    {
        Destroying_ = true;
        Compressor_.reset();
    }

    TFuture<void> Write(const TSharedRef& buffer) override
    {
        Holder_ = buffer;

        CreateCompressor();

        if (Finished_) {
            THROW_ERROR_EXCEPTION("Attempting write to closed compression stream");
        }

        Compressor_->Write(buffer.Begin(), buffer.Size());
        return VoidFuture;
    }

    TFuture<void> Flush() override
    {
        CreateCompressor();
        Compressor_->Flush();
        return VoidFuture;
    }

    TFuture<void> Close() override
    {
        if (!Finished_) {
            Finished_ = true;
            CreateCompressor();
            Compressor_->Finish();
        }
        return VoidFuture;
    }

private:
    const IAsyncOutputStreamPtr Underlying_;
    const TContentEncoding ContentEncoding_;

    // NB: Arcadia streams got some "interesting" ideas about
    // exception handling and the role of destructors in the C++
    // programming language.
    TSharedRef Holder_;
    bool Destroying_ = false;
    bool Finished_ = false;
    std::unique_ptr<IOutputStream> Compressor_;

    void CreateCompressor()
    {
        if (Compressor_) {
            return;
        }

        if (ContentEncoding_.StartsWith("z-")) {
            Compressor_.reset(new NBlockCodecs::TCodedOutput(
                this,
                NBlockCodecs::Codec(ContentEncoding_.substr(2)),
                DefaultStreamBufferSize));
            return;
        }

        if (ContentEncoding_ == "gzip") {
            Compressor_.reset(new TZLibCompress(this, ZLib::GZip, 4, DefaultStreamBufferSize));
            return;
        }

        if (ContentEncoding_ == "deflate") {
            Compressor_.reset(new TZLibCompress(this, ZLib::ZLib, 4, DefaultStreamBufferSize));
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

    void DoWrite(const void* buf, size_t len) override
    {
        if (Destroying_) {
            return;
        }

        WaitFor(Underlying_->Write(TSharedRef(buf, len, New<TStreamHolder>(this))))
            .ThrowOnError();
    }

    void DoFlush() override
    { }

    void DoFinish() override
    {
        if (Destroying_) {
            return;
        }

        WaitFor(Underlying_->Close())
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
        TContentEncoding contentEncoding)
        : Underlying_(underlying)
        , ContentEncoding_(contentEncoding)
    { }

    TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        CreateDecompressorIfNeeded();
        return MakeFuture<size_t>(Decompressor_->Read(buffer.Begin(), buffer.Size()));
    }

private:
    const IAsyncZeroCopyInputStreamPtr Underlying_;
    const TContentEncoding ContentEncoding_;

    std::unique_ptr<IInputStream> Decompressor_;

    TSharedRef LastRead_;
    bool IsEnd_ = false;

    size_t DoRead(void* buf, size_t len) override
    {
        if (IsEnd_) {
            return 0;
        }

        if (LastRead_.Empty()) {
            LastRead_ = WaitFor(Underlying_->Read())
                .ValueOrThrow();
            IsEnd_ = LastRead_.Empty();
        }

        size_t readSize = std::min(len, LastRead_.Size());
        std::copy(LastRead_.Begin(), LastRead_.Begin() + readSize, reinterpret_cast<char*>(buf));
        if (readSize != LastRead_.Size()) {
            LastRead_ = LastRead_.Slice(readSize, LastRead_.Size());
        } else {
            LastRead_ = {};
        }
        return readSize;
    }

    void CreateDecompressorIfNeeded()
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
            Decompressor_.reset(new TZLibDecompress(this));
            return;
        }

        if (ContentEncoding_ == "br") {
            Decompressor_.reset(new TBrotliDecompress(this));
            return;
        }

        if (Decompressor_ = TryDetectOptionalDecompressors(ContentEncoding_, this)) {
            return;
        }

        THROW_ERROR_EXCEPTION("Unsupported content encoding")
            << TErrorAttribute("content_encoding", ContentEncoding_);
    }
};

DEFINE_REFCOUNTED_TYPE(TDecompressingInputStream)

////////////////////////////////////////////////////////////////////////////////

const TContentEncoding IdentityContentEncoding = "identity";

bool IsCompressionSupported(const TContentEncoding& contentEncoding)
{
    if (contentEncoding.StartsWith("z-")) {
        try {
            NBlockCodecs::Codec(contentEncoding.substr(2));
            return true;
        } catch (const NBlockCodecs::TNotFound&) {
            return false;
        }
    }

    for (const auto& supported : SupportedCompressions) {
        if (supported == contentEncoding) {
            return true;
        }
    }

    return false;
}

std::vector<TContentEncoding> GetSupportedCompressions()
{
    auto result = SupportedCompressions;
    for (auto blockCodec : NBlockCodecs::ListAllCodecs()) {
        result.push_back(TString("z-") + blockCodec);
    }
    return result;
}

// NOTE: Does not implement the spec, but a reasonable approximation.
TErrorOr<TContentEncoding> GetBestAcceptedEncoding(const TString& clientAcceptEncodingHeader)
{
    auto bestPosition = TString::npos;
    TContentEncoding bestEncoding;

    auto checkCandidate = [&] (const TString& candidate, size_t position) {
        if (position != TString::npos && (bestPosition == TString::npos || position < bestPosition)) {
            bestEncoding = candidate;
            bestPosition = position;
        }
    };

    for (const auto& candidate : SupportedCompressions) {
        if (candidate == "x-lzop") {
            continue;
        }

        auto position = clientAcceptEncodingHeader.find(candidate);
        checkCandidate(candidate, position);
    }

    for (const auto& blockcodec : NBlockCodecs::ListAllCodecs()) {
        auto candidate = TString{"z-"} + blockcodec;

        auto position = clientAcceptEncodingHeader.find(candidate);
        checkCandidate(candidate, position);
    }

    if (!bestEncoding.empty()) {
        return bestEncoding;
    }

    return TError("Could not determine feasible Content-Encoding given Accept-Encoding constraints")
        << TErrorAttribute("client_accept_encoding", clientAcceptEncodingHeader);
}

IFlushableAsyncOutputStreamPtr CreateCompressingAdapter(
    IAsyncOutputStreamPtr underlying,
    TContentEncoding contentEncoding)
{
    return New<TCompressingOutputStream>(underlying, contentEncoding);
}

IAsyncInputStreamPtr CreateDecompressingAdapter(
    IAsyncZeroCopyInputStreamPtr underlying,
    TContentEncoding contentEncoding)
{
    return New<TDecompressingInputStream>(underlying, contentEncoding);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
