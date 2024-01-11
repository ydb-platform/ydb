#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

class TSharedRefOutputStream
    : public NConcurrency::IFlushableAsyncOutputStream
{
public:
    TFuture<void> Write(const TSharedRef& buffer) override;
    TFuture<void> Flush() override;
    TFuture<void> Close() override;

    const std::vector<TSharedRef>& GetRefs() const;

private:
    std::vector<TSharedRef> Refs_;
};

DEFINE_REFCOUNTED_TYPE(TSharedRefOutputStream)

////////////////////////////////////////////////////////////////////////////////

bool IsCompressionSupported(const TContentEncoding& contentEncoding);

std::vector<TContentEncoding> GetSupportedCompressions();

extern const TContentEncoding IdentityContentEncoding;
extern const std::vector<TContentEncoding> SupportedCompressions;

TErrorOr<TContentEncoding> GetBestAcceptedEncoding(const TString& clientAcceptEncodingHeader);

NConcurrency::IFlushableAsyncOutputStreamPtr CreateCompressingAdapter(
    NConcurrency::IAsyncOutputStreamPtr underlying,
    TContentEncoding contentEncoding);

NConcurrency::IAsyncInputStreamPtr CreateDecompressingAdapter(
    NConcurrency::IAsyncZeroCopyInputStreamPtr underlying,
    TContentEncoding contentEncoding);

std::unique_ptr<IOutputStream> TryDetectOptionalCompressors(
    TContentEncoding contentEncoding,
    IOutputStream* inner);

std::unique_ptr<IInputStream> TryDetectOptionalDecompressors(
    TContentEncoding contentEncoding,
    IInputStream* inner);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
