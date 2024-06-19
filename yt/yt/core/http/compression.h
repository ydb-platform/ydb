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

    std::vector<TSharedRef> Finish();

private:
    std::vector<TSharedRef> Parts_;
};

DEFINE_REFCOUNTED_TYPE(TSharedRefOutputStream)

////////////////////////////////////////////////////////////////////////////////

inline const TContentEncoding IdentityContentEncoding = "identity";
const std::vector<TContentEncoding>& GetSupportedContentEncodings();
bool IsContentEncodingSupported(const TContentEncoding& contentEncoding);
TErrorOr<TContentEncoding> GetBestAcceptedContentEncoding(const TString& clientAcceptEncodingHeader);

////////////////////////////////////////////////////////////////////////////////

NConcurrency::IFlushableAsyncOutputStreamPtr CreateCompressingAdapter(
    NConcurrency::IAsyncOutputStreamPtr underlying,
    TContentEncoding contentEncoding,
    IInvokerPtr compressionInvoker);
NConcurrency::IAsyncInputStreamPtr CreateDecompressingAdapter(
    NConcurrency::IAsyncZeroCopyInputStreamPtr underlying,
    TContentEncoding contentEncoding,
    IInvokerPtr compressionInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
