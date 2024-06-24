#pragma once

#include <yt/yt/core/http/public.h>

#include <library/cpp/monlib/encode/format.h>
#include <library/cpp/monlib/encode/encoder.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct TOutputEncodingContext
{
    ::NMonitoring::EFormat Format = ::NMonitoring::EFormat::JSON;
    //! JSON and SPACK are considered solomon formats, unless this value is explicitly overriden in headers.
    bool IsSolomonPull = true;
    ::NMonitoring::ECompression Compression = ::NMonitoring::ECompression::IDENTITY;

    //! Buffer to be filled by encoder below.
    std::shared_ptr<TStringStream> EncoderBuffer;
    ::NMonitoring::IMetricEncoderPtr Encoder;
};

//! Fills content type related headers according to format and compression.
void FillResponseHeaders(const TOutputEncodingContext& outputEncodingContext, const NHttp::THeadersPtr& headers);

//! Creates output encoder according to request headers.
TOutputEncodingContext CreateOutputEncodingContextFromHeaders(const NHttp::THeadersPtr& headers);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
