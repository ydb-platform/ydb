#include "helpers.h"
#include "private.h"

#include <yt/yt/core/http/http.h>

#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/prometheus/prometheus.h>

namespace NYT::NProfiling {

using namespace NHttp;

////////////////////////////////////////////////////////////////////////////////

void FillResponseHeaders(const TOutputEncodingContext& outputEncodingContext, const THeadersPtr& headers)
{
    headers->Set("Content-Type", TString{::NMonitoring::ContentTypeByFormat(outputEncodingContext.Format)});
    headers->Set("Content-Encoding", TString{::NMonitoring::ContentEncodingByCompression(outputEncodingContext.Compression)});
}

TOutputEncodingContext CreateOutputEncodingContextFromHeaders(const THeadersPtr& headers)
{
    TOutputEncodingContext context;

    if (auto accept = headers->Find("Accept")) {
        context.Format = ::NMonitoring::FormatFromAcceptHeader(*accept);
    }

    if (auto acceptEncoding = headers->Find("Accept-Encoding")) {
        context.Compression = ::NMonitoring::CompressionFromAcceptEncodingHeader(*acceptEncoding);
        if (context.Compression == ::NMonitoring::ECompression::UNKNOWN) {
            // Fallback to identity if we cannot recognize the requested encoding.
            context.Compression = ::NMonitoring::ECompression::IDENTITY;
        }
    }

    if (auto isSolomonPull = headers->Find(IsSolomonPullHeaderName)) {
        if (!TryFromString<bool>(*isSolomonPull, context.IsSolomonPull)) {
            THROW_ERROR_EXCEPTION("Invalid value of %Qv header", IsSolomonPullHeaderName)
                << TErrorAttribute("value", *isSolomonPull);
        }
    } else {
        context.IsSolomonPull = context.Format == ::NMonitoring::EFormat::JSON || context.Format == ::NMonitoring::EFormat::SPACK;
    }

    context.EncoderBuffer = std::make_shared<TStringStream>();

    switch (context.Format) {
        case ::NMonitoring::EFormat::UNKNOWN:
        case ::NMonitoring::EFormat::JSON:
            context.Encoder = ::NMonitoring::BufferedEncoderJson(context.EncoderBuffer.get());
            context.Format = ::NMonitoring::EFormat::JSON;
            context.Compression = ::NMonitoring::ECompression::IDENTITY;
            break;

        case ::NMonitoring::EFormat::SPACK:
            context.Encoder = ::NMonitoring::EncoderSpackV1(
                context.EncoderBuffer.get(),
                ::NMonitoring::ETimePrecision::SECONDS,
                context.Compression);
            break;

        case ::NMonitoring::EFormat::PROMETHEUS:
            context.Encoder = ::NMonitoring::EncoderPrometheus(context.EncoderBuffer.get());
            context.Compression = ::NMonitoring::ECompression::IDENTITY;
            break;

        default:
            THROW_ERROR_EXCEPTION("Unsupported format %Qv", ::NMonitoring::ContentTypeByFormat(context.Format));
    }

    return context;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
