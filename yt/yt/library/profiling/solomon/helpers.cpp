#include "helpers.h"
#include "percpu.h"
#include "private.h"
#include "producer.h"
#include "sensor_set.h"

#include <yt/yt/core/http/http.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/prometheus/prometheus.h>

namespace NYT::NProfiling {

using namespace NHttp;

////////////////////////////////////////////////////////////////////////////////

void FillResponseHeaders(const TOutputEncodingContext& outputEncodingContext, const THeadersPtr& headers)
{
    headers->Set("Content-Type", std::string{::NMonitoring::ContentTypeByFormat(outputEncodingContext.Format)});
    headers->Set("Content-Encoding", std::string{::NMonitoring::ContentEncodingByCompression(outputEncodingContext.Compression)});
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

i64 GetCountersBytesAlive()
{
    auto* tracker = TRefCountedTracker::Get();
    i64 usage = 0;

    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TSimpleCounter>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TPerCpuCounter>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TCounterState>());

    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TSimpleTimeCounter>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TPerCpuTimeCounter>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TTimeCounterState>());

    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TSimpleGauge>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TPerCpuGauge>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TGaugeState>());

    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TSimpleSummary<double>>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TPerCpuSummary<double>>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TSimpleSummary<TDuration>>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TPerCpuSummary<TDuration>>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TSummaryState>());

    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TProducerState>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<THistogram>());

    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<TTimerSummaryState>());
    usage += tracker->GetBytesAlive(GetRefCountedTypeKey<THistogramState>());

    return usage;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
