#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>

#include <opentelemetry/version.h>
#include <opentelemetry/nostd/shared_ptr.h>

OPENTELEMETRY_BEGIN_NAMESPACE
namespace metrics {
class MeterProvider;
}
OPENTELEMETRY_END_NAMESPACE

namespace NYdb::inline Dev::NMetrics {

std::shared_ptr<IMetricRegistry> CreateOtelMetricRegistry(
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::MeterProvider> meterProvider);

} // namespace NYdb::NMetrics
