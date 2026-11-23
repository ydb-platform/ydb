// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>

#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/configuration/grpc_tls_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

class OtlpGrpcBuilderUtils
{
public:
  static bool GrpcUseSsl(const std::string &endpoint,
                         const opentelemetry::sdk::configuration::GrpcTlsConfiguration *tls)
  {
    if (endpoint.substr(0, 6) == "https:")
    {
      return true;
    }

    if (endpoint.substr(0, 5) == "http:")
    {
      if (tls && !tls->insecure)
      {
        OTEL_INTERNAL_LOG_WARN(
            "[Otlp Grpc Exporter] endpoint is http but tls.insecure is false: using insecure "
            "connection");
      }
      return false;
    }

    if (tls != nullptr)
    {
      return !tls->insecure;
    }

    OTEL_INTERNAL_LOG_DEBUG(
        "[Otlp Grpc Exporter] endpoint does not specify http or https and tls is not configured. "
        "Using secure connection by default. To use an insecure connection, set tls.insecure to "
        "true.");
    return true;
  }
};

}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
