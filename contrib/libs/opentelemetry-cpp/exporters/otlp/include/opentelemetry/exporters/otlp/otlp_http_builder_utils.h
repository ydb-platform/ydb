// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/exporters/otlp/otlp_http.h"
#include "opentelemetry/sdk/configuration/otlp_http_encoding.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

class OtlpHttpBuilderUtils
{
public:
  static HttpRequestContentType ConvertOtlpHttpEncoding(
      opentelemetry::sdk::configuration::OtlpHttpEncoding model)
  {
    switch (model)
    {
      case opentelemetry::sdk::configuration::OtlpHttpEncoding::json:
        return exporter::otlp::HttpRequestContentType::kJson;
      case opentelemetry::sdk::configuration::OtlpHttpEncoding::protobuf:
      default:
        return exporter::otlp::HttpRequestContentType::kBinary;
    }
  }
};

}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
