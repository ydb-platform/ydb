// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/ext/http/client/http_client_factory.h"
#ifdef ENABLE_HTTP_CLIENT_CURL
#  include "opentelemetry/ext/http/client/curl/http_client_factory_curl.h"
#else
#  include "opentelemetry/sdk/common/global_log_handler.h"
#endif

OPENTELEMETRY_BEGIN_NAMESPACE
namespace ext
{
namespace http
{
namespace client
{
namespace detail
{

// Internal helper — not public API. Returns the built-in curl factory when
// ENABLE_HTTP_CLIENT_CURL is defined; terminates with an error otherwise.
inline std::shared_ptr<HttpClientFactory> GetDefaultHttpClientFactory()
{
#ifdef ENABLE_HTTP_CLIENT_CURL
  static auto instance = std::make_shared<curl::HttpCurlClientFactory>();
  return instance;
#else
  OTEL_INTERNAL_LOG_ERROR(
      "No default HTTP client backend is compiled in. "
      "Use the HttpClientFactory or HttpClient constructor overloads, "
      "or enable curl support (OTELCPP_WITH_HTTP_CLIENT_CURL=ON).");
  std::terminate();
#endif
}

}  // namespace detail
}  // namespace client
}  // namespace http
}  // namespace ext
OPENTELEMETRY_END_NAMESPACE
