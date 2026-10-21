// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/ext/http/client/http_client.h"
#include "opentelemetry/sdk/common/thread_instrumentation.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace ext
{
namespace http
{
namespace client
{
class HttpClientFactory
{
public:
  HttpClientFactory()                                     = default;
  HttpClientFactory(const HttpClientFactory &)            = delete;
  HttpClientFactory(HttpClientFactory &&)                 = delete;
  HttpClientFactory &operator=(const HttpClientFactory &) = delete;
  HttpClientFactory &operator=(HttpClientFactory &&)      = delete;

  virtual ~HttpClientFactory() = default;

  virtual std::shared_ptr<HttpClientSync> CreateSync() = 0;

  virtual std::shared_ptr<HttpClient> Create() = 0;
  virtual std::shared_ptr<HttpClient> Create(
      const std::shared_ptr<sdk::common::ThreadInstrumentation> &thread_instrumentation) = 0;
};

}  // namespace client
}  // namespace http
}  // namespace ext
OPENTELEMETRY_END_NAMESPACE
