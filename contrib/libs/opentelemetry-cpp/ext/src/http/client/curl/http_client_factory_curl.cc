// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <memory>

#include "opentelemetry/ext/http/client/curl/http_client_curl.h"
#include "opentelemetry/ext/http/client/curl/http_client_factory_curl.h"

namespace http_client = opentelemetry::ext::http::client;

std::shared_ptr<http_client::HttpClient> http_client::curl::HttpCurlClientFactory::Create()
{
  return std::make_shared<http_client::curl::HttpClient>();
}

std::shared_ptr<http_client::HttpClient> http_client::curl::HttpCurlClientFactory::Create(
    const std::shared_ptr<opentelemetry::sdk::common::ThreadInstrumentation>
        &thread_instrumentation)
{
  return std::make_shared<http_client::curl::HttpClient>(thread_instrumentation);
}

std::shared_ptr<http_client::HttpClientSync> http_client::curl::HttpCurlClientFactory::CreateSync()
{
  return std::make_shared<http_client::curl::HttpClientSync>();
}
