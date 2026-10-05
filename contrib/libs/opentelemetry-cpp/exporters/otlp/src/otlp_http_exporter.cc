// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "opentelemetry/exporters/otlp/otlp_http_client.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_options.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_runtime_options.h"
#include "opentelemetry/exporters/otlp/otlp_recordable.h"
#include "opentelemetry/exporters/otlp/otlp_recordable_utils.h"
#include "opentelemetry/ext/http/client/http_client.h"
#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/common/exporter_utils.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/common/thread_instrumentation.h"
#include "opentelemetry/version.h"

// clang-format off
#include "opentelemetry/exporters/otlp/protobuf_include_prefix.h" // IWYU pragma: keep
#include "google/protobuf/arena.h"
#include "opentelemetry/proto/collector/trace/v1/trace_service.pb.h"
#include "opentelemetry/exporters/otlp/protobuf_include_suffix.h" // IWYU pragma: keep
// clang-format on

namespace google
{
namespace protobuf
{
class Message;
}  // namespace protobuf
}  // namespace google

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

OtlpHttpExporter::OtlpHttpExporter() : OtlpHttpExporter(OtlpHttpExporterOptions()) {}

OtlpHttpExporter::OtlpHttpExporter(const OtlpHttpExporterOptions &options)
    : options_(options),
      runtime_options_(),
      http_client_(std::make_unique<OtlpHttpClient>(OtlpHttpClientOptions(
          options.url,
          options.ssl_insecure_skip_verify,
          options.ssl_ca_cert_path,
          options.ssl_ca_cert_string,
          options.ssl_client_key_path,
          options.ssl_client_key_string,
          options.ssl_client_cert_path,
          options.ssl_client_cert_string,
          options.ssl_min_tls,
          options.ssl_max_tls,
          options.ssl_cipher,
          options.ssl_cipher_suite,
          options.content_type,
          options.json_bytes_mapping,
          options.compression,
          options.use_json_name,
          options.console_debug,
          options.timeout,
          options.http_headers,
          options.retry_policy_max_attempts,
          options.retry_policy_initial_backoff,
          options.retry_policy_max_backoff,
          options.retry_policy_backoff_multiplier,
          std::shared_ptr<sdk::common::ThreadInstrumentation>{nullptr}
#ifdef ENABLE_ASYNC_EXPORT
          ,
          options.max_concurrent_requests,
          options.max_requests_per_connection
#endif
          )))
{}

OtlpHttpExporter::OtlpHttpExporter(const OtlpHttpExporterOptions &options,
                                   const OtlpHttpExporterRuntimeOptions &runtime_options)
    : options_(options),
      runtime_options_(runtime_options),
      http_client_(std::make_unique<OtlpHttpClient>(OtlpHttpClientOptions(
          options.url,
          options.ssl_insecure_skip_verify,
          options.ssl_ca_cert_path,
          options.ssl_ca_cert_string,
          options.ssl_client_key_path,
          options.ssl_client_key_string,
          options.ssl_client_cert_path,
          options.ssl_client_cert_string,
          options.ssl_min_tls,
          options.ssl_max_tls,
          options.ssl_cipher,
          options.ssl_cipher_suite,
          options.content_type,
          options.json_bytes_mapping,
          options.compression,
          options.use_json_name,
          options.console_debug,
          options.timeout,
          options.http_headers,
          options.retry_policy_max_attempts,
          options.retry_policy_initial_backoff,
          options.retry_policy_max_backoff,
          options.retry_policy_backoff_multiplier,
          runtime_options.thread_instrumentation
#ifdef ENABLE_ASYNC_EXPORT
          ,
          options.max_concurrent_requests,
          options.max_requests_per_connection
#endif
          )))
{}

OtlpHttpExporter::OtlpHttpExporter(
    const OtlpHttpExporterOptions &options,
    const std::shared_ptr<ext::http::client::HttpClientFactory> &factory)
    : options_(options),
      runtime_options_(),
      http_client_(std::make_unique<OtlpHttpClient>(
          OtlpHttpClientOptions(options.url,
                                options.ssl_insecure_skip_verify,
                                options.ssl_ca_cert_path,
                                options.ssl_ca_cert_string,
                                options.ssl_client_key_path,
                                options.ssl_client_key_string,
                                options.ssl_client_cert_path,
                                options.ssl_client_cert_string,
                                options.ssl_min_tls,
                                options.ssl_max_tls,
                                options.ssl_cipher,
                                options.ssl_cipher_suite,
                                options.content_type,
                                options.json_bytes_mapping,
                                options.compression,
                                options.use_json_name,
                                options.console_debug,
                                options.timeout,
                                options.http_headers,
                                options.retry_policy_max_attempts,
                                options.retry_policy_initial_backoff,
                                options.retry_policy_max_backoff,
                                options.retry_policy_backoff_multiplier,
                                std::shared_ptr<sdk::common::ThreadInstrumentation>{nullptr}
#ifdef ENABLE_ASYNC_EXPORT
                                ,
                                options.max_concurrent_requests,
                                options.max_requests_per_connection
#endif
                                ),
          factory))
{}

OtlpHttpExporter::OtlpHttpExporter(
    const OtlpHttpExporterOptions &options,
    const OtlpHttpExporterRuntimeOptions &runtime_options,
    const std::shared_ptr<ext::http::client::HttpClientFactory> &factory)
    : options_(options),
      runtime_options_(runtime_options),
      http_client_(std::make_unique<OtlpHttpClient>(
          OtlpHttpClientOptions(options.url,
                                options.ssl_insecure_skip_verify,
                                options.ssl_ca_cert_path,
                                options.ssl_ca_cert_string,
                                options.ssl_client_key_path,
                                options.ssl_client_key_string,
                                options.ssl_client_cert_path,
                                options.ssl_client_cert_string,
                                options.ssl_min_tls,
                                options.ssl_max_tls,
                                options.ssl_cipher,
                                options.ssl_cipher_suite,
                                options.content_type,
                                options.json_bytes_mapping,
                                options.compression,
                                options.use_json_name,
                                options.console_debug,
                                options.timeout,
                                options.http_headers,
                                options.retry_policy_max_attempts,
                                options.retry_policy_initial_backoff,
                                options.retry_policy_max_backoff,
                                options.retry_policy_backoff_multiplier,
                                runtime_options.thread_instrumentation
#ifdef ENABLE_ASYNC_EXPORT
                                ,
                                options.max_concurrent_requests,
                                options.max_requests_per_connection
#endif
                                ),
          factory))
{}

OtlpHttpExporter::OtlpHttpExporter(const OtlpHttpExporterOptions &options,
                                   std::shared_ptr<ext::http::client::HttpClient> http_client)
    : options_(options),
      runtime_options_(),
      http_client_(std::make_unique<OtlpHttpClient>(
          OtlpHttpClientOptions(options.url,
                                options.ssl_insecure_skip_verify,
                                options.ssl_ca_cert_path,
                                options.ssl_ca_cert_string,
                                options.ssl_client_key_path,
                                options.ssl_client_key_string,
                                options.ssl_client_cert_path,
                                options.ssl_client_cert_string,
                                options.ssl_min_tls,
                                options.ssl_max_tls,
                                options.ssl_cipher,
                                options.ssl_cipher_suite,
                                options.content_type,
                                options.json_bytes_mapping,
                                options.compression,
                                options.use_json_name,
                                options.console_debug,
                                options.timeout,
                                options.http_headers,
                                options.retry_policy_max_attempts,
                                options.retry_policy_initial_backoff,
                                options.retry_policy_max_backoff,
                                options.retry_policy_backoff_multiplier,
                                std::shared_ptr<sdk::common::ThreadInstrumentation>{nullptr}
#ifdef ENABLE_ASYNC_EXPORT
                                ,
                                options.max_concurrent_requests,
                                options.max_requests_per_connection
#endif
                                ),
          std::move(http_client)))
{}

OtlpHttpExporter::OtlpHttpExporter(const OtlpHttpExporterOptions &options,
                                   const OtlpHttpExporterRuntimeOptions &runtime_options,
                                   std::shared_ptr<ext::http::client::HttpClient> http_client)
    : options_(options),
      runtime_options_(runtime_options),
      http_client_(std::make_unique<OtlpHttpClient>(
          OtlpHttpClientOptions(options.url,
                                options.ssl_insecure_skip_verify,
                                options.ssl_ca_cert_path,
                                options.ssl_ca_cert_string,
                                options.ssl_client_key_path,
                                options.ssl_client_key_string,
                                options.ssl_client_cert_path,
                                options.ssl_client_cert_string,
                                options.ssl_min_tls,
                                options.ssl_max_tls,
                                options.ssl_cipher,
                                options.ssl_cipher_suite,
                                options.content_type,
                                options.json_bytes_mapping,
                                options.compression,
                                options.use_json_name,
                                options.console_debug,
                                options.timeout,
                                options.http_headers,
                                options.retry_policy_max_attempts,
                                options.retry_policy_initial_backoff,
                                options.retry_policy_max_backoff,
                                options.retry_policy_backoff_multiplier,
                                runtime_options.thread_instrumentation
#ifdef ENABLE_ASYNC_EXPORT
                                ,
                                options.max_concurrent_requests,
                                options.max_requests_per_connection
#endif
                                ),
          std::move(http_client)))
{}

OtlpHttpExporter::OtlpHttpExporter(std::unique_ptr<OtlpHttpClient> http_client)
    : options_(OtlpHttpExporterOptions()), http_client_(std::move(http_client))
{
  options_.url                          = http_client_->GetOptions().url;
  options_.content_type                 = http_client_->GetOptions().content_type;
  options_.json_bytes_mapping           = http_client_->GetOptions().json_bytes_mapping;
  options_.use_json_name                = http_client_->GetOptions().use_json_name;
  options_.console_debug                = http_client_->GetOptions().console_debug;
  options_.timeout                      = http_client_->GetOptions().timeout;
  options_.http_headers                 = http_client_->GetOptions().http_headers;
  options_.retry_policy_max_attempts    = http_client_->GetOptions().retry_policy.max_attempts;
  options_.retry_policy_initial_backoff = http_client_->GetOptions().retry_policy.initial_backoff;
  options_.retry_policy_max_backoff     = http_client_->GetOptions().retry_policy.max_backoff;
  options_.retry_policy_backoff_multiplier =
      http_client_->GetOptions().retry_policy.backoff_multiplier;
#ifdef ENABLE_ASYNC_EXPORT
  options_.max_concurrent_requests     = http_client_->GetOptions().max_concurrent_requests;
  options_.max_requests_per_connection = http_client_->GetOptions().max_requests_per_connection;
#endif
  runtime_options_.thread_instrumentation = http_client_->GetOptions().thread_instrumentation;
}
// ----------------------------- Exporter methods ------------------------------

std::unique_ptr<opentelemetry::sdk::trace::Recordable> OtlpHttpExporter::MakeRecordable() noexcept
{
  return std::make_unique<exporter::otlp::OtlpRecordable>(
      options_.max_attributes, options_.max_events, options_.max_links,
      options_.max_attributes_per_event, options_.max_attributes_per_link);
}

opentelemetry::sdk::common::ExportResult OtlpHttpExporter::Export(
    const opentelemetry::nostd::span<std::unique_ptr<opentelemetry::sdk::trace::Recordable>>
        &spans) noexcept
{
  if (http_client_->IsShutdown())
  {
    std::size_t span_count = spans.size();
    OTEL_INTERNAL_LOG_ERROR("[OTLP TRACE HTTP Exporter] ERROR: Export "
                            << span_count << " trace span(s) failed, exporter is shutdown");
    return opentelemetry::sdk::common::ExportResult::kFailure;
  }

  if (spans.empty())
  {
    return opentelemetry::sdk::common::ExportResult::kSuccess;
  }

  google::protobuf::ArenaOptions arena_options;
  // It's easy to allocate datas larger than 1024 when we populate basic resource and attributes
  arena_options.initial_block_size = 1024;
  // When in batch mode, it's easy to export a large number of spans at once, we can alloc a lager
  // block to reduce memory fragments.
  arena_options.max_block_size = 65536;
  // Ownership transfers into HttpSessionData until the request completes
  auto arena = std::make_unique<google::protobuf::Arena>(arena_options);

  proto::collector::trace::v1::ExportTraceServiceRequest *service_request =
      google::protobuf::Arena::Create<proto::collector::trace::v1::ExportTraceServiceRequest>(
          arena.get());
  OtlpRecordableUtils::PopulateRequest(spans, service_request);
  std::size_t span_count = spans.size();

  proto::collector::trace::v1::ExportTraceServiceResponse *response =
      google::protobuf::Arena::Create<proto::collector::trace::v1::ExportTraceServiceResponse>(
          arena.get());

  auto handle_result = [span_count](opentelemetry::sdk::common::ExportResult result,
                                    google::protobuf::Message *response_msg) {
    if (result != opentelemetry::sdk::common::ExportResult::kSuccess)
    {
      OTEL_INTERNAL_LOG_ERROR("[OTLP TRACE HTTP Exporter] ERROR: Export "
                              << span_count
                              << " trace span(s) error: " << static_cast<int>(result));
      return true;
    }
    auto *response =
        static_cast<proto::collector::trace::v1::ExportTraceServiceResponse *>(response_msg);
    if (response->has_partial_success() && (response->partial_success().rejected_spans() != 0 ||
                                            !response->partial_success().error_message().empty()))
    {
      const auto &partial = response->partial_success();
      OTEL_INTERNAL_LOG_ERROR("[OTLP TRACE HTTP Exporter] Export partial success: "
                              << partial.rejected_spans() << " span(s) rejected: \""
                              << partial.error_message() << "\"");
    }
    else
    {
      OTEL_INTERNAL_LOG_DEBUG("[OTLP TRACE HTTP Exporter] Export " << span_count
                                                                   << " trace span(s) success");
    }
    return true;
  };

#ifdef ENABLE_ASYNC_EXPORT
  http_client_->Export(*service_request, std::move(arena), response, std::move(handle_result),
                       options_.max_concurrent_requests);
  return opentelemetry::sdk::common::ExportResult::kSuccess;
#else
  return http_client_->Export(*service_request, std::move(arena), response,
                              std::move(handle_result), 0);
#endif
}

bool OtlpHttpExporter::ForceFlush(std::chrono::microseconds timeout) noexcept
{
  return http_client_->ForceFlush(timeout);
}

bool OtlpHttpExporter::Shutdown(std::chrono::microseconds timeout) noexcept
{
  return http_client_->Shutdown(timeout);
}

}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
