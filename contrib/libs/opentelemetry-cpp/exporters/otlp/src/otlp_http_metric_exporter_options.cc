// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h"
#include "opentelemetry/exporters/otlp/otlp_environment.h"
#include "opentelemetry/exporters/otlp/otlp_http.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace exporter
{
namespace otlp
{

OtlpHttpMetricExporterOptions::OtlpHttpMetricExporterOptions()
    : url(GetOtlpDefaultMetricsEndpoint()),
      content_type(GetOtlpHttpProtocolFromString(GetOtlpDefaultHttpMetricsProtocol())),
      timeout(GetOtlpDefaultMetricsTimeout()),
      http_headers(GetOtlpDefaultMetricsHeaders()),
      ssl_ca_cert_path(GetOtlpDefaultMetricsSslCertificatePath()),
      ssl_ca_cert_string(GetOtlpDefaultMetricsSslCertificateString()),
      ssl_client_key_path(GetOtlpDefaultMetricsSslClientKeyPath()),
      ssl_client_key_string(GetOtlpDefaultMetricsSslClientKeyString()),
      ssl_client_cert_path(GetOtlpDefaultMetricsSslClientCertificatePath()),
      ssl_client_cert_string(GetOtlpDefaultMetricsSslClientCertificateString()),
      ssl_min_tls(GetOtlpDefaultMetricsSslTlsMinVersion()),
      ssl_max_tls(GetOtlpDefaultMetricsSslTlsMaxVersion()),
      ssl_cipher(GetOtlpDefaultMetricsSslTlsCipher()),
      ssl_cipher_suite(GetOtlpDefaultMetricsSslTlsCipherSuite()),
      compression(GetOtlpDefaultMetricsCompression()),
      retry_policy_max_attempts(GetOtlpDefaultMetricsRetryMaxAttempts()),
      retry_policy_initial_backoff(GetOtlpDefaultMetricsRetryInitialBackoff()),
      retry_policy_max_backoff(GetOtlpDefaultMetricsRetryMaxBackoff()),
      retry_policy_backoff_multiplier(GetOtlpDefaultMetricsRetryBackoffMultiplier())
{}

OtlpHttpMetricExporterOptions::OtlpHttpMetricExporterOptions(void *) {}

OtlpHttpMetricExporterOptions::~OtlpHttpMetricExporterOptions() {}

}  // namespace otlp
}  // namespace exporter
OPENTELEMETRY_END_NAMESPACE
