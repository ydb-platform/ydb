/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * DO NOT EDIT, this is an Auto-generated file from:
 * buildscripts/semantic-convention/templates/registry/semantic_events-h.j2
 */

#pragma once

#include "opentelemetry/common/macros.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace semconv
{
namespace http
{

/**
  This event represents an exception that occurred during an HTTP client request, such as network
  failures, timeouts, or other errors that prevent the request from completing successfully. <p>
  This event SHOULD be recorded when an exception occurs during HTTP client operations.
  Instrumentations SHOULD set the severity to WARN (severity number 13) when recording this event.
  Some HTTP client frameworks generate artificial exceptions for non-successful HTTP status codes
  (e.g., 404 Not Found). When possible, instrumentations SHOULD NOT record these artificial
  exceptions, or SHOULD set the severity to DEBUG (severity number 5). Instrumentations MAY provide
  a configuration option to populate exception events with the attributes captured on the
  corresponding HTTP client span.
 */
static constexpr const char *kHttpClientRequestException = "http.client.request.exception";

/**
  This event represents an exception that occurred during HTTP server request processing, such as
  application errors, internal failures, or other exceptions that prevent the server from
  successfully handling the request. <p> This event SHOULD be recorded when an exception occurs
  during HTTP server request processing. Instrumentations SHOULD set the severity to ERROR (severity
  number 17) when recording this event. Instrumentations MAY provide a configuration option to
  populate exception events with the attributes captured on the corresponding HTTP server span.
 */
static constexpr const char *kHttpServerRequestException = "http.server.request.exception";

}  // namespace http
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
