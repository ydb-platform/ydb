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
namespace db
{

/**
  This event represents an exception that occurred during a database client operation, such as
  connection failures, query errors, timeouts, or other errors that prevent the operation from
  completing successfully. <p> This event SHOULD be recorded when an exception occurs during
  database client operations. Instrumentations SHOULD set the severity to WARN (severity number 13)
  when recording this event. Instrumentations MAY provide a configuration option to populate
  exception events with the attributes captured on the corresponding database client span.
 */
static constexpr const char *kDbClientOperationException = "db.client.operation.exception";

}  // namespace db
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
