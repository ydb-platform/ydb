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
namespace faas
{

/**
  This event represents an exception that occurred during FaaS function invocation, such as
  application errors, internal failures, or other exceptions that prevent the function from
  completing successfully. <p> This event SHOULD be recorded when an exception occurs during FaaS
  function invocation. Instrumentations SHOULD set the severity to ERROR (severity number 17) when
  recording this event. Instrumentations MAY provide a configuration option to populate exception
  events with the attributes captured on the corresponding FaaS span.
 */
static constexpr const char *kFaasInvocationException = "faas.invocation.exception";

}  // namespace faas
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
