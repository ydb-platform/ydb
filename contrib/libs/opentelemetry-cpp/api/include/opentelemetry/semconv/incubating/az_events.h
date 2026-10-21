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
namespace az
{

/**
  Deprecated. Use @code azure.resource.log @endcode instead.

  @deprecated
  {"note": "Replaced by @code azure.resource.log @endcode.", "reason": "renamed", "renamed_to":
  "azure.resource.log"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kAzResourceLog = "az.resource.log";

}  // namespace az
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
