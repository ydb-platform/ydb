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
namespace azure
{

/**
  Describes Azure Resource Log event, see <a
  href="https://learn.microsoft.com/azure/azure-monitor/essentials/resource-logs-schema#top-level-common-schema">Azure
  Resource Log Top-level Schema</a> for more details.
 */
static constexpr const char *kAzureResourceLog = "azure.resource.log";

}  // namespace azure
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
