// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/sdk/configuration/registry.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

void RegisterDefaultTraceBuilders(Registry *registry);

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
