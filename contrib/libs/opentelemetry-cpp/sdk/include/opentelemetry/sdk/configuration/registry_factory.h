// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/registry.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

class RegistryFactory
{
public:
  // Returns a Registry pre-populated with all default signal builders.
  static std::unique_ptr<Registry> Create();
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
