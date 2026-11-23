// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace resource
{
class Resource;
}  // namespace resource

namespace configuration
{

class Registry;

/**
 * Configuration context for building a meter provider.
 */
struct MeterProviderBuilderContext
{
  /** Registry is required and must not be null. */
  const Registry *registry{nullptr};

  /** Resource is required and must not be null. */
  const opentelemetry::sdk::resource::Resource *resource{nullptr};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
