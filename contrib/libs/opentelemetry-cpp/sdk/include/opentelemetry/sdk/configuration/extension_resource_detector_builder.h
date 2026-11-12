// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/extension_resource_detector_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace resource
{
class ResourceDetector;
}  // namespace resource

namespace configuration
{

class ExtensionResourceDetectorBuilder
{
public:
  ExtensionResourceDetectorBuilder()                                               = default;
  ExtensionResourceDetectorBuilder(ExtensionResourceDetectorBuilder &&)            = default;
  ExtensionResourceDetectorBuilder(const ExtensionResourceDetectorBuilder &)       = default;
  ExtensionResourceDetectorBuilder &operator=(ExtensionResourceDetectorBuilder &&) = default;
  ExtensionResourceDetectorBuilder &operator=(const ExtensionResourceDetectorBuilder &other) =
      default;
  virtual ~ExtensionResourceDetectorBuilder() = default;

  virtual std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> Build(
      const opentelemetry::sdk::configuration::ExtensionResourceDetectorConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
