// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/container_resource_detector_configuration.h"
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

class ContainerResourceDetectorBuilder
{
public:
  ContainerResourceDetectorBuilder()                                               = default;
  ContainerResourceDetectorBuilder(ContainerResourceDetectorBuilder &&)            = default;
  ContainerResourceDetectorBuilder(const ContainerResourceDetectorBuilder &)       = default;
  ContainerResourceDetectorBuilder &operator=(ContainerResourceDetectorBuilder &&) = default;
  ContainerResourceDetectorBuilder &operator=(const ContainerResourceDetectorBuilder &other) =
      default;
  virtual ~ContainerResourceDetectorBuilder() = default;

  virtual std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> Build(
      const opentelemetry::sdk::configuration::ContainerResourceDetectorConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
