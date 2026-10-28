// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/service_resource_detector_configuration.h"
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

class ServiceResourceDetectorBuilder
{
public:
  ServiceResourceDetectorBuilder()                                                       = default;
  ServiceResourceDetectorBuilder(ServiceResourceDetectorBuilder &&)                      = default;
  ServiceResourceDetectorBuilder(const ServiceResourceDetectorBuilder &)                 = default;
  ServiceResourceDetectorBuilder &operator=(ServiceResourceDetectorBuilder &&)           = default;
  ServiceResourceDetectorBuilder &operator=(const ServiceResourceDetectorBuilder &other) = default;
  virtual ~ServiceResourceDetectorBuilder()                                              = default;

  virtual std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> Build(
      const opentelemetry::sdk::configuration::ServiceResourceDetectorConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
