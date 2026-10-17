// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/host_resource_detector_configuration.h"
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

class HostResourceDetectorBuilder
{
public:
  HostResourceDetectorBuilder()                                                    = default;
  HostResourceDetectorBuilder(HostResourceDetectorBuilder &&)                      = default;
  HostResourceDetectorBuilder(const HostResourceDetectorBuilder &)                 = default;
  HostResourceDetectorBuilder &operator=(HostResourceDetectorBuilder &&)           = default;
  HostResourceDetectorBuilder &operator=(const HostResourceDetectorBuilder &other) = default;
  virtual ~HostResourceDetectorBuilder()                                           = default;

  virtual std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> Build(
      const opentelemetry::sdk::configuration::HostResourceDetectorConfiguration *model) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
