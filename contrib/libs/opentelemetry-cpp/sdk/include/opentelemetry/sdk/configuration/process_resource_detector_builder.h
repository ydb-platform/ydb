// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>

#include "opentelemetry/sdk/configuration/process_resource_detector_configuration.h"
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

class ProcessResourceDetectorBuilder
{
public:
  ProcessResourceDetectorBuilder()                                                       = default;
  ProcessResourceDetectorBuilder(ProcessResourceDetectorBuilder &&)                      = default;
  ProcessResourceDetectorBuilder(const ProcessResourceDetectorBuilder &)                 = default;
  ProcessResourceDetectorBuilder &operator=(ProcessResourceDetectorBuilder &&)           = default;
  ProcessResourceDetectorBuilder &operator=(const ProcessResourceDetectorBuilder &other) = default;
  virtual ~ProcessResourceDetectorBuilder()                                              = default;

  virtual std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> Build(
      const opentelemetry::sdk::configuration::ProcessResourceDetectorConfiguration *model)
      const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
