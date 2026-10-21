// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

class ResourceDetectorConfigurationVisitor;

// YAML-SCHEMA: schema/resource.json
// YAML-NODE: ExperimentalResourceDetector
class ResourceDetectorConfiguration
{
public:
  ResourceDetectorConfiguration()                                                      = default;
  ResourceDetectorConfiguration(ResourceDetectorConfiguration &&)                      = default;
  ResourceDetectorConfiguration(const ResourceDetectorConfiguration &)                 = default;
  ResourceDetectorConfiguration &operator=(ResourceDetectorConfiguration &&)           = default;
  ResourceDetectorConfiguration &operator=(const ResourceDetectorConfiguration &other) = default;
  virtual ~ResourceDetectorConfiguration()                                             = default;

  virtual void Accept(ResourceDetectorConfigurationVisitor *visitor) const = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
