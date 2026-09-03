// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/sdk/configuration/resource_detector_configuration.h"
#include "opentelemetry/sdk/configuration/resource_detector_configuration_visitor.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/resource.json
// YAML-NODE: service
class ServiceResourceDetectorConfiguration : public ResourceDetectorConfiguration
{
public:
  void Accept(ResourceDetectorConfigurationVisitor *visitor) const override
  {
    visitor->VisitService(this);
  }
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
