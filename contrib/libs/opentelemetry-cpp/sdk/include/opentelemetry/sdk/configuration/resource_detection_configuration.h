// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <vector>

#include "opentelemetry/sdk/configuration/include_exclude_configuration.h"
#include "opentelemetry/sdk/configuration/resource_detector_configuration.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/resource.json
// YAML-NODE: ExperimentalResourceDetection
class ResourceDetectionConfiguration
{
public:
  std::unique_ptr<IncludeExcludeConfiguration> attributes;
  std::vector<std::unique_ptr<ResourceDetectorConfiguration>> detectors;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
