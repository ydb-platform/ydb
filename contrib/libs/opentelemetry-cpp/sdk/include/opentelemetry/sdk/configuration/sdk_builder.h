// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <string>

#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/configuration/attribute_value_configuration.h"
#include "opentelemetry/sdk/configuration/configuration.h"
#include "opentelemetry/sdk/configuration/configured_sdk.h"
#include "opentelemetry/sdk/configuration/container_resource_detector_configuration.h"
#include "opentelemetry/sdk/configuration/extension_resource_detector_configuration.h"
#include "opentelemetry/sdk/configuration/host_resource_detector_configuration.h"
#include "opentelemetry/sdk/configuration/process_resource_detector_configuration.h"
#include "opentelemetry/sdk/configuration/propagator_configuration.h"
#include "opentelemetry/sdk/configuration/registry.h"
#include "opentelemetry/sdk/configuration/resource_configuration.h"
#include "opentelemetry/sdk/configuration/resource_detection_configuration.h"
#include "opentelemetry/sdk/configuration/resource_detector_configuration.h"
#include "opentelemetry/sdk/configuration/service_resource_detector_configuration.h"
#include "opentelemetry/sdk/configuration/severity_number.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk/resource/resource_detector.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

class SdkBuilder
{
public:
  explicit SdkBuilder(std::shared_ptr<Registry> registry);

  std::unique_ptr<opentelemetry::context::propagation::TextMapPropagator> CreateTextMapPropagator(
      const std::string &name) const;

  std::unique_ptr<opentelemetry::context::propagation::TextMapPropagator> CreatePropagator(
      const std::unique_ptr<opentelemetry::sdk::configuration::PropagatorConfiguration> &model)
      const;

  std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> CreateContainerResourceDetector(
      const opentelemetry::sdk::configuration::ContainerResourceDetectorConfiguration *model) const;

  std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> CreateHostResourceDetector(
      const opentelemetry::sdk::configuration::HostResourceDetectorConfiguration *model) const;

  std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> CreateProcessResourceDetector(
      const opentelemetry::sdk::configuration::ProcessResourceDetectorConfiguration *model) const;

  std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> CreateServiceResourceDetector(
      const opentelemetry::sdk::configuration::ServiceResourceDetectorConfiguration *model) const;

  std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> CreateExtensionResourceDetector(
      const opentelemetry::sdk::configuration::ExtensionResourceDetectorConfiguration *model) const;

  std::unique_ptr<opentelemetry::sdk::resource::ResourceDetector> CreateResourceDetector(
      const std::unique_ptr<opentelemetry::sdk::configuration::ResourceDetectorConfiguration>
          &model) const;

  opentelemetry::sdk::resource::Resource CreateDetectedResource(
      const std::unique_ptr<opentelemetry::sdk::configuration::ResourceDetectionConfiguration>
          &model) const;

  void SetResourceAttribute(
      opentelemetry::sdk::resource::ResourceAttributes &resource_attributes,
      const std::string &name,
      const opentelemetry::sdk::configuration::AttributeValueConfiguration *model) const;

  void SetResource(opentelemetry::sdk::resource::Resource &resource,
                   const std::unique_ptr<opentelemetry::sdk::configuration::ResourceConfiguration>
                       &opt_model) const;

  void SetLogLevel(opentelemetry::sdk::common::internal_log::LogLevel &sdk_log_level,
                   opentelemetry::sdk::configuration::SeverityNumber model_log_level) const;

  std::unique_ptr<ConfiguredSdk> CreateConfiguredSdk(
      const std::unique_ptr<opentelemetry::sdk::configuration::Configuration> &model) const;

private:
  std::shared_ptr<Registry> registry_;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
