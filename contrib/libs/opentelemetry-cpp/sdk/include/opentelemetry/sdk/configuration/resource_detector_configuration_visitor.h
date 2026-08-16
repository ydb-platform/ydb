// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

class ContainerResourceDetectorConfiguration;
class HostResourceDetectorConfiguration;
class ProcessResourceDetectorConfiguration;
class ServiceResourceDetectorConfiguration;
class ExtensionResourceDetectorConfiguration;

class ResourceDetectorConfigurationVisitor
{
public:
  ResourceDetectorConfigurationVisitor()                                             = default;
  ResourceDetectorConfigurationVisitor(ResourceDetectorConfigurationVisitor &&)      = default;
  ResourceDetectorConfigurationVisitor(const ResourceDetectorConfigurationVisitor &) = default;
  ResourceDetectorConfigurationVisitor &operator=(ResourceDetectorConfigurationVisitor &&) =
      default;
  ResourceDetectorConfigurationVisitor &operator=(
      const ResourceDetectorConfigurationVisitor &other) = default;
  virtual ~ResourceDetectorConfigurationVisitor()        = default;

  virtual void VisitContainer(const ContainerResourceDetectorConfiguration *model) = 0;
  virtual void VisitHost(const HostResourceDetectorConfiguration *model)           = 0;
  virtual void VisitProcess(const ProcessResourceDetectorConfiguration *model)     = 0;
  virtual void VisitService(const ServiceResourceDetectorConfiguration *model)     = 0;
  virtual void VisitExtension(const ExtensionResourceDetectorConfiguration *model) = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
