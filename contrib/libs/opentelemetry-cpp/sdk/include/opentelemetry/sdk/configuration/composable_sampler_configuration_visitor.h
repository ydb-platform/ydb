// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

class ComposableAlwaysOffSamplerConfiguration;
class ComposableAlwaysOnSamplerConfiguration;
class ComposableProbabilitySamplerConfiguration;
class ComposableParentThresholdSamplerConfiguration;
class ComposableRuleBasedSamplerConfiguration;
class ExtensionComposableSamplerConfiguration;

class ComposableSamplerConfigurationVisitor
{
public:
  ComposableSamplerConfigurationVisitor()                                              = default;
  ComposableSamplerConfigurationVisitor(ComposableSamplerConfigurationVisitor &&)      = default;
  ComposableSamplerConfigurationVisitor(const ComposableSamplerConfigurationVisitor &) = default;
  ComposableSamplerConfigurationVisitor &operator=(ComposableSamplerConfigurationVisitor &&) =
      default;
  ComposableSamplerConfigurationVisitor &operator=(const ComposableSamplerConfigurationVisitor &) =
      default;
  virtual ~ComposableSamplerConfigurationVisitor() = default;

  virtual void VisitComposableAlwaysOff(const ComposableAlwaysOffSamplerConfiguration *model) = 0;
  virtual void VisitComposableAlwaysOn(const ComposableAlwaysOnSamplerConfiguration *model)   = 0;
  virtual void VisitComposableProbability(
      const ComposableProbabilitySamplerConfiguration *model) = 0;
  virtual void VisitComposableParentThreshold(
      const ComposableParentThresholdSamplerConfiguration *model)                             = 0;
  virtual void VisitComposableRuleBased(const ComposableRuleBasedSamplerConfiguration *model) = 0;
  virtual void VisitComposableExtension(const ExtensionComposableSamplerConfiguration *model) = 0;
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
