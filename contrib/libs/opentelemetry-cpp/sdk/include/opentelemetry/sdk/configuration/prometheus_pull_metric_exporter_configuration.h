// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstddef>
#include <string>

#include "opentelemetry/sdk/configuration/headers_configuration.h"
#include "opentelemetry/sdk/configuration/include_exclude_configuration.h"
#include "opentelemetry/sdk/configuration/pull_metric_exporter_configuration.h"
#include "opentelemetry/sdk/configuration/pull_metric_exporter_configuration_visitor.h"
#include "opentelemetry/sdk/configuration/translation_strategy.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/meter_provider.json
// YAML-NODE: Prometheus
class PrometheusPullMetricExporterConfiguration : public PullMetricExporterConfiguration
{
public:
  static constexpr const char *kDefaultHost       = "localhost";
  static constexpr std::size_t kDefaultPort       = 9464;
  static constexpr bool kDefaultWithoutScopeInfo  = false;  // schema: scope_info_enabled=true
  static constexpr bool kDefaultWithoutTargetInfo = false;  // schema: target_info_enabled=true
  static constexpr TranslationStrategy kDefaultTranslationStrategy =
      TranslationStrategy::UnderscoreEscapingWithSuffixes;

  void Accept(PullMetricExporterConfigurationVisitor *visitor) const override
  {
    visitor->VisitPrometheus(this);
  }

  std::string host{kDefaultHost};
  std::size_t port{kDefaultPort};
  bool without_scope_info{kDefaultWithoutScopeInfo};
  bool without_target_info{kDefaultWithoutTargetInfo};
  std::unique_ptr<IncludeExcludeConfiguration> with_resource_constant_labels;
  TranslationStrategy translation_strategy{kDefaultTranslationStrategy};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
