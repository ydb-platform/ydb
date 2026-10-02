// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/sdk/configuration/severity_number.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

// YAML-SCHEMA: schema/logger_provider.yaml
// YAML-NODE: LoggerConfig
class LoggerConfigConfiguration
{
public:
  static constexpr bool kDefaultEnabled                   = true;
  static constexpr SeverityNumber kDefaultMinimumSeverity = SeverityNumber::trace;
  static constexpr bool kDefaultTraceBased                = false;

  bool enabled{kDefaultEnabled};
  SeverityNumber minimum_severity{kDefaultMinimumSeverity};
  bool trace_based{kDefaultTraceBased};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
