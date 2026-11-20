// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
class InstrumentMetaDataValidator
{
public:
  InstrumentMetaDataValidator();
  bool ValidateName(nostd::string_view name) const;
  bool ValidateUnit(nostd::string_view unit) const;
  bool ValidateDescription(nostd::string_view description) const;
};

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
