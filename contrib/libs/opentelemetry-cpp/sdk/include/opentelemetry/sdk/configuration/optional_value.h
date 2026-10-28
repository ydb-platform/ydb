// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stdexcept>

#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace configuration
{

/**
 * C++14-friendly optional for configuration fields that may be set, omitted, or
 * explicitly null in YAML.
 */
template <typename T>
class OptionalValue
{
public:
  OptionalValue() = default;

  explicit OptionalValue(T value) : has_value_(true), value_(value) {}

  bool HasValue() const { return has_value_; }

  const T &Value() const
  {
    if (!has_value_)
    {
      throw std::runtime_error("OptionalValue has no value");
    }
    return value_;
  }

  T ValueOr(T fallback) const { return has_value_ ? value_ : fallback; }

  OptionalValue &operator=(T value)
  {
    has_value_ = true;
    value_     = value;
    return *this;
  }

private:
  bool has_value_{false};
  T value_{};
};

}  // namespace configuration
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
