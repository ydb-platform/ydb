// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdlib>
#include <map>
#include <memory>
#include <string>

#include "opentelemetry/context/propagation/text_map_propagator.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace context
{
namespace propagation
{

// EnvironmentCarrier is a TextMapCarrier that reads from and writes to environment variables.
//
// This carrier enables context propagation across process boundaries using environment variables
// as specified in the OpenTelemetry specification:
// https://opentelemetry.io/docs/specs/otel/context/env-carriers/
//
// The carrier supports two usage scenarios:
//
// 1. Extract (default constructor): Reads context from environment variables.
//    Get() reads from TRACEPARENT, TRACESTATE, BAGGAGE environment variables.
//    Set() is a no-op. Values are cached on first access for lifetime management.
//
// 2. Inject (shared_ptr constructor): Writes context to a provided map.
//    Set() writes to the provided std::map. Keys are automatically converted
//    from lowercase header names to uppercase environment variable names.

class EnvironmentCarrier : public TextMapCarrier
{
public:
  // Constructs an EnvironmentCarrier for Extract operations.
  EnvironmentCarrier() noexcept = default;

  // Constructs an EnvironmentCarrier for Inject operations.
  explicit EnvironmentCarrier(std::shared_ptr<std::map<std::string, std::string>> env_map) noexcept
      : env_map_ptr_(std::move(env_map))
  {}

  // Returns the value associated with the passed key.
  // The key is normalized per spec before lookup and caching.
  nostd::string_view Get(nostd::string_view key) const noexcept override
  {
    std::string env_name = NormalizeKey(key);

    // Cache is keyed by normalized name for consistent lookup
    auto cache_it = cache_.find(env_name);
    if (cache_it != cache_.end())
    {
      return cache_it->second;
    }

    const char *value = std::getenv(env_name.c_str());
    if (value != nullptr)
    {
      cache_[env_name] = std::string(value);
      return cache_[env_name];
    }
    return "";
  }

  // Stores the key-value pair in the map if one was provided at construction.
  // The key is normalized per spec before writing.
  void Set(nostd::string_view key, nostd::string_view value) noexcept override
  {
    if (!env_map_ptr_)
    {
      return;
    }

    env_map_ptr_->operator[](NormalizeKey(key)) = std::string(value);
  }

private:
  std::shared_ptr<std::map<std::string, std::string>> env_map_ptr_;
  mutable std::map<std::string, std::string> cache_;

  // Normalizes a key to an environment variable name per the OTel spec:
  //   - ASCII letters are uppercased
  //   - characters that are not ASCII letters, digits, or '_' are replaced with '_'
  //   - a leading '_' is prepended if the first character is a digit
  // e.g., "traceparent" -> "TRACEPARENT", "x-b3-traceid" -> "X_B3_TRACEID",
  //        "1bad" -> "_1BAD"
  static std::string NormalizeKey(nostd::string_view key)
  {
    std::string result(key);
    for (auto &c : result)
    {
      unsigned char uc = static_cast<unsigned char>(c);
      if (uc >= 'a' && uc <= 'z')
      {
        c = static_cast<char>(uc - ('a' - 'A'));
      }
      else if (!((uc >= 'A' && uc <= 'Z') || (uc >= '0' && uc <= '9') || uc == '_'))
      {
        c = '_';
      }
    }
    if (!result.empty() && result[0] >= '0' && result[0] <= '9')
    {
      result.insert(result.begin(), '_');
    }
    return result;
  }
};

}  // namespace propagation
}  // namespace context
OPENTELEMETRY_END_NAMESPACE
