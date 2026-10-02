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
//    Get() reads directly from the process environment on every call. Callers should
//    create a carrier and call Extract() once during process initialization and retain
//    the resulting Context for the lifetime of the process or task. Keys are normalized
//    to environment variable names before lookup (e.g. "traceparent" -> "TRACEPARENT").
//    Set() is a no-op.
//
// 2. Inject (shared_ptr constructor): Writes context to a provided map.
//    Set() writes to the provided std::map. Keys are automatically converted from
//    lowercase header names to uppercase environment variable names. The resulting map
//    is then typically merged into the environment of a child process before spawning.
//
// Operational guidance (non-normative):
//
// - Initialization-time extraction: Extract context once at program startup and thread
//   the resulting Context through the rest of the application. Do not repeatedly call
//   Get() on a long-lived carrier; environment variables are intended to be set by the
//   parent before this process starts and should not change during process lifetime.
//
// - Child process environment handling: To propagate context to a child process, create
//   a carrier with an env_map, call Inject() to populate the map, then merge the map
//   into the child process environment (e.g., via execve/posix_spawn env, or equivalent
//   platform APIs) before spawning. Create separate maps for child processes with
//   different contexts.
//
// - Security considerations: Environment variables are visible to all code within the
//   process and may be visible to other processes or users with sufficient permissions.
//   Do not propagate sensitive information (credentials, secrets) via environment
//   variable carriers. In multi-tenant environments, consider the extra exposure risk
//   of environment variables being accessible to co-located processes.

class EnvironmentCarrier : public TextMapCarrier
{
public:
  // Constructs an EnvironmentCarrier for Extract operations.
  EnvironmentCarrier() noexcept = default;

  // Constructs an EnvironmentCarrier for Inject operations.
  explicit EnvironmentCarrier(std::shared_ptr<std::map<std::string, std::string>> env_map) noexcept
      : env_map_ptr_(std::move(env_map))
  {}

  // Returns the value of the environment variable corresponding to the normalized form of key.
  // The key is normalized per spec before lookup (e.g. "traceparent" -> "TRACEPARENT").
  // Returns an empty string_view if the variable is not set.
  //
  // The returned string_view is valid for the lifetime of this carrier object.
  //
  // Note: values are cached on first access. This is safe because environment variables used
  // for context propagation are set by the parent process before this process starts and are
  // not expected to change during process lifetime. Caching also ensures that the returned
  // string_view remains valid for the lifetime of the carrier, regardless of when it is read.
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
  //   - empty key is normalized to "_"
  //   - ASCII letters are uppercased
  //   - characters that are not ASCII letters, digits, or '_' are replaced with '_'
  //   - a leading '_' is prepended if the first character is a digit
  // Examples:
  //   ""            -> "_"
  //   "traceparent" -> "TRACEPARENT"
  //   "x-b3-traceid"-> "X_B3_TRACEID"
  //   "1bad"        -> "_1BAD"
  static std::string NormalizeKey(nostd::string_view key)
  {
    // Spec: empty key normalizes to "_"
    if (key.empty())
    {
      return "_";
    }

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
    if (result[0] >= '0' && result[0] <= '9')
    {
      result.insert(result.begin(), '_');
    }
    return result;
  }
};

}  // namespace propagation
}  // namespace context
OPENTELEMETRY_END_NAMESPACE
