// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/metrics/view/attributes_processor.h"

#include <utility>

#include "opentelemetry/common/attribute_value.h"
#include "opentelemetry/common/key_value_iterable.h"
#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/version.h"
#include "src/common/wildcard_match.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

IncludeExcludeAttributesProcessor::IncludeExcludeAttributesProcessor(
    bool include_all,
    std::vector<std::string> included_patterns,
    std::vector<std::string> excluded_patterns)
    : include_all_(include_all),
      included_patterns_(std::move(included_patterns)),
      excluded_patterns_(std::move(excluded_patterns))
{}

MetricAttributes IncludeExcludeAttributesProcessor::process(
    const opentelemetry::common::KeyValueIterable &attributes) const noexcept
{
  MetricAttributes result;
  attributes.ForEachKeyValue(
      [&](nostd::string_view key, opentelemetry::common::AttributeValue value) noexcept {
        if (isPresent(key))
        {
          result.SetAttribute(key, value);
        }
        return true;
      });

  result.UpdateHash();
  return result;
}

bool IncludeExcludeAttributesProcessor::isPresent(nostd::string_view key) const noexcept
{
  return (include_all_ || MatchesAny(included_patterns_, key)) &&
         !MatchesAny(excluded_patterns_, key);
}

bool IncludeExcludeAttributesProcessor::MatchesAny(const std::vector<std::string> &patterns,
                                                   nostd::string_view key) noexcept
{
  for (const auto &pattern : patterns)
  {
    if (common::WildcardMatch(pattern, key))
    {
      return true;
    }
  }
  return false;
}

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
