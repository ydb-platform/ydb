// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include <memory>

#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/sdk/metrics/view/predicate.h"
#include "opentelemetry/sdk/metrics/view/predicate_factory.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{
std::unique_ptr<Predicate> PredicateFactory::GetPredicate(opentelemetry::nostd::string_view pattern,
                                                          PredicateType type)
{
  if (((type == PredicateType::kPattern || type == PredicateType::kWildcard) && pattern == "*") ||
      (type == PredicateType::kExact && pattern == ""))
  {
    return std::unique_ptr<Predicate>(new MatchEverythingPattern());
  }
  if (type == PredicateType::kPattern)
  {
    return std::unique_ptr<Predicate>(new PatternPredicate(pattern));
  }
  if (type == PredicateType::kExact)
  {
    return std::unique_ptr<Predicate>(new ExactPredicate(pattern));
  }
  if (type == PredicateType::kWildcard)
  {
    return std::unique_ptr<Predicate>(new WildcardPredicate(pattern));
  }
  return std::unique_ptr<Predicate>(new MatchNothingPattern());
}
}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
