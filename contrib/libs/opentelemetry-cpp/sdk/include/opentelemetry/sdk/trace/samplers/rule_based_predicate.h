// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "opentelemetry/sdk/trace/samplers/predicate.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

/**
 * Matching criteria for RuleBasedPredicate. A criteria group left at its
 * default is inactive and matches every span.
 */
struct RuleBasedPredicateOptions
{
  bool match_values = false;
  std::string values_key;
  std::vector<std::string> values;

  bool match_patterns = false;
  std::string patterns_key;
  std::vector<std::string> included;
  std::vector<std::string> excluded;

  bool match_parent_none   = false;
  bool match_parent_remote = false;
  bool match_parent_local  = false;

  bool match_span_kind_internal = false;
  bool match_span_kind_server   = false;
  bool match_span_kind_client   = false;
  bool match_span_kind_producer = false;
  bool match_span_kind_consumer = false;

  // Significant digits used to format double attribute values for matching, capped at 17.
  std::uint32_t double_precision = 6;
};

/**
 * RuleBasedPredicate matches a span when every active criteria group in its
 * options matches: parent kind, span kind, an attribute equal to one of the
 * given values, and an attribute matching the included/excluded wildcard
 * patterns (excluded wins). Non-string attributes are matched by their string
 * representation; doubles are formatted with %g at double_precision significant
 * digits, so 404.0 matches "404".
 */
class RuleBasedPredicate : public Predicate
{
public:
  explicit RuleBasedPredicate(RuleBasedPredicateOptions options);

  bool SpanMatches(
      const opentelemetry::trace::SpanContext &parent_context,
      nostd::string_view name,
      opentelemetry::trace::SpanKind span_kind,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links) const noexcept override;

  nostd::string_view GetDescription() const noexcept override;

private:
  bool ParentMatches(const opentelemetry::trace::SpanContext &parent_context) const noexcept;
  bool SpanKindMatches(opentelemetry::trace::SpanKind span_kind) const noexcept;

  RuleBasedPredicateOptions options_;
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
