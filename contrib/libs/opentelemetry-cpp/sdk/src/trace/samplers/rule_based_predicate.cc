// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "opentelemetry/sdk/trace/samplers/rule_based_predicate.h"

#include <algorithm>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <limits>
#include <string>
#include <utility>

#include "opentelemetry/common/attribute_value.h"
#include "opentelemetry/common/key_value_iterable.h"
#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/nostd/span.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/nostd/variant.h"
#include "opentelemetry/sdk/common/attribute_utils.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/version.h"

#include "src/common/wildcard_match.h"

namespace
{

using opentelemetry::nostd::function_ref;
using opentelemetry::sdk::common::WildcardMatch;
namespace nostd = opentelemetry::nostd;

// Fits the widest value below: a 17 significant digit double takes 24 characters, an int64 20.
constexpr std::size_t kValueBufferSize = 32;

// No two doubles differ past max_digits10 significant digits.
constexpr std::uint32_t kMaxDoublePrecision =
    static_cast<std::uint32_t>(std::numeric_limits<double>::max_digits10);

// Checks every string form of an attribute value; array values match if any item matches.
class ValueMatcher
{
public:
  ValueMatcher(function_ref<bool(nostd::string_view)> check, std::uint32_t double_precision)
      : check_(check),
        double_precision_(static_cast<int>((std::min)(double_precision, kMaxDoublePrecision)))
  {}

  bool operator()(bool v) const { return check_(v ? "true" : "false"); }
  bool operator()(int32_t v) const
  {
    char buffer[kValueBufferSize];
    return CheckWritten(buffer, std::snprintf(buffer, sizeof(buffer), "%" PRId32, v));
  }
  bool operator()(int64_t v) const
  {
    char buffer[kValueBufferSize];
    return CheckWritten(buffer, std::snprintf(buffer, sizeof(buffer), "%" PRId64, v));
  }
  bool operator()(uint32_t v) const
  {
    char buffer[kValueBufferSize];
    return CheckWritten(buffer, std::snprintf(buffer, sizeof(buffer), "%" PRIu32, v));
  }
  bool operator()(uint64_t v) const
  {
    char buffer[kValueBufferSize];
    return CheckWritten(buffer, std::snprintf(buffer, sizeof(buffer), "%" PRIu64, v));
  }
  bool operator()(double v) const
  {
    if (std::isnan(v))
    {
      return check_("NaN");
    }
    if (std::isinf(v))
    {
      return check_(v < 0 ? "-Infinity" : "Infinity");
    }
    char buffer[kValueBufferSize];
    return CheckWritten(buffer,
                        std::snprintf(buffer, sizeof(buffer), "%.*g", double_precision_, v));
  }
  bool operator()(const char *v) const { return v != nullptr && check_(v); }
  bool operator()(nostd::string_view v) const { return check_(v); }
  // Array values match when any item matches.
  template <typename T>
  bool operator()(nostd::span<const T> v) const
  {
    for (const auto &value : v)
    {
      if ((*this)(value))
      {
        return true;
      }
    }
    return false;
  }
  // Byte arrays have no string form to match.
  bool operator()(nostd::span<const uint8_t> /* v */) const { return false; }

private:
  // A failed or truncated write cannot match.
  bool CheckWritten(const char *buffer, int length) const
  {
    if (length < 0 || length >= static_cast<int>(kValueBufferSize))
    {
      return false;
    }
    return check_(nostd::string_view(buffer, static_cast<std::size_t>(length)));
  }

  function_ref<bool(nostd::string_view)> check_;
  int double_precision_;
};

bool AttributeMatches(const opentelemetry::common::KeyValueIterable &attributes,
                      const std::string &key,
                      std::uint32_t double_precision,
                      function_ref<bool(nostd::string_view)> check) noexcept
{
  bool matched = false;
  attributes.ForEachKeyValue(
      [&](nostd::string_view attr_key, opentelemetry::common::AttributeValue value) {
        if (attr_key != key)
        {
          return true;
        }
        const auto result =
            opentelemetry::sdk::common::VisitVariant(ValueMatcher(check, double_precision), value);
        matched = result.second && result.first;
        return false;
      });
  return matched;
}

}  // namespace

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

RuleBasedPredicate::RuleBasedPredicate(RuleBasedPredicateOptions options)
    : options_(std::move(options))
{}

bool RuleBasedPredicate::SpanMatches(
    const opentelemetry::trace::SpanContext &parent_context,
    nostd::string_view /* name */,
    opentelemetry::trace::SpanKind span_kind,
    const opentelemetry::common::KeyValueIterable &attributes,
    const opentelemetry::trace::SpanContextKeyValueIterable & /* links */) const noexcept
{
  if (!ParentMatches(parent_context))
  {
    return false;
  }
  if (!SpanKindMatches(span_kind))
  {
    return false;
  }
  if (options_.match_values &&
      !AttributeMatches(attributes, options_.values_key, options_.double_precision,
                        [this](nostd::string_view candidate) {
                          return std::find(options_.values.begin(), options_.values.end(),
                                           candidate) != options_.values.end();
                        }))
  {
    return false;
  }
  if (options_.match_patterns &&
      !AttributeMatches(attributes, options_.patterns_key, options_.double_precision,
                        [this](nostd::string_view candidate) {
                          for (const auto &pattern : options_.excluded)
                          {
                            if (WildcardMatch(pattern, candidate))
                            {
                              return false;
                            }
                          }
                          if (options_.included.empty())
                          {
                            return true;
                          }
                          for (const auto &pattern : options_.included)
                          {
                            if (WildcardMatch(pattern, candidate))
                            {
                              return true;
                            }
                          }
                          return false;
                        }))
  {
    return false;
  }
  return true;
}

nostd::string_view RuleBasedPredicate::GetDescription() const noexcept
{
  return "RuleBasedPredicate";
}

bool RuleBasedPredicate::ParentMatches(
    const opentelemetry::trace::SpanContext &parent_context) const noexcept
{
  if (!(options_.match_parent_none || options_.match_parent_remote || options_.match_parent_local))
  {
    return true;
  }
  if (!parent_context.IsValid())
  {
    return options_.match_parent_none;
  }
  return parent_context.IsRemote() ? options_.match_parent_remote : options_.match_parent_local;
}

bool RuleBasedPredicate::SpanKindMatches(opentelemetry::trace::SpanKind span_kind) const noexcept
{
  if (!(options_.match_span_kind_internal || options_.match_span_kind_server ||
        options_.match_span_kind_client || options_.match_span_kind_producer ||
        options_.match_span_kind_consumer))
  {
    return true;
  }
  switch (span_kind)
  {
    case opentelemetry::trace::SpanKind::kInternal:
      return options_.match_span_kind_internal;
    case opentelemetry::trace::SpanKind::kServer:
      return options_.match_span_kind_server;
    case opentelemetry::trace::SpanKind::kClient:
      return options_.match_span_kind_client;
    case opentelemetry::trace::SpanKind::kProducer:
      return options_.match_span_kind_producer;
    case opentelemetry::trace::SpanKind::kConsumer:
      return options_.match_span_kind_consumer;
  }
  return false;
}

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
