// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "opentelemetry/common/key_value_iterable.h"
#include "opentelemetry/nostd/string_view.h"
#include "opentelemetry/trace/span_context.h"
#include "opentelemetry/trace/span_context_kv_iterable.h"
#include "opentelemetry/trace/span_metadata.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

/**
 * Predicate decides whether a span matches a rule in a
 * ComposableRuleBasedSampler. Implementations must be thread-safe; SpanMatches
 * may be called concurrently for spans sharing the sampler.
 */
class Predicate
{
public:
  Predicate()                             = default;
  Predicate(const Predicate &)            = delete;
  Predicate(Predicate &&)                 = delete;
  Predicate &operator=(const Predicate &) = delete;
  Predicate &operator=(Predicate &&)      = delete;
  virtual ~Predicate()                    = default;

  virtual bool SpanMatches(
      const opentelemetry::trace::SpanContext &parent_context,
      nostd::string_view name,
      opentelemetry::trace::SpanKind span_kind,
      const opentelemetry::common::KeyValueIterable &attributes,
      const opentelemetry::trace::SpanContextKeyValueIterable &links) const noexcept = 0;

  virtual nostd::string_view GetDescription() const noexcept = 0;
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
