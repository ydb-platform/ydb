// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#ifdef ENABLE_METRICS_EXEMPLAR_PREVIEW

#  include "opentelemetry/context/context.h"
#  include "opentelemetry/sdk/metrics/exemplar/filter_type.h"
#  include "opentelemetry/trace/context.h"
#  include "opentelemetry/trace/span_context.h"
#  include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace metrics
{

/**
 * Decides whether a measurement is eligible to be offered to the exemplar reservoir.
 *
 *   - kAlwaysOff  : no measurement is eligible (exemplar sampling disabled).
 *   - kAlwaysOn   : every measurement is eligible.
 *   - kTraceBased : eligible only when recorded inside a valid, sampled span.
 */
inline bool ExemplarFilterEnabled(ExemplarFilterType filter_type,
                                  const opentelemetry::context::Context &context) noexcept
{
  switch (filter_type)
  {
    case ExemplarFilterType::kAlwaysOn:
      return true;
    case ExemplarFilterType::kAlwaysOff:
      return false;
    default:  // TraceBased.
      const opentelemetry::trace::SpanContext span_context =
          opentelemetry::trace::GetSpanContext(context);
      return span_context.IsValid() && span_context.IsSampled();
  }
}

}  // namespace metrics
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE

#endif  // ENABLE_METRICS_EXEMPLAR_PREVIEW
