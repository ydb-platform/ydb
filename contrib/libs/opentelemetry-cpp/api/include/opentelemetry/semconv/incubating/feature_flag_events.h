/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * DO NOT EDIT, this is an Auto-generated file from:
 * buildscripts/semantic-convention/templates/registry/semantic_events-h.j2
 */

#pragma once

#include "opentelemetry/common/macros.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace semconv
{
namespace feature_flag
{

/**
  Defines feature flag evaluation as an event.
  <p>
  A @code feature_flag.evaluation @endcode event SHOULD be emitted whenever a feature flag value is
  evaluated, which may happen many times over the course of an application lifecycle. For example, a
  website A/B testing different animations may evaluate a flag each time a button is clicked. A
  @code feature_flag.evaluation @endcode event is emitted on each evaluation even if the result is
  the same.
 */
static constexpr const char *kFeatureFlagEvaluation = "feature_flag.evaluation";

}  // namespace feature_flag
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
