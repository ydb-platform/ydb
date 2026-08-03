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
namespace app
{

/**
  This event represents a termination of a user-facing application due to
  unrecoverable programming errors such as exceptions or signals that
  indicate an error has happened at a lower level.
  <p>
  Crash events can be produced asynchronously by an OTel SDK instance that is
  not running in the application instance in which the crash happened. For
  example, the instrumentation may report crashes from previous app instances
  based on information found in tombstones on disk.
  If the reporter of the crash is not the crashing application instance itself,
  relevant resource attributes that identify the application instance that
  crashed MUST be provided as event attributes so that the corresponding
  attributes from the reporter's resource aren't used instead.
  How the instrumentation will determine whether an instance of a crash has
  already been reported and how the necessary data will be retrieved is left up
  to the instrumentation. Providing enough data to dedupe is NOT REQUIRED.
 */
static constexpr const char *kAppCrash = "app.crash";

/**
  This event indicates that the application has detected substandard UI rendering performance.
  <p>
  Jank happens when the UI is rendered slowly enough for the user to experience some disruption or
  sluggishness.
 */
static constexpr const char *kAppJank = "app.jank";

/**
  This event represents an instantaneous click on the screen of an application.
  <p>
  The @code app.screen.click @endcode event can be used to indicate that a user has clicked or
  tapped on the screen portion of an application. Clicks outside of an application's active area
  SHOULD NOT generate this event. This event does not differentiate between touch/mouse down and
  touch/mouse up. Implementations SHOULD give preference to generating this event at the time the
  click is complete, typically on touch release or mouse up. The location of the click event MUST be
  provided in absolute screen pixels.
 */
static constexpr const char *kAppScreenClick = "app.screen.click";

/**
  This event indicates that an application widget has been clicked.
  <p>
  Use this event to indicate that visual application component has been clicked, typically through a
  user's manual interaction.
 */
static constexpr const char *kAppWidgetClick = "app.widget.click";

}  // namespace app
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
