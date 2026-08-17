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
namespace session
{

/**
  Indicates that a session has ended.
  <p>
  For instrumentation that tracks user behavior during user sessions, a @code session.end @endcode
  event SHOULD be emitted every time a session ends. When a session ends and continues as a new
  session, this event SHOULD be emitted prior to the @code session.start @endcode event.
 */
static constexpr const char *kSessionEnd = "session.end";

/**
  Indicates that a new session has been started, optionally linking to the prior session.
  <p>
  For instrumentation that tracks user behavior during user sessions, a @code session.start @endcode
  event MUST be emitted every time a session is created. When a new session is created as a
  continuation of a prior session, the @code session.previous_id @endcode SHOULD be included in the
  event. The values of @code session.id @endcode and @code session.previous_id @endcode MUST be
  different. When the @code session.start @endcode event contains both @code session.id @endcode and
  @code session.previous_id @endcode fields, the event indicates that the previous session has
  ended. If the session ID in @code session.previous_id @endcode has not yet ended via explicit
  @code session.end @endcode event, then the consumer SHOULD treat this continuation event as
  semantically equivalent to @code session.end(session.previous_id) @endcode and @code
  session.start(session.id) @endcode.
 */
static constexpr const char *kSessionStart = "session.start";

}  // namespace session
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
