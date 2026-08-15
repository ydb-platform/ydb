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
namespace device
{

/**
  This event represents an occurrence of a lifecycle transition on Android or iOS platform.
  <p>
  The event body fields MUST be used to describe the state of the application at the time of the
  event. This event is meant to be used in conjunction with @code os.name @endcode <a
  href="/docs/resource/os.md">resource semantic convention</a> to identify the mobile operating
  system (e.g. Android, iOS). The @code android.app.state @endcode and @code ios.app.state @endcode
  fields are mutually exclusive and MUST NOT be used together, each field MUST be used with its
  corresponding @code os.name @endcode value.
 */
static constexpr const char *kDeviceAppLifecycle = "device.app.lifecycle";

}  // namespace device
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
