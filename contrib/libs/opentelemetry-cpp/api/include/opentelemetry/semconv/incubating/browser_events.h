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
namespace browser
{

/**
  This event describes the website performance metrics introduced by Google, See <a
  href="https://web.dev/vitals">web vitals</a>.
 */
static constexpr const char *kBrowserWebVital = "browser.web_vital";

}  // namespace browser
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
