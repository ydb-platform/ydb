/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * DO NOT EDIT, this is an Auto-generated file from:
 * buildscripts/semantic-convention/templates/registry/semantic_attributes-h.j2
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
  Array of brand name and version separated by a space
  <p>
  This value is intended to be taken from the <a
  href="https://wicg.github.io/ua-client-hints/#interface">UA client hints API</a> (@code
  navigator.userAgentData.brands @endcode).
 */
static constexpr const char *kBrowserBrands = "browser.brands";

/**
  Absolute URL of the current browser document according to <a
  href="https://www.rfc-editor.org/rfc/rfc3986">RFC3986</a>.
 */
static constexpr const char *kBrowserDocumentUrlFull = "browser.document.url.full";

/**
  Preferred language of the user using the browser
  <p>
  This value is intended to be taken from the Navigator API @code navigator.language @endcode.
 */
static constexpr const char *kBrowserLanguage = "browser.language";

/**
  A boolean that is true if the browser is running on a mobile device
  <p>
  This value is intended to be taken from the <a
  href="https://wicg.github.io/ua-client-hints/#interface">UA client hints API</a> (@code
  navigator.userAgentData.mobile @endcode). If unavailable, this attribute SHOULD be left unset.
 */
static constexpr const char *kBrowserMobile = "browser.mobile";

/**
  The platform on which the browser is running
  <p>
  This value is intended to be taken from the <a
  href="https://wicg.github.io/ua-client-hints/#interface">UA client hints API</a> (@code
  navigator.userAgentData.platform @endcode). If unavailable, the legacy @code navigator.platform
  @endcode API SHOULD NOT be used instead and this attribute SHOULD be left unset in order for the
  values to be consistent. The list of possible values is defined in the <a
  href="https://wicg.github.io/ua-client-hints/#sec-ch-ua-platform">W3C User-Agent Client Hints
  specification</a>. Note that some (but not all) of these values can overlap with values in the <a
  href="./os.md">@code os.type @endcode and @code os.name @endcode attributes</a>. However, for
  consistency, the values in the @code browser.platform @endcode attribute should capture the exact
  value that the user agent provides.
 */
static constexpr const char *kBrowserPlatform = "browser.platform";

/**
  The delta between the current value and the last-reported value. See <a
  href="https://github.com/GoogleChrome/web-vitals?tab=readme-ov-file#report-only-the-delta-of-changes">delta</a>.
 */
static constexpr const char *kBrowserWebVitalDelta = "browser.web_vital.delta";

/**
  A unique ID representing this particular metric instance.
 */
static constexpr const char *kBrowserWebVitalId = "browser.web_vital.id";

/**
  Name of the web vital.
 */
static constexpr const char *kBrowserWebVitalName = "browser.web_vital.name";

/**
  The type of navigation, as reported by the <a
  href="https://developer.mozilla.org/docs/Web/API/PerformanceNavigationTiming/type">Navigation
  Timing API</a>, with additional values reported by the web-vitals library.
 */
static constexpr const char *kBrowserWebVitalNavigationType = "browser.web_vital.navigation_type";

/**
  The rating of the web vital value against the "good", "needs improvement", and "poor" thresholds
  defined for the metric.
 */
static constexpr const char *kBrowserWebVitalRating = "browser.web_vital.rating";

/**
  Value of the web vital.
 */
static constexpr const char *kBrowserWebVitalValue = "browser.web_vital.value";

namespace BrowserWebVitalNameValues
{
/**
  Cumulative Layout Shift. See <a href="https://web.dev/articles/cls">cls</a>.
 */
static constexpr const char *kCls = "cls";

/**
  Largest Contentful Paint. See <a href="https://web.dev/articles/lcp">lcp</a>.
 */
static constexpr const char *kLcp = "lcp";

/**
  First Contentful Paint. See <a href="https://web.dev/articles/fcp">fcp</a>.
 */
static constexpr const char *kFcp = "fcp";

/**
  Interaction to Next Paint. See <a href="https://web.dev/articles/inp">inp</a>.
 */
static constexpr const char *kInp = "inp";

/**
  Time to First Byte. See <a href="https://web.dev/articles/ttfb">ttfb</a>.
 */
static constexpr const char *kTtfb = "ttfb";

/**
  First Input Delay. See <a href="https://web.dev/articles/fid">fid</a>.

  @deprecated
  {"note": "Replaced by Interaction to Next Paint (@code inp @endcode), which became a Core Web
  Vital in March 2024. See <a href="https://web.dev/articles/inp">inp</a>.", "reason": "obsoleted"}
 */
OPENTELEMETRY_DEPRECATED static constexpr const char *kFid = "fid";

}  // namespace BrowserWebVitalNameValues

namespace BrowserWebVitalNavigationTypeValues
{
/**
  Navigation started by clicking a link, entering a URL, form submission, or a script operation.
 */
static constexpr const char *kNavigate = "navigate";

/**
  Navigation through a reload operation or a @code Location.reload() @endcode call.
 */
static constexpr const char *kReload = "reload";

/**
  Navigation through the browser's history traversal (e.g. back/forward buttons).
 */
static constexpr const char *kBackForward = "back-forward";

/**
  Navigation restoring a page from the back/forward cache (bfcache).
 */
static constexpr const char *kBackForwardCache = "back-forward-cache";

/**
  Navigation to a page that was prerendered.
 */
static constexpr const char *kPrerender = "prerender";

/**
  Navigation restoring a page that was previously discarded by the browser.
 */
static constexpr const char *kRestore = "restore";

}  // namespace BrowserWebVitalNavigationTypeValues

namespace BrowserWebVitalRatingValues
{
/**
  The metric value is within the "good" threshold.
 */
static constexpr const char *kGood = "good";

/**
  The metric value is within the "needs improvement" threshold.
 */
static constexpr const char *kNeedsImprovement = "needs-improvement";

/**
  The metric value is within the "poor" threshold.
 */
static constexpr const char *kPoor = "poor";

}  // namespace BrowserWebVitalRatingValues

}  // namespace browser
}  // namespace semconv
OPENTELEMETRY_END_NAMESPACE
