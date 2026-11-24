# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

BROWSER_BRANDS: Final = "browser.brands"
"""
Array of brand name and version separated by a space.
Note: This value is intended to be taken from the [UA client hints API](https://wicg.github.io/ua-client-hints/#interface) (`navigator.userAgentData.brands`).
"""

BROWSER_DOCUMENT_URL_FULL: Final = "browser.document.url.full"
"""
Absolute URL of the current browser document according to [RFC3986](https://www.rfc-editor.org/rfc/rfc3986).
"""

BROWSER_LANGUAGE: Final = "browser.language"
"""
Preferred language of the user using the browser.
Note: This value is intended to be taken from the Navigator API `navigator.language`.
"""

BROWSER_MOBILE: Final = "browser.mobile"
"""
A boolean that is true if the browser is running on a mobile device.
Note: This value is intended to be taken from the [UA client hints API](https://wicg.github.io/ua-client-hints/#interface) (`navigator.userAgentData.mobile`). If unavailable, this attribute SHOULD be left unset.
"""

BROWSER_PLATFORM: Final = "browser.platform"
"""
The platform on which the browser is running.
Note: This value is intended to be taken from the [UA client hints API](https://wicg.github.io/ua-client-hints/#interface) (`navigator.userAgentData.platform`). If unavailable, the legacy `navigator.platform` API SHOULD NOT be used instead and this attribute SHOULD be left unset in order for the values to be consistent.
The list of possible values is defined in the [W3C User-Agent Client Hints specification](https://wicg.github.io/ua-client-hints/#sec-ch-ua-platform). Note that some (but not all) of these values can overlap with values in the [`os.type` and `os.name` attributes](./os.md). However, for consistency, the values in the `browser.platform` attribute should capture the exact value that the user agent provides.
"""
