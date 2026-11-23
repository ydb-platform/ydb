# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

URL_FRAGMENT: Final = "url.fragment"
"""
The [URI fragment](https://www.rfc-editor.org/rfc/rfc3986#section-3.5) component.
"""

URL_FULL: Final = "url.full"
"""
Absolute URL describing a network resource according to [RFC3986](https://www.rfc-editor.org/rfc/rfc3986).
Note: For network calls, URL usually has `scheme://host[:port][path][?query][#fragment]` format, where the fragment
is not transmitted over HTTP, but if it is known, it SHOULD be included nevertheless.

`url.full` MUST NOT contain credentials passed via URL in form of `https://username:password@www.example.com/`.
In such case username and password SHOULD be redacted and attribute's value SHOULD be `https://REDACTED:REDACTED@www.example.com/`.

`url.full` SHOULD capture the absolute URL when it is available (or can be reconstructed).

Sensitive content provided in `url.full` SHOULD be scrubbed when instrumentations can identify it.

![Development](https://img.shields.io/badge/-development-blue)
Query string values for the following keys SHOULD be redacted by default and replaced by the
value `REDACTED`:

* [`X-Amz-Signature`](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html)
* [`X-Amz-Credential`](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html)
* [`X-Amz-Security-Token`](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html)
* [`sig`](https://learn.microsoft.com/azure/storage/common/storage-sas-overview#sas-token)
* [`X-Goog-Signature`](https://cloud.google.com/storage/docs/access-control/signed-urls)

This list is subject to change over time.

Matching of query parameter keys against the sensitive list SHOULD be case-sensitive.

![Development](https://img.shields.io/badge/-development-blue)
Instrumentation MAY provide a way to override this list via declarative configuration.
If so, it SHOULD use the `sensitive_query_parameters` property
(an array of case-sensitive strings with minimum items 0) under
`.instrumentation/development.general.sanitization.url`.
This list is a full override of the default sensitive query parameter keys,
it is not a list of keys in addition to the defaults.

When a query string value is redacted, the query string key SHOULD still be preserved, e.g.
`https://www.example.com/path?color=blue&sig=REDACTED`.
"""

URL_PATH: Final = "url.path"
"""
The [URI path](https://www.rfc-editor.org/rfc/rfc3986#section-3.3) component.
Note: Sensitive content provided in `url.path` SHOULD be scrubbed when instrumentations can identify it.
"""

URL_QUERY: Final = "url.query"
"""
The [URI query](https://www.rfc-editor.org/rfc/rfc3986#section-3.4) component.
Note: Sensitive content provided in `url.query` SHOULD be scrubbed when instrumentations can identify it.

![Development](https://img.shields.io/badge/-development-blue)
Query string values for the following keys SHOULD be redacted by default and replaced by the value `REDACTED`:

* [`X-Amz-Signature`](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html)
* [`X-Amz-Credential`](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html)
* [`X-Amz-Security-Token`](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-authentication-methods.html)
* [`sig`](https://learn.microsoft.com/azure/storage/common/storage-sas-overview#sas-token)
* [`X-Goog-Signature`](https://cloud.google.com/storage/docs/access-control/signed-urls)

This list is subject to change over time.

Matching of query parameter keys against the sensitive list SHOULD be case-sensitive.

Instrumentation MAY provide a way to override this list via declarative configuration.
If so, it SHOULD use the `sensitive_query_parameters` property
(an array of case-sensitive strings with minimum items 0) under
`.instrumentation/development.general.sanitization.url`.
This list is a full override of the default sensitive query parameter keys,
it is not a list of keys in addition to the defaults.

When a query string value is redacted, the query string key SHOULD still be preserved, e.g.
`q=OpenTelemetry&sig=REDACTED`.
"""

URL_SCHEME: Final = "url.scheme"
"""
The [URI scheme](https://www.rfc-editor.org/rfc/rfc3986#section-3.1) component identifying the used protocol.
"""
