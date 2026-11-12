# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


from typing import Final

HTTP_CLIENT_REQUEST_DURATION: Final = "http.client.request.duration"
"""
Duration of HTTP client requests
Instrument: histogram
Unit: s
"""


HTTP_SERVER_REQUEST_DURATION: Final = "http.server.request.duration"
"""
Duration of HTTP server requests
Instrument: histogram
Unit: s
"""
