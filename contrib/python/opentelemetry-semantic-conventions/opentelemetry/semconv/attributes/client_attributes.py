# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

CLIENT_ADDRESS: Final = "client.address"
"""
Client address - domain name if available without reverse DNS lookup; otherwise, IP address or Unix domain socket name.
Note: When observed from the server side, and when communicating through an intermediary, `client.address` SHOULD represent the client address behind any intermediaries,  for example proxies, if it's available.
"""

CLIENT_PORT: Final = "client.port"
"""
Client port number.
Note: When observed from the server side, and when communicating through an intermediary, `client.port` SHOULD represent the client port behind any intermediaries,  for example proxies, if it's available.
"""
