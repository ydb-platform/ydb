# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

SERVER_ADDRESS: Final = "server.address"
"""
Server domain name if available without reverse DNS lookup; otherwise, IP address or Unix domain socket name.
Note: When observed from the client side, and when communicating through an intermediary, `server.address` SHOULD represent the server address behind any intermediaries, for example proxies, if it's available.
"""

SERVER_PORT: Final = "server.port"
"""
Server port number.
Note: When observed from the client side, and when communicating through an intermediary, `server.port` SHOULD represent the server port behind any intermediaries, for example proxies, if it's available.
"""
