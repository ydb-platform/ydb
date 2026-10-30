# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

DESTINATION_ADDRESS: Final = "destination.address"
"""
Destination address - domain name if available without reverse DNS lookup; otherwise, IP address or Unix domain socket name.
Note: When observed from the source side, and when communicating through an intermediary, `destination.address` SHOULD represent the destination address behind any intermediaries, for example proxies, if it's available.
"""

DESTINATION_PORT: Final = "destination.port"
"""
Destination port number.
"""
