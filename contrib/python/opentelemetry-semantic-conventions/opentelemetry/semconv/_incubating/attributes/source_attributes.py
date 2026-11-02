# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

SOURCE_ADDRESS: Final = "source.address"
"""
Source address - domain name if available without reverse DNS lookup; otherwise, IP address or Unix domain socket name.
Note: When observed from the destination side, and when communicating through an intermediary, `source.address` SHOULD represent the source address behind any intermediaries, for example proxies, if it's available.
"""

SOURCE_PORT: Final = "source.port"
"""
Source port number.
"""
