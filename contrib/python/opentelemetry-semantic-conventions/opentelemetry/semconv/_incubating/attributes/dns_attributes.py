# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from typing import Final

DNS_ANSWERS: Final = "dns.answers"
"""
The list of IPv4 or IPv6 addresses resolved during DNS lookup.
"""

DNS_QUESTION_NAME: Final = "dns.question.name"
"""
The name being queried.
Note: The name represents the queried domain name as it appears in the DNS query without any additional normalization.
"""
