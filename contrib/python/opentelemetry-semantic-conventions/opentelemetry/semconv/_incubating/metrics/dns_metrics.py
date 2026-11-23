# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


from typing import Final

from opentelemetry.metrics import Histogram, Meter

DNS_LOOKUP_DURATION: Final = "dns.lookup.duration"
"""
Measures the time taken to perform a DNS lookup
Instrument: histogram
Unit: s
"""


def create_dns_lookup_duration(meter: Meter) -> Histogram:
    """Measures the time taken to perform a DNS lookup"""
    return meter.create_histogram(
        name=DNS_LOOKUP_DURATION,
        description="Measures the time taken to perform a DNS lookup.",
        unit="s",
    )
