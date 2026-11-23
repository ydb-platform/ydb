# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


from typing import Final

from opentelemetry.metrics import Histogram, Meter

RPC_CLIENT_CALL_DURATION: Final = "rpc.client.call.duration"
"""
Measures the duration of an outgoing Remote Procedure Call (RPC)
Instrument: histogram
Unit: s
Note: When this metric is reported alongside an RPC client span, the metric value
SHOULD be the same as the RPC client span duration.
"""


def create_rpc_client_call_duration(meter: Meter) -> Histogram:
    """Measures the duration of an outgoing Remote Procedure Call (RPC)"""
    return meter.create_histogram(
        name=RPC_CLIENT_CALL_DURATION,
        description="Measures the duration of an outgoing Remote Procedure Call (RPC).",
        unit="s",
    )


RPC_CLIENT_DURATION: Final = "rpc.client.duration"
"""
Deprecated: Replaced by `rpc.client.call.duration` with unit `s`.
"""


def create_rpc_client_duration(meter: Meter) -> Histogram:
    """Deprecated, use `rpc.client.call.duration` instead. Note: the unit also changed from `ms` to `s`"""
    return meter.create_histogram(
        name=RPC_CLIENT_DURATION,
        description="Deprecated, use `rpc.client.call.duration` instead. Note: the unit also changed from `ms` to `s`.",
        unit="ms",
    )


RPC_CLIENT_REQUEST_SIZE: Final = "rpc.client.request.size"
"""
Deprecated: Removed, no replacement at this time.
"""


def create_rpc_client_request_size(meter: Meter) -> Histogram:
    """Measures the size of RPC request messages (uncompressed)"""
    return meter.create_histogram(
        name=RPC_CLIENT_REQUEST_SIZE,
        description="Measures the size of RPC request messages (uncompressed).",
        unit="By",
    )


RPC_CLIENT_REQUESTS_PER_RPC: Final = "rpc.client.requests_per_rpc"
"""
Deprecated: Removed, no replacement at this time.
"""


def create_rpc_client_requests_per_rpc(meter: Meter) -> Histogram:
    """Measures the number of messages received per RPC"""
    return meter.create_histogram(
        name=RPC_CLIENT_REQUESTS_PER_RPC,
        description="Measures the number of messages received per RPC.",
        unit="{count}",
    )


RPC_CLIENT_RESPONSE_SIZE: Final = "rpc.client.response.size"
"""
Deprecated: Removed, no replacement at this time.
"""


def create_rpc_client_response_size(meter: Meter) -> Histogram:
    """Measures the size of RPC response messages (uncompressed)"""
    return meter.create_histogram(
        name=RPC_CLIENT_RESPONSE_SIZE,
        description="Measures the size of RPC response messages (uncompressed).",
        unit="By",
    )


RPC_CLIENT_RESPONSES_PER_RPC: Final = "rpc.client.responses_per_rpc"
"""
Deprecated: Removed, no replacement at this time.
"""


def create_rpc_client_responses_per_rpc(meter: Meter) -> Histogram:
    """Measures the number of messages sent per RPC"""
    return meter.create_histogram(
        name=RPC_CLIENT_RESPONSES_PER_RPC,
        description="Measures the number of messages sent per RPC.",
        unit="{count}",
    )


RPC_SERVER_CALL_DURATION: Final = "rpc.server.call.duration"
"""
Measures the duration of an incoming Remote Procedure Call (RPC)
Instrument: histogram
Unit: s
Note: When this metric is reported alongside an RPC server span, the metric value
SHOULD be the same as the RPC server span duration.
"""


def create_rpc_server_call_duration(meter: Meter) -> Histogram:
    """Measures the duration of an incoming Remote Procedure Call (RPC)"""
    return meter.create_histogram(
        name=RPC_SERVER_CALL_DURATION,
        description="Measures the duration of an incoming Remote Procedure Call (RPC).",
        unit="s",
    )


RPC_SERVER_DURATION: Final = "rpc.server.duration"
"""
Deprecated: Replaced by `rpc.server.call.duration` with unit `s`.
"""


def create_rpc_server_duration(meter: Meter) -> Histogram:
    """Deprecated, use `rpc.server.call.duration` instead. Note: the unit also changed from `ms` to `s`"""
    return meter.create_histogram(
        name=RPC_SERVER_DURATION,
        description="Deprecated, use `rpc.server.call.duration` instead. Note: the unit also changed from `ms` to `s`.",
        unit="ms",
    )


RPC_SERVER_REQUEST_SIZE: Final = "rpc.server.request.size"
"""
Deprecated: Removed, no replacement at this time.
"""


def create_rpc_server_request_size(meter: Meter) -> Histogram:
    """Measures the size of RPC request messages (uncompressed)"""
    return meter.create_histogram(
        name=RPC_SERVER_REQUEST_SIZE,
        description="Measures the size of RPC request messages (uncompressed).",
        unit="By",
    )


RPC_SERVER_REQUESTS_PER_RPC: Final = "rpc.server.requests_per_rpc"
"""
Deprecated: Removed, no replacement at this time.
"""


def create_rpc_server_requests_per_rpc(meter: Meter) -> Histogram:
    """Measures the number of messages received per RPC"""
    return meter.create_histogram(
        name=RPC_SERVER_REQUESTS_PER_RPC,
        description="Measures the number of messages received per RPC.",
        unit="{count}",
    )


RPC_SERVER_RESPONSE_SIZE: Final = "rpc.server.response.size"
"""
Deprecated: Removed, no replacement at this time.
"""


def create_rpc_server_response_size(meter: Meter) -> Histogram:
    """Measures the size of RPC response messages (uncompressed)"""
    return meter.create_histogram(
        name=RPC_SERVER_RESPONSE_SIZE,
        description="Measures the size of RPC response messages (uncompressed).",
        unit="By",
    )


RPC_SERVER_RESPONSES_PER_RPC: Final = "rpc.server.responses_per_rpc"
"""
Deprecated: Removed, no replacement at this time.
"""


def create_rpc_server_responses_per_rpc(meter: Meter) -> Histogram:
    """Measures the number of messages sent per RPC"""
    return meter.create_histogram(
        name=RPC_SERVER_RESPONSES_PER_RPC,
        description="Measures the number of messages sent per RPC.",
        unit="{count}",
    )
