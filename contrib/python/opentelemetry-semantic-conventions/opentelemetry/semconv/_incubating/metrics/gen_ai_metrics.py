# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0


from typing import Final

from opentelemetry.metrics import Histogram, Meter

GEN_AI_CLIENT_OPERATION_DURATION: Final = "gen_ai.client.operation.duration"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


def create_gen_ai_client_operation_duration(meter: Meter) -> Histogram:
    """GenAI operation duration"""
    return meter.create_histogram(
        name=GEN_AI_CLIENT_OPERATION_DURATION,
        description="GenAI operation duration.",
        unit="s",
    )


GEN_AI_CLIENT_OPERATION_TIME_PER_OUTPUT_CHUNK: Final = (
    "gen_ai.client.operation.time_per_output_chunk"
)
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


def create_gen_ai_client_operation_time_per_output_chunk(
    meter: Meter,
) -> Histogram:
    """Time per output chunk, recorded for each chunk received after the first one, measured as the time elapsed from the end of the previous chunk to the end of the current chunk"""
    return meter.create_histogram(
        name=GEN_AI_CLIENT_OPERATION_TIME_PER_OUTPUT_CHUNK,
        description="Time per output chunk, recorded for each chunk received after the first one, measured as the time elapsed from the end of the previous chunk to the end of the current chunk.",
        unit="s",
    )


GEN_AI_CLIENT_OPERATION_TIME_TO_FIRST_CHUNK: Final = (
    "gen_ai.client.operation.time_to_first_chunk"
)
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


def create_gen_ai_client_operation_time_to_first_chunk(
    meter: Meter,
) -> Histogram:
    """Time to receive the first chunk, measured from when the client issues the generation request to when the first chunk is received in the response stream"""
    return meter.create_histogram(
        name=GEN_AI_CLIENT_OPERATION_TIME_TO_FIRST_CHUNK,
        description="Time to receive the first chunk, measured from when the client issues the generation request to when the first chunk is received in the response stream.",
        unit="s",
    )


GEN_AI_CLIENT_TOKEN_USAGE: Final = "gen_ai.client.token.usage"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


def create_gen_ai_client_token_usage(meter: Meter) -> Histogram:
    """Number of input and output tokens used"""
    return meter.create_histogram(
        name=GEN_AI_CLIENT_TOKEN_USAGE,
        description="Number of input and output tokens used.",
        unit="{token}",
    )


GEN_AI_SERVER_REQUEST_DURATION: Final = "gen_ai.server.request.duration"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


def create_gen_ai_server_request_duration(meter: Meter) -> Histogram:
    """Generative AI server request duration such as time-to-last byte or last output token"""
    return meter.create_histogram(
        name=GEN_AI_SERVER_REQUEST_DURATION,
        description="Generative AI server request duration such as time-to-last byte or last output token.",
        unit="s",
    )


GEN_AI_SERVER_TIME_PER_OUTPUT_TOKEN: Final = (
    "gen_ai.server.time_per_output_token"
)
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


def create_gen_ai_server_time_per_output_token(meter: Meter) -> Histogram:
    """Time per output token generated after the first token for successful responses"""
    return meter.create_histogram(
        name=GEN_AI_SERVER_TIME_PER_OUTPUT_TOKEN,
        description="Time per output token generated after the first token for successful responses.",
        unit="s",
    )


GEN_AI_SERVER_TIME_TO_FIRST_TOKEN: Final = "gen_ai.server.time_to_first_token"
"""
Deprecated: Moved to the [OpenTelemetry GenAI semantic conventions repository](https://github.com/open-telemetry/semantic-conventions-genai).
"""


def create_gen_ai_server_time_to_first_token(meter: Meter) -> Histogram:
    """Time to generate first token for successful responses"""
    return meter.create_histogram(
        name=GEN_AI_SERVER_TIME_TO_FIRST_TOKEN,
        description="Time to generate first token for successful responses.",
        unit="s",
    )
