from random import Random
from typing import Callable

def ulid(random: Random, ms_timestamp_generator: Callable[[], int]) -> int:
    """Generate an integer ULID compatible with UUID v4.

    ULIDs as defined by the [spec](https://github.com/ulid/spec) look like this:

     01AN4Z07BY      79KA1307SR9X4MV3
    |----------|    |----------------|
     Timestamp         Randomness
     48bits            80bits

    In the future it would be nice to make this compatible with a UUID,
    e.g. v4 UUIDs by setting the version and variant bits correctly.
    We can't currently do this because setting these bits would leave us with only 7 bytes of randomness,
    which isn't enough for the Python SDK's sampler that currently expects 8 bytes of randomness.
    In the future OTEL will probably adopt https://www.w3.org/TR/trace-context-2/#random-trace-id-flag
    which relies only on the lower 7 bytes of the trace ID, then all SDKs and tooling should be updated
    and leaving only 7 bytes of randomness should be fine.

    Right now we only care about:
    - Our SDK / Python SDK's in general.
    - The OTEL collector.

    And both behave properly with 8 bytes of randomness because trace IDs were originally 64 bits
    so to be compatible with old trace IDs nothing in OTEL can assume >8 bytes of randomness in trace IDs
    unless they generated the trace ID themselves (e.g. the Go SDK _does_ expect >8 bytes of randomness internally).
    """
