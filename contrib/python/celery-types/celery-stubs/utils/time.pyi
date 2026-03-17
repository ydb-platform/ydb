class ffwd: ...

def get_exponential_backoff_interval(
    factor: int,
    retries: int,
    maximum: int,
    full_jitter: bool | None,
) -> int: ...
