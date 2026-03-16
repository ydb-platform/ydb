import time

# Polling for server-side events (such as job completion) uses exponential backoff for the sleep intervals between polls
ASYNC_POLL_MIN_INTERVAL = 0.5
ASYNC_POLL_MAX_INTERVAL = 30
ASYNC_POLL_BACKOFF_FACTOR = 1.4


class ExponentialBackoffTimer:
    def __init__(self, *, timeout=None):
        self.start_time = time.time()
        self.timeout = timeout
        self.current_sleep_interval = ASYNC_POLL_MIN_INTERVAL

    def sleep(self):
        max_sleep_time = ASYNC_POLL_MAX_INTERVAL
        if self.timeout is not None:
            elapsed = time.time() - self.start_time
            if elapsed >= self.timeout:
                raise TimeoutError(f"Timeout after {elapsed} seconds waiting for asynchronous event")
            remaining_time = self.timeout - elapsed
            # Usually, we would sleep for `ASYNC_POLL_MAX_INTERVAL`, but we don't want to sleep over the timeout
            max_sleep_time = min(ASYNC_POLL_MAX_INTERVAL, remaining_time)
            # We want to sleep at least for `ASYNC_POLL_MIN_INTERVAL`. This is important to ensure that, as we get
            # closer to the timeout, we don't accidentally wake up multiple times and hit the server in rapid succession
            # due to waking up to early from the `sleep`.
            max_sleep_time = max(max_sleep_time, ASYNC_POLL_MIN_INTERVAL)

        time.sleep(min(self.current_sleep_interval, max_sleep_time))
        self.current_sleep_interval *= ASYNC_POLL_BACKOFF_FACTOR
