"""Optional workload oracle. SQL runs off the recovery/restore thread."""
import os
import threading
import time


class WorkloadProbe:
    def __init__(self, check, *, interval=5, max_age=30, clock=time.monotonic):
        self._check, self._interval, self._max_age, self._clock = check, interval, max_age, clock
        self._lock = threading.Lock()
        self._sample = None
        self._required_after = 0
        self._stop = threading.Event()
        self._thread = None

    def invalidate(self):
        with self._lock:
            self._required_after = self._clock()

    def tick(self):
        started = self._clock()
        error = None
        try:
            self._check()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"[:1000]
        with self._lock:
            self._sample = (started, self._clock(), error)

    def status(self, after=0):
        with self._lock:
            sample, required = self._sample, max(after, self._required_after)
        if sample is None:
            return {"status": "pending", "error": None, "age_sec": None}
        started, finished, error = sample
        age = self._clock() - finished
        state = ("pending" if started < required else "stale" if age > self._max_age
                 else "failed" if error is not None else "healthy")
        return {"status": state, "error": error, "age_sec": round(age, 1)}

    def start(self):
        def run():
            while not self._stop.is_set():
                self.tick()
                self._stop.wait(self._interval)
        self._thread = threading.Thread(target=run, name="sql-recovery", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)


def sql_check(endpoints, database, query, timeout):
    """Check a user-supplied read-only invariant, not just SELECT 1 connectivity.

    Each attempt uses current endpoints and a new SDK driver, avoiding a dead
    bootstrap pinned for the process lifetime. SDK discovery provides failover.
    """
    import ydb
    from ydb.convert import aggregate_result_sets_by_index
    auth_env = ("YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS", "YDB_USER", "YDB_ANONYMOUS_CREDENTIALS",
                "YDB_METADATA_CREDENTIALS", "YDB_ACCESS_TOKEN_CREDENTIALS", "YDB_OAUTH2_KEY_FILE")
    credentials = (ydb.credentials_from_env_variables() if any(os.getenv(key) for key in auth_env)
                   else ydb.AnonymousCredentials())
    attempt = 0

    def check():
        nonlocal attempt
        addresses = tuple(endpoints())
        if not addresses:
            raise RuntimeError("No SQL endpoints discovered")
        endpoint = addresses[attempt % len(addresses)]
        attempt += 1
        with ydb.Driver(ydb.DriverConfig(endpoint=endpoint, database=database,
                                         credentials=credentials)) as driver:
            driver.wait(timeout=timeout)
            with ydb.QuerySessionPool(driver, size=1) as pool:
                def read(session):
                    with session.transaction(ydb.QuerySnapshotReadOnly()) as tx:
                        return aggregate_result_sets_by_index(tx.execute(
                            query, commit_tx=True, settings=ydb.BaseRequestSettings().with_timeout(timeout)))
                result = pool.retry_operation_sync(read, retry_settings=ydb.RetrySettings(
                    max_retries=0, max_session_acquire_timeout=timeout))
                if len(result) != 1 or len(result[0].rows) != 1 or result[0].rows[0].get("ok") is not True:
                    raise RuntimeError("SQL recovery query must return exactly one row with ok=true")
    return check
