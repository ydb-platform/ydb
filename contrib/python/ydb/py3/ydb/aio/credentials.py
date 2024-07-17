import time

import abc
import asyncio
import logging
from ydb import issues, credentials

logger = logging.getLogger(__name__)


class _OneToManyValue(object):
    def __init__(self):
        self._value = None
        self._condition = asyncio.Condition()

    async def consume(self, timeout=3):
        async with self._condition:
            if self._value is None:
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=timeout)
                except Exception:
                    return self._value
            return self._value

    async def update(self, n_value):
        async with self._condition:
            prev_value = self._value
            self._value = n_value
            if prev_value is None:
                self._condition.notify_all()


class _AtMostOneExecution(object):
    def __init__(self):
        self._can_schedule = True
        self._lock = asyncio.Lock()  # Lock to guarantee only one execution

    async def _wrapped_execution(self, callback):
        await self._lock.acquire()
        try:
            res = callback()
            if asyncio.iscoroutine(res):
                await res
        except Exception:
            pass

        finally:
            self._lock.release()
            self._can_schedule = True

    def submit(self, callback):
        if self._can_schedule:
            self._can_schedule = False
            asyncio.ensure_future(self._wrapped_execution(callback))


class AbstractExpiringTokenCredentials(credentials.AbstractExpiringTokenCredentials):
    def __init__(self):
        super(AbstractExpiringTokenCredentials, self).__init__()
        self._tp = _AtMostOneExecution()
        self._cached_token = _OneToManyValue()

    @abc.abstractmethod
    async def _make_token_request(self):
        pass

    async def _refresh(self):
        current_time = time.time()
        self._log_refresh_start(current_time)

        try:
            auth_metadata = await self._make_token_request()
            await self._cached_token.update(auth_metadata["access_token"])
            self._update_expiration_info(auth_metadata)
            self.logger.info(
                "Token refresh successful. current_time %s, refresh_in %s",
                current_time,
                self._refresh_in,
            )

        except (KeyboardInterrupt, SystemExit):
            return

        except Exception as e:
            self.last_error = str(e)
            await asyncio.sleep(1)
            self._tp.submit(self._refresh)

        except BaseException as e:
            self.last_error = str(e)
            raise

    async def token(self):
        current_time = time.time()
        if current_time > self._refresh_in:
            self._tp.submit(self._refresh)

        cached_token = await self._cached_token.consume(timeout=3)
        if cached_token is None:
            if self.last_error is None:
                raise issues.ConnectionError(
                    "%s: timeout occurred while waiting for token.\n%s"
                    % (
                        self.__class__.__name__,
                        self.extra_error_message,
                    )
                )
            raise issues.ConnectionError(
                "%s: %s.\n%s" % (self.__class__.__name__, self.last_error, self.extra_error_message)
            )
        return cached_token

    async def auth_metadata(self):
        return [(credentials.YDB_AUTH_TICKET_HEADER, await self.token())]
