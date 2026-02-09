import abc
import asyncio
import logging
import time

from ydb import credentials
from ydb import issues

logger = logging.getLogger(__name__)
YDB_AUTH_TICKET_HEADER = "x-ydb-auth-ticket"


class AtMostOneExecution(object):
    def __init__(self):
        self._can_schedule = True
        self._lock = asyncio.Lock()

    async def wrapped_execution(self, callback):
        async with self._lock:
            try:
                await callback()
            except Exception:
                pass
            finally:
                self._can_schedule = True

    def submit(self, callback):
        if self._can_schedule:
            self._can_schedule = False
            asyncio.create_task(self.wrapped_execution(callback))


class AbstractExpiringTokenCredentials(credentials.AbstractExpiringTokenCredentials):
    def __init__(self):
        super(AbstractExpiringTokenCredentials, self).__init__()
        self._token_lock = asyncio.Lock()
        self._tp = AtMostOneExecution()

    @abc.abstractmethod
    async def _make_token_request(self):
        pass

    async def get_auth_token(self) -> str:  # type: ignore[override]
        for header, token in await self.auth_metadata():
            if header == YDB_AUTH_TICKET_HEADER:
                return token
        return ""

    async def _refresh_token(self, should_raise=False):
        current_time = time.time()

        try:
            self.logger.debug(
                "Refreshing token async, current_time: %s, expires_in: %s", current_time, self._expires_in
            )

            token_response = await self._make_token_request()
            self._update_token_info(token_response, current_time)

            self.logger.info("Token refreshed successfully async, expires_in: %s", self._expires_in)
            self.last_error = None

        except Exception as e:
            self.last_error = str(e)
            self.logger.error("Failed to refresh token async: %s", e)
            if should_raise:
                raise issues.ConnectionError(
                    "%s: %s.\n%s" % (self.__class__.__name__, self.last_error, self.extra_error_message)
                )

    async def token(self):
        if self._is_token_valid():
            if self._should_refresh():
                self._tp.submit(self._refresh_token)

            return self._cached_token

        async with self._token_lock:
            if self._is_token_valid():
                return self._cached_token

            await self._refresh_token(should_raise=True)

        return self._cached_token

    async def auth_metadata(self):
        return [(credentials.YDB_AUTH_TICKET_HEADER, await self.token())]
