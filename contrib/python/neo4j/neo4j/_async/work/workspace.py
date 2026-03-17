# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

import logging

from ... import _typing as t
from ..._async_compat.util import AsyncUtil
from ..._auth_management import to_auth_dict
from ..._conf import WorkspaceConfig
from ..._warnings import unclosed_resource_warn
from ...api import Bookmarks
from ...exceptions import (
    ServiceUnavailable,
    SessionError,
)
from .._debug import AsyncNonConcurrentMethodChecker
from ..io import (
    acquisition_timeout_to_deadline,
    AcquisitionAuth,
    AcquisitionDatabase,
)


if t.TYPE_CHECKING:
    from ..._deadline import Deadline
    from ...api import _TAuth
    from ...auth_management import (
        AsyncAuthManager,
        AuthManager,
    )
    from ..home_db_cache import (
        AsyncHomeDbCache,
        TKey,
    )
else:
    _TAuth = t.Any


log = logging.getLogger("neo4j")


class AsyncWorkspace(AsyncNonConcurrentMethodChecker):
    def __init__(self, pool, config):
        assert isinstance(config, WorkspaceConfig)
        self._pool = pool
        self._config = config
        self._connection = None
        self._connection_access_mode = None
        self._last_cache_key: TKey | None = None
        # Sessions are supposed to cache the database on which to operate.
        self._pinned_database = False
        self._bookmarks = ()
        self._initial_bookmarks = ()
        self._bookmark_manager = None
        self._last_from_bookmark_manager = None
        # Workspace has been closed.
        self._closed = False
        super().__init__()

    # Copy globals as function locals to make sure that they are available
    # during Python shutdown when the Session is destroyed.
    def __del__(
        self,
        _unclosed_resource_warn=unclosed_resource_warn,
    ):
        if self._closed:
            return
        _unclosed_resource_warn(self)

    async def __aenter__(self) -> AsyncWorkspace:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def _make_db_resolution_callback(
        self,
    ) -> t.Callable[[str | None], None] | None:
        if self._pinned_database:
            return None

        def _database_callback(database: str | None) -> None:
            self._set_pinned_database(database)
            if self._last_cache_key is None or database is None:
                return
            db_cache: AsyncHomeDbCache = self._pool.home_db_cache
            db_cache.set(self._last_cache_key, database)

        return _database_callback

    def _set_pinned_database(self, database):
        if self._pinned_database:
            return
        log.debug("[#0000]  _: <WORKSPACE> pinning database: %r", database)
        self._pinned_database = True
        self._config.database = database

    def _initialize_bookmarks(self, bookmarks):
        if bookmarks is None:
            prepared_bookmarks = ()
        elif isinstance(bookmarks, Bookmarks):
            prepared_bookmarks = tuple(bookmarks.raw_values)
        else:
            raise TypeError(
                "Bookmarks must be an instance of Bookmarks or None."
            )
        self._initial_bookmarks = self._bookmarks = prepared_bookmarks

    async def _get_bookmarks(self):
        if self._bookmark_manager is None:
            return self._bookmarks

        bmm = await AsyncUtil.callback(self._bookmark_manager.get_bookmarks)
        initial = self._initial_bookmarks
        self._last_from_bookmark_manager = tuple({*bmm, *initial})
        return self._last_from_bookmark_manager

    async def _update_bookmarks(self, new_bookmarks):
        if not new_bookmarks:
            return
        self._initial_bookmarks = ()
        self._bookmarks = new_bookmarks
        if self._bookmark_manager is None:
            return
        previous_bookmarks = self._last_from_bookmark_manager
        await AsyncUtil.callback(
            self._bookmark_manager.update_bookmarks,
            previous_bookmarks,
            new_bookmarks,
        )

    async def _update_bookmark(self, bookmark):
        if not bookmark:
            return
        await self._update_bookmarks((bookmark,))

    async def _connect(self, access_mode, auth=None, **acquire_kwargs) -> None:
        acquisition_timeout = self._config.connection_acquisition_timeout
        force_auth = acquire_kwargs.pop("force_auth", False)
        acquire_auth = AcquisitionAuth(auth, force_auth=force_auth)

        if self._connection:
            # TODO: Investigate this
            # log.warning("FIXME: should always disconnect before connect")
            await self._connection.send_all()
            await self._connection.fetch_all()
            await self._disconnect()

        acquisition_deadline = acquisition_timeout_to_deadline(
            acquisition_timeout
        )

        ssr_enabled = self._pool.ssr_enabled
        target_db = await self._get_routing_target_database(
            acquire_auth,
            ssr_enabled=ssr_enabled,
            acquisition_deadline=acquisition_deadline,
        )
        acquire_kwargs_ = {
            "access_mode": access_mode,
            "timeout": acquisition_deadline,
            "database": target_db,
            "bookmarks": await self._get_bookmarks(),
            "auth": acquire_auth,
            "liveness_check_timeout": None,
            "database_callback": self._make_db_resolution_callback(),
        }
        acquire_kwargs_.update(acquire_kwargs)
        self._connection = await self._pool.acquire(**acquire_kwargs_)
        if (
            target_db.guessed
            and not self._pinned_database
            and not self._connection.ssr_enabled
        ):
            # race condition: we now have created a connection which does not
            # support SSR.
            # => we need to fall back to explicit home database resolution
            log.debug(
                "[#0000]  _: <WORKSPACE> detected ssr support race; "
                "falling back to explicit home database resolution",
            )
            await self._disconnect()
            target_db = await self._get_routing_target_database(
                acquire_auth,
                ssr_enabled=False,
                acquisition_deadline=acquisition_deadline,
            )
            acquire_kwargs_["database"] = target_db
            self._connection = await self._pool.acquire(**acquire_kwargs_)
        self._connection_access_mode = access_mode

    async def _get_routing_target_database(
        self,
        acquire_auth: AcquisitionAuth,
        ssr_enabled: bool,
        acquisition_deadline: Deadline,
    ) -> AcquisitionDatabase:
        if (
            self._pinned_database
            or self._config.database is not None
            or self._pool.is_direct_pool
        ):
            log.debug(
                "[#0000]  _: <WORKSPACE> routing towards fixed database: %s",
                self._config.database,
            )
            self._set_pinned_database(self._config.database)
            return AcquisitionDatabase(self._config.database)

        auth = acquire_auth.auth
        resolved_auth = await self._resolve_session_auth(auth)
        db_cache: AsyncHomeDbCache = self._pool.home_db_cache
        cache_key = db_cache.compute_key(
            self._config.impersonated_user,
            resolved_auth,
        )
        self._last_cache_key = cache_key

        if ssr_enabled:
            cached_db = db_cache.get(cache_key)
            if cached_db is not None:
                log.debug(
                    (
                        "[#0000]  _: <WORKSPACE> routing towards cached "
                        "database: %s"
                    ),
                    cached_db,
                )
                return AcquisitionDatabase(cached_db, guessed=True)

        log.debug("[#0000]  _: <WORKSPACE> resolve home database")
        await self._pool.update_routing_table(
            database=self._config.database,
            imp_user=self._config.impersonated_user,
            bookmarks=await self._get_bookmarks(),
            auth=acquire_auth,
            acquisition_timeout=acquisition_deadline,
            database_callback=self._make_db_resolution_callback(),
        )
        return AcquisitionDatabase(self._config.database)

    @staticmethod
    async def _resolve_session_auth(
        auth: AsyncAuthManager | AuthManager | None,
    ) -> dict | None:
        if auth is None:
            return None
        # resolved_auth = await AsyncUtil.callback(auth.get_auth)
        # The above line breaks mypy
        # https://github.com/python/mypy/issues/15295
        auth_getter: t.Callable[[], _TAuth | t.Awaitable[_TAuth]] = (
            auth.get_auth
        )
        # so we enforce the right type here
        # (explicit type annotation above added as it's a necessary assumption
        #  for this cast to be correct)
        resolved_auth = t.cast(_TAuth, await AsyncUtil.callback(auth_getter))
        return to_auth_dict(resolved_auth)

    async def _disconnect(self, sync=False):
        self._last_cache_key = None
        if self._connection:
            if sync:
                try:
                    await self._connection.send_all()
                    await self._connection.fetch_all()
                except ServiceUnavailable:
                    pass
            if self._connection:
                await self._pool.release(self._connection)
                self._connection = None
            self._connection_access_mode = None

    @AsyncNonConcurrentMethodChecker._non_concurrent_method
    async def close(self) -> None:
        if self._closed:
            return
        await self._disconnect(sync=True)
        self._closed = True

    def closed(self) -> bool:
        """
        Indicate whether the session has been closed.

        :returns: :data:`True` if closed, :data:`False` otherwise.
        """
        return self._closed

    def _check_state(self):
        if self._closed:
            raise SessionError(self, "Session closed")
