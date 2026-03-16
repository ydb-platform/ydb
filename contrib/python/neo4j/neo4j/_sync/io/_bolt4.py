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


from logging import getLogger
from ssl import SSLSocket

from ..._api import TelemetryAPI
from ..._exceptions import BoltProtocolError
from ..._io import BoltProtocolVersion
from ...api import (
    READ_ACCESS,
    SYSTEM_DATABASE,
)
from ...exceptions import (
    ConfigurationError,
    DatabaseUnavailable,
    ForbiddenOnReadOnlyDatabase,
    Neo4jError,
    NotALeader,
    ServiceUnavailable,
)
from ._bolt import (
    Bolt,
    ClientStateManagerBase,
    ServerStateManagerBase,
    tx_timeout_as_ms,
)
from ._bolt3 import (
    BoltStates,
    ClientStateManager,
    ServerStateManager,
)
from ._common import (
    CommitResponse,
    InitResponse,
    ResetResponse,
    Response,
)


log = getLogger("neo4j.io")


class Bolt4x0(Bolt):
    """
    Protocol handler for Bolt 4.0.

    This is supported by Neo4j versions 4.0-4.4.
    """

    PROTOCOL_VERSION = BoltProtocolVersion(4, 0)

    ssr_enabled = False

    supports_multiple_results = True

    supports_multiple_databases = True

    supports_re_auth = False

    supports_notification_filtering = False

    SKIP_REGISTRATION = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._server_state_manager = ServerStateManager(
            BoltStates.CONNECTED, on_change=self._on_server_state_change
        )
        self._client_state_manager = ClientStateManager(
            BoltStates.CONNECTED, on_change=self._on_client_state_change
        )

    def _on_server_state_change(self, old_state, new_state):
        log.debug(
            "[#%04X]  _: <CONNECTION> server state: %s > %s",
            self.local_port,
            old_state.name,
            new_state.name,
        )

    def _get_server_state_manager(self) -> ServerStateManagerBase:
        return self._server_state_manager

    def _on_client_state_change(self, old_state, new_state):
        log.debug(
            "[#%04X]  _: <CONNECTION> client state: %s > %s",
            self.local_port,
            old_state.name,
            new_state.name,
        )

    def _get_client_state_manager(self) -> ClientStateManagerBase:
        return self._client_state_manager

    @property
    def is_reset(self):
        # We can't be sure of the server's state if there are still pending
        # responses. Unless the last message we sent was RESET. In that case
        # the server state will always be READY when we're done.
        if self.responses:
            return self.responses[-1] and self.responses[-1].message == "reset"
        return self._server_state_manager.state == BoltStates.READY

    @property
    def encrypted(self):
        return isinstance(self.socket, SSLSocket)

    @property
    def der_encoded_server_certificate(self):
        return self.socket.getpeercert(binary_form=True)

    def get_base_headers(self):
        return {
            "user_agent": self.user_agent,
        }

    def hello(self, dehydration_hooks=None, hydration_hooks=None):
        if (
            self.notifications_min_severity is not None
            or self.notifications_disabled_classifications is not None
        ):
            self.assert_notification_filtering_support()
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        headers = self.get_base_headers()
        headers.update(self.auth_dict)
        logged_headers = dict(headers)
        if "credentials" in logged_headers:
            logged_headers["credentials"] = "*******"
        log.debug("[#%04X]  C: HELLO %r", self.local_port, logged_headers)
        self._append(
            b"\x01",
            (headers,),
            response=InitResponse(
                self,
                "hello",
                hydration_hooks,
                on_success=self.server_info.update,
            ),
            dehydration_hooks=dehydration_hooks,
        )
        self.send_all()
        self.fetch_all()

    def logon(self, dehydration_hooks=None, hydration_hooks=None):
        """Append a LOGON message to the outgoing queue."""
        self.assert_re_auth_support()

    def logoff(self, dehydration_hooks=None, hydration_hooks=None):
        """Append a LOGOFF message to the outgoing queue."""
        self.assert_re_auth_support()

    def telemetry(
        self,
        api: TelemetryAPI,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ) -> None:
        # TELEMETRY not support by this protocol version, so we ignore it.
        pass

    def route(
        self,
        database=None,
        imp_user=None,
        bookmarks=None,
        dehydration_hooks=None,
        hydration_hooks=None,
    ):
        if imp_user is not None:
            raise ConfigurationError(
                "Impersonation is not supported in Bolt Protocol "
                f"{self.PROTOCOL_VERSION}. Trying to impersonate "
                f"{imp_user!r}."
            )
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        metadata = {}
        records = []

        if database is None:  # default database
            self.run(
                "CALL dbms.routing.getRoutingTable($context)",
                {"context": self.routing_context},
                mode="r",
                bookmarks=bookmarks,
                db=SYSTEM_DATABASE,
                on_success=metadata.update,
            )
        else:
            self.run(
                "CALL dbms.routing.getRoutingTable($context, $database)",
                {"context": self.routing_context, "database": database},
                mode="r",
                bookmarks=bookmarks,
                db=SYSTEM_DATABASE,
                on_success=metadata.update,
            )
        self.pull(
            dehydration_hooks=dehydration_hooks,
            hydration_hooks=hydration_hooks,
            on_success=metadata.update,
            on_records=records.extend,
        )
        self.send_all()
        self.fetch_all()
        return [
            dict(zip(metadata.get("fields", ()), values, strict=True))
            for values in records
        ]

    def run(
        self,
        query,
        parameters=None,
        mode=None,
        bookmarks=None,
        metadata=None,
        timeout=None,
        db=None,
        imp_user=None,
        notifications_min_severity=None,
        notifications_disabled_classifications=None,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ):
        if imp_user is not None:
            raise ConfigurationError(
                "Impersonation is not supported in Bolt Protocol "
                f"{self.PROTOCOL_VERSION}. Trying to impersonate "
                f"{imp_user!r}."
            )
        if (
            notifications_min_severity is not None
            or notifications_disabled_classifications is not None
        ):
            self.assert_notification_filtering_support()
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        if not parameters:
            parameters = {}
        extra = {}
        if mode in {READ_ACCESS, "r"}:
            # It will default to mode "w" if nothing is specified
            extra["mode"] = "r"
        if db:
            extra["db"] = db
        if (
            self._client_state_manager.state
            != BoltStates.TX_READY_OR_TX_STREAMING
        ):
            self.last_database = db
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError(
                    "Bookmarks must be provided within an iterable"
                ) from None
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError(
                    "Metadata must be coercible to a dict"
                ) from None
        if timeout is not None:
            extra["tx_timeout"] = tx_timeout_as_ms(timeout)
        fields = (query, parameters, extra)
        log.debug(
            "[#%04X]  C: RUN %s", self.local_port, " ".join(map(repr, fields))
        )
        self._append(
            b"\x10",
            fields,
            Response(self, "run", hydration_hooks, **handlers),
            dehydration_hooks=dehydration_hooks,
        )

    def discard(
        self,
        n=-1,
        qid=-1,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ):
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        extra = {"n": n}
        if qid != -1:
            extra["qid"] = qid
        log.debug("[#%04X]  C: DISCARD %r", self.local_port, extra)
        self._append(
            b"\x2f",
            (extra,),
            Response(self, "discard", hydration_hooks, **handlers),
            dehydration_hooks=dehydration_hooks,
        )

    def pull(
        self,
        n=-1,
        qid=-1,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ):
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        extra = {"n": n}
        if qid != -1:
            extra["qid"] = qid
        log.debug("[#%04X]  C: PULL %r", self.local_port, extra)
        self._append(
            b"\x3f",
            (extra,),
            Response(self, "pull", hydration_hooks, **handlers),
            dehydration_hooks=dehydration_hooks,
        )

    def begin(
        self,
        mode=None,
        bookmarks=None,
        metadata=None,
        timeout=None,
        db=None,
        imp_user=None,
        notifications_min_severity=None,
        notifications_disabled_classifications=None,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ):
        if imp_user is not None:
            raise ConfigurationError(
                "Impersonation is not supported in Bolt Protocol "
                f"{self.PROTOCOL_VERSION}. Trying to impersonate "
                f"{imp_user!r}."
            )
        if (
            notifications_min_severity is not None
            or notifications_disabled_classifications is not None
        ):
            self.assert_notification_filtering_support()
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        extra = {}
        if mode in {READ_ACCESS, "r"}:
            # It will default to mode "w" if nothing is specified
            extra["mode"] = "r"
        if db:
            extra["db"] = db
        self.last_database = db
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError(
                    "Bookmarks must be provided within an iterable"
                ) from None
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError(
                    "Metadata must be coercible to a dict"
                ) from None
        if timeout is not None:
            extra["tx_timeout"] = tx_timeout_as_ms(timeout)
        log.debug("[#%04X]  C: BEGIN %r", self.local_port, extra)
        self._append(
            b"\x11",
            (extra,),
            Response(self, "begin", hydration_hooks, **handlers),
            dehydration_hooks=dehydration_hooks,
        )

    def commit(self, dehydration_hooks=None, hydration_hooks=None, **handlers):
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        log.debug("[#%04X]  C: COMMIT", self.local_port)
        self._append(
            b"\x12",
            (),
            CommitResponse(self, "commit", hydration_hooks, **handlers),
            dehydration_hooks=dehydration_hooks,
        )

    def rollback(
        self, dehydration_hooks=None, hydration_hooks=None, **handlers
    ):
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        log.debug("[#%04X]  C: ROLLBACK", self.local_port)
        self._append(
            b"\x13",
            (),
            Response(self, "rollback", hydration_hooks, **handlers),
            dehydration_hooks=dehydration_hooks,
        )

    def reset(self, dehydration_hooks=None, hydration_hooks=None):
        """
        Reset the connection.

        Add a RESET message to the outgoing queue, send it and consume all
        remaining messages.
        """
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        log.debug("[#%04X]  C: RESET", self.local_port)
        response = ResetResponse(self, "reset", hydration_hooks)
        self._append(
            b"\x0f", response=response, dehydration_hooks=dehydration_hooks
        )
        self.send_all()
        self.fetch_all()

    def goodbye(self, dehydration_hooks=None, hydration_hooks=None):
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        log.debug("[#%04X]  C: GOODBYE", self.local_port)
        self._append(b"\x02", (), dehydration_hooks=dehydration_hooks)

    def _process_message(self, tag, fields):
        """
        Process at most one message from the server, if available.

        :returns: 2-tuple of number of detail messages and number of summary
                 messages fetched
        """
        details = []
        summary_signature = summary_metadata = None
        if tag == b"\x71":  # RECORD
            details = fields
        elif fields:
            summary_signature = tag
            summary_metadata = fields[0]
        else:
            summary_signature = tag

        if details:
            # Do not log any data
            log.debug("[#%04X]  S: RECORD * %d", self.local_port, len(details))
            self.responses[0].on_records(details)

        if summary_signature is None:
            return len(details), 0

        response = self.responses.popleft()
        response.complete = True
        if summary_signature == b"\x70":
            log.debug(
                "[#%04X]  S: SUCCESS %r", self.local_port, summary_metadata
            )
            self._server_state_manager.transition(
                response.message, summary_metadata
            )
            response.on_success(summary_metadata or {})
        elif summary_signature == b"\x7e":
            log.debug("[#%04X]  S: IGNORED", self.local_port)
            response.on_ignored(summary_metadata or {})
        elif summary_signature == b"\x7f":
            log.debug(
                "[#%04X]  S: FAILURE %r", self.local_port, summary_metadata
            )
            self._server_state_manager.state = BoltStates.FAILED
            try:
                response.on_failure(summary_metadata or {})
            except (ServiceUnavailable, DatabaseUnavailable):
                if self.pool:
                    self.pool.deactivate(address=self.unresolved_address)
                raise
            except (NotALeader, ForbiddenOnReadOnlyDatabase):
                if self.pool:
                    self.pool.on_write_failure(
                        address=self.unresolved_address,
                        database=self.last_database,
                    )
                raise
            except Neo4jError as e:
                if self.pool:
                    self.pool.on_neo4j_error(e, self)
                raise
        else:
            sig_int = ord(summary_signature)
            raise BoltProtocolError(
                f"Unexpected response message with signature {sig_int:02X}",
                self.unresolved_address,
            )

        return len(details), 1


class Bolt4x1(Bolt4x0):
    """
    Protocol handler for Bolt 4.1.

    This is supported by Neo4j versions 4.1 - 4.4.
    """

    PROTOCOL_VERSION = BoltProtocolVersion(4, 1)

    def get_base_headers(self):
        # Bolt 4.1 passes the routing context, originally taken from
        # the URI, into the connection initialisation message. This
        # enables server-side routing to propagate the same behaviour
        # through its driver.
        headers = {
            "user_agent": self.user_agent,
        }
        if self.routing_context is not None:
            headers["routing"] = self.routing_context
        return headers


class Bolt4x2(Bolt4x1):
    """
    Protocol handler for Bolt 4.2.

    This is supported by Neo4j version 4.2 - 4.4.
    """

    PROTOCOL_VERSION = BoltProtocolVersion(4, 2)

    SKIP_REGISTRATION = False


class Bolt4x3(Bolt4x2):
    """
    Protocol handler for Bolt 4.3.

    This is supported by Neo4j version 4.3 - 4.4.
    """

    PROTOCOL_VERSION = BoltProtocolVersion(4, 3)

    def get_base_headers(self):
        headers = super().get_base_headers()
        headers["patch_bolt"] = ["utc"]
        return headers

    def route(
        self,
        database=None,
        imp_user=None,
        bookmarks=None,
        dehydration_hooks=None,
        hydration_hooks=None,
    ):
        if imp_user is not None:
            raise ConfigurationError(
                "Impersonation is not supported in Bolt Protocol "
                f"{self.PROTOCOL_VERSION}. Trying to impersonate "
                f"{imp_user!r}."
            )
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )

        routing_context = self.routing_context or {}
        log.debug(
            "[#%04X]  C: ROUTE %r %r %r",
            self.local_port,
            routing_context,
            bookmarks,
            database,
        )
        metadata = {}
        bookmarks = [] if bookmarks is None else list(bookmarks)
        self._append(
            b"\x66",
            (routing_context, bookmarks, database),
            response=Response(
                self, "route", hydration_hooks, on_success=metadata.update
            ),
            dehydration_hooks=dehydration_hooks,
        )
        self.send_all()
        self.fetch_all()
        return [metadata.get("rt")]

    def hello(self, dehydration_hooks=None, hydration_hooks=None):
        if (
            self.notifications_min_severity is not None
            or self.notifications_disabled_classifications is not None
        ):
            self.assert_notification_filtering_support()
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )

        def on_success(metadata):
            self.connection_hints.update(metadata.pop("hints", {}))
            self.server_info.update(metadata)
            if "connection.recv_timeout_seconds" in self.connection_hints:
                recv_timeout = self.connection_hints[
                    "connection.recv_timeout_seconds"
                ]
                if isinstance(recv_timeout, int) and recv_timeout > 0:
                    self.socket.set_read_timeout(recv_timeout)
                else:
                    log.info(
                        "[#%04X]  _: <CONNECTION> Server supplied an "
                        "invalid value for "
                        "connection.recv_timeout_seconds (%r). Make sure "
                        "the server and network is set up correctly.",
                        self.local_port,
                        recv_timeout,
                    )
            self.patch = set(metadata.pop("patch_bolt", []))
            if "utc" in self.patch:
                self.hydration_handler.patch_utc()

        headers = self.get_base_headers()
        headers.update(self.auth_dict)
        logged_headers = dict(headers)
        if "credentials" in logged_headers:
            logged_headers["credentials"] = "*******"
        log.debug("[#%04X]  C: HELLO %r", self.local_port, logged_headers)
        self._append(
            b"\x01",
            (headers,),
            response=InitResponse(
                self, "hello", hydration_hooks, on_success=on_success
            ),
            dehydration_hooks=dehydration_hooks,
        )
        self.send_all()
        self.fetch_all()


class Bolt4x4(Bolt4x3):
    """
    Protocol handler for Bolt 4.4.

    This is supported by Neo4j version 4.4.
    """

    PROTOCOL_VERSION = BoltProtocolVersion(4, 4)

    def route(
        self,
        database=None,
        imp_user=None,
        bookmarks=None,
        dehydration_hooks=None,
        hydration_hooks=None,
    ):
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        routing_context = self.routing_context or {}
        db_context = {}
        if database is not None:
            db_context.update(db=database)
        if imp_user is not None:
            db_context.update(imp_user=imp_user)
        log.debug(
            "[#%04X]  C: ROUTE %r %r %r",
            self.local_port,
            routing_context,
            bookmarks,
            db_context,
        )
        metadata = {}
        bookmarks = [] if bookmarks is None else list(bookmarks)
        self._append(
            b"\x66",
            (routing_context, bookmarks, db_context),
            response=Response(
                self, "route", hydration_hooks, on_success=metadata.update
            ),
            dehydration_hooks=dehydration_hooks,
        )
        self.send_all()
        self.fetch_all()
        return [metadata.get("rt")]

    def run(
        self,
        query,
        parameters=None,
        mode=None,
        bookmarks=None,
        metadata=None,
        timeout=None,
        db=None,
        imp_user=None,
        notifications_min_severity=None,
        notifications_disabled_classifications=None,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ):
        if (
            notifications_min_severity is not None
            or notifications_disabled_classifications is not None
        ):
            self.assert_notification_filtering_support()
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        if not parameters:
            parameters = {}
        extra = {}
        if mode in {READ_ACCESS, "r"}:
            # It will default to mode "w" if nothing is specified
            extra["mode"] = "r"
        if db:
            extra["db"] = db
        if (
            self._client_state_manager.state
            != BoltStates.TX_READY_OR_TX_STREAMING
        ):
            self.last_database = db
        if imp_user:
            extra["imp_user"] = imp_user
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError(
                    "Bookmarks must be provided within an iterable"
                ) from None
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError(
                    "Metadata must be coercible to a dict"
                ) from None
        if timeout is not None:
            extra["tx_timeout"] = tx_timeout_as_ms(timeout)
        fields = (query, parameters, extra)
        log.debug(
            "[#%04X]  C: RUN %s", self.local_port, " ".join(map(repr, fields))
        )
        self._append(
            b"\x10",
            fields,
            Response(self, "run", hydration_hooks, **handlers),
            dehydration_hooks=dehydration_hooks,
        )

    def begin(
        self,
        mode=None,
        bookmarks=None,
        metadata=None,
        timeout=None,
        db=None,
        imp_user=None,
        notifications_min_severity=None,
        notifications_disabled_classifications=None,
        dehydration_hooks=None,
        hydration_hooks=None,
        **handlers,
    ):
        if (
            notifications_min_severity is not None
            or notifications_disabled_classifications is not None
        ):
            self.assert_notification_filtering_support()
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        extra = {}
        if mode in {READ_ACCESS, "r"}:
            # It will default to mode "w" if nothing is specified
            extra["mode"] = "r"
        if db:
            extra["db"] = db
        self.last_database = db
        if imp_user:
            extra["imp_user"] = imp_user
        if bookmarks:
            try:
                extra["bookmarks"] = list(bookmarks)
            except TypeError:
                raise TypeError(
                    "Bookmarks must be provided within an iterable"
                ) from None
        if metadata:
            try:
                extra["tx_metadata"] = dict(metadata)
            except TypeError:
                raise TypeError(
                    "Metadata must be coercible to a dict"
                ) from None
        if timeout is not None:
            extra["tx_timeout"] = tx_timeout_as_ms(timeout)
        log.debug("[#%04X]  C: BEGIN %r", self.local_port, extra)
        self._append(
            b"\x11",
            (extra,),
            Response(self, "begin", hydration_hooks, **handlers),
            dehydration_hooks=dehydration_hooks,
        )
