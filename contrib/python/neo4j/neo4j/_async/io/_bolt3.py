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

from enum import Enum
from logging import getLogger
from ssl import SSLSocket

from ... import _typing as t
from ..._exceptions import BoltProtocolError
from ..._io import BoltProtocolVersion
from ...api import READ_ACCESS
from ...exceptions import (
    ConfigurationError,
    DatabaseUnavailable,
    ForbiddenOnReadOnlyDatabase,
    Neo4jError,
    NotALeader,
    ServiceUnavailable,
)
from ._bolt import (
    AsyncBolt,
    ClientStateManagerBase,
    ServerStateManagerBase,
    tx_timeout_as_ms,
)
from ._common import (
    CommitResponse,
    InitResponse,
    ResetResponse,
    Response,
)


if t.TYPE_CHECKING:
    from ..._api import TelemetryAPI


log = getLogger("neo4j.io")


class BoltStates(Enum):
    CONNECTED = "CONNECTED"
    READY = "READY"
    STREAMING = "STREAMING"
    TX_READY_OR_TX_STREAMING = "TX_READY||TX_STREAMING"
    FAILED = "FAILED"


class ServerStateManager(ServerStateManagerBase):
    _STATE_TRANSITIONS: t.ClassVar[dict[Enum, dict[str, Enum]]] = {
        BoltStates.CONNECTED: {
            "hello": BoltStates.READY,
        },
        BoltStates.READY: {
            "run": BoltStates.STREAMING,
            "begin": BoltStates.TX_READY_OR_TX_STREAMING,
        },
        BoltStates.STREAMING: {
            "pull": BoltStates.READY,
            "discard": BoltStates.READY,
            "reset": BoltStates.READY,
        },
        BoltStates.TX_READY_OR_TX_STREAMING: {
            "commit": BoltStates.READY,
            "rollback": BoltStates.READY,
            "reset": BoltStates.READY,
        },
        BoltStates.FAILED: {
            "reset": BoltStates.READY,
        },
    }

    def __init__(self, init_state, on_change=None):
        self.state = init_state
        self._on_change = on_change

    def transition(self, message, metadata):
        if metadata.get("has_more"):
            return
        state_before = self.state
        self.state = self._STATE_TRANSITIONS.get(self.state, {}).get(
            message, self.state
        )
        if state_before != self.state and callable(self._on_change):
            self._on_change(state_before, self.state)

    def failed(self):
        return self.state == BoltStates.FAILED


class ClientStateManager(ClientStateManagerBase):
    _STATE_TRANSITIONS: t.ClassVar[dict[Enum, dict[str, Enum]]] = {
        BoltStates.CONNECTED: {
            "hello": BoltStates.READY,
        },
        BoltStates.READY: {
            "run": BoltStates.STREAMING,
            "begin": BoltStates.TX_READY_OR_TX_STREAMING,
        },
        BoltStates.STREAMING: {
            "begin": BoltStates.TX_READY_OR_TX_STREAMING,
            "reset": BoltStates.READY,
        },
        BoltStates.TX_READY_OR_TX_STREAMING: {
            "commit": BoltStates.READY,
            "rollback": BoltStates.READY,
            "reset": BoltStates.READY,
        },
    }

    def __init__(self, init_state, on_change=None):
        self.state = init_state
        self._on_change = on_change

    def transition(self, message):
        state_before = self.state
        self.state = self._STATE_TRANSITIONS.get(self.state, {}).get(
            message, self.state
        )
        if state_before != self.state and callable(self._on_change):
            self._on_change(state_before, self.state)


class AsyncBolt3(AsyncBolt):
    """
    Protocol handler for Bolt 3.

    This is supported by Neo4j versions 3.5, 4.0, 4.1, 4.2, 4.3, and 4.4.
    """

    PROTOCOL_VERSION = BoltProtocolVersion(3, 0)

    ssr_enabled = False

    supports_multiple_results = False

    supports_multiple_databases = False

    supports_re_auth = False

    supports_notification_filtering = False

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

    async def hello(self, dehydration_hooks=None, hydration_hooks=None):
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
        await self.send_all()
        await self.fetch_all()

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

    async def route(
        self,
        database=None,
        imp_user=None,
        bookmarks=None,
        dehydration_hooks=None,
        hydration_hooks=None,
    ):
        if database is not None:
            raise ConfigurationError(
                "Database name parameter for selecting database is not "
                f"supported in Bolt Protocol {self.PROTOCOL_VERSION}. "
                f"Database name {database!r}. "
                f"Server Agent {self.server_info.agent!r}"
            )
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

        # Ignoring database and bookmarks because there is no multi-db support.
        # The bookmarks are only relevant for making sure a previously created
        # db exists before querying a routing table for it.
        self.run(
            # This is an internal procedure call.
            # Only available if the Neo4j 3.5 is setup with clustering.
            "CALL dbms.cluster.routing.getRoutingTable($context)",
            {"context": self.routing_context},
            mode="r",  # Bolt Protocol Version(3, 0) supports mode="r"
            dehydration_hooks=dehydration_hooks,
            hydration_hooks=hydration_hooks,
            on_success=metadata.update,
        )
        self.pull(
            dehydration_hooks=None,
            hydration_hooks=None,
            on_success=metadata.update,
            on_records=records.extend,
        )
        await self.send_all()
        await self.fetch_all()
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
        if db is not None:
            raise ConfigurationError(
                "Database name parameter for selecting database is not "
                f"supported in Bolt Protocol {self.PROTOCOL_VERSION}. "
                f"Database name {db!r}."
            )
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
        # Just ignore n and qid, it is not supported in the Bolt 3 Protocol.
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        log.debug("[#%04X]  C: DISCARD_ALL", self.local_port)
        self._append(
            b"\x2f",
            (),
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
        # Just ignore n and qid, it is not supported in the Bolt 3 Protocol.
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        log.debug("[#%04X]  C: PULL_ALL", self.local_port)
        self._append(
            b"\x3f",
            (),
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
        if db is not None:
            raise ConfigurationError(
                "Database name parameter for selecting database is not "
                f"supported in Bolt Protocol {self.PROTOCOL_VERSION}. "
                f"Database name {db!r}."
            )
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

    async def reset(self, dehydration_hooks=None, hydration_hooks=None):
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
        await self.send_all()
        await self.fetch_all()

    def goodbye(self, dehydration_hooks=None, hydration_hooks=None):
        dehydration_hooks, hydration_hooks = self._default_hydration_hooks(
            dehydration_hooks, hydration_hooks
        )
        log.debug("[#%04X]  C: GOODBYE", self.local_port)
        self._append(b"\x02", (), dehydration_hooks=dehydration_hooks)

    async def _process_message(self, tag, fields):
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
            await self.responses[0].on_records(details)

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
            await response.on_success(summary_metadata or {})
        elif summary_signature == b"\x7e":
            log.debug("[#%04X]  S: IGNORED", self.local_port)
            await response.on_ignored(summary_metadata or {})
        elif summary_signature == b"\x7f":
            log.debug(
                "[#%04X]  S: FAILURE %r", self.local_port, summary_metadata
            )
            self._server_state_manager.state = BoltStates.FAILED
            try:
                await response.on_failure(summary_metadata or {})
            except (ServiceUnavailable, DatabaseUnavailable):
                if self.pool:
                    await self.pool.deactivate(address=self.unresolved_address)
                raise
            except (NotALeader, ForbiddenOnReadOnlyDatabase):
                if self.pool:
                    await self.pool.on_write_failure(
                        address=self.unresolved_address,
                        database=self.last_database,
                    )
                raise
            except Neo4jError as e:
                if self.pool:
                    await self.pool.on_neo4j_error(e, self)
                raise
        else:
            sig_int = ord(summary_signature)
            raise BoltProtocolError(
                f"Unexpected response message with signature {sig_int:02X}",
                self.unresolved_address,
            )

        return len(details), 1
