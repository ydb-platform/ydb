"""
This module implements TdsSocket class
"""

from __future__ import annotations

import logging
import datetime

from . import tds_base
from . import tds_types
from . import tls
from .tds_base import PreLoginEnc, _TdsEnv, _TdsLogin, Route
from .row_strategies import list_row_strategy
from .smp import SmpManager

# _token_map is needed by sqlalchemy_pytds connector
from .tds_session import (
    _TdsSession,
)

logger = logging.getLogger(__name__)


class _TdsSocket:
    """
    This class represents root TDS connection
    if MARS is used it can have multiple sessions represented by _TdsSession class
    if MARS is not used it would have single _TdsSession instance
    """

    def __init__(
        self,
        sock: tds_base.TransportProtocol,
        login: _TdsLogin,
        tzinfo_factory: tds_types.TzInfoFactoryType | None = None,
        row_strategy=list_row_strategy,
        use_tz: datetime.tzinfo | None = None,
        autocommit=False,
        isolation_level=0,
    ):
        self._is_connected = False
        self.env = _TdsEnv()
        self.env.isolation_level = isolation_level
        self.collation = None
        self.tds72_transaction = 0
        self._mars_enabled = False
        self.sock = sock
        self.bufsize = login.blocksize
        self.use_tz = use_tz
        self.tds_version = login.tds_version
        self.type_factory = tds_types.SerializerFactory(self.tds_version)
        self._tzinfo_factory = tzinfo_factory
        self._smp_manager: SmpManager | None = None
        self._main_session = _TdsSession(
            tds=self,
            transport=sock,
            tzinfo_factory=tzinfo_factory,
            row_strategy=row_strategy,
            env=self.env,
            # initially we use fixed bufsize
            # it may be updated later if server specifies different block size
            bufsize=4096,
        )
        self._login = login
        self.route: Route | None = None
        self._row_strategy = row_strategy
        self.env.autocommit = autocommit
        self.query_timeout = login.query_timeout
        self.type_inferrer = tds_types.TdsTypeInferrer(
            type_factory=self.type_factory,
            collation=self.collation,
            bytes_to_unicode=self._login.bytes_to_unicode,
            allow_tz=not self.use_tz,
        )
        self.server_library_version = (0, 0)
        self.product_name = ""
        self.product_version = 0
        self.fedauth_required = False

    def __repr__(self) -> str:
        fmt = "<_TdsSocket tran={} mars={} tds_version={} use_tz={}>"
        return fmt.format(
            self.tds72_transaction, self._mars_enabled, self.tds_version, self.use_tz
        )

    def login(self) -> Route | None:
        self._login.server_enc_flag = PreLoginEnc.ENCRYPT_NOT_SUP
        if tds_base.IS_TDS71_PLUS(self._main_session):
            self._main_session.send_prelogin(self._login)
            self._main_session.process_prelogin(self._login)
        self._main_session.tds7_send_login(self._login)
        if self._login.server_enc_flag == PreLoginEnc.ENCRYPT_OFF:
            tls.revert_to_clear(self._main_session)
        self._main_session.begin_response()
        if not self._main_session.process_login_tokens():
            self._main_session.raise_db_exception()
        if self.route is not None:
            return self.route

        # update block size if server returned different one
        if (
            self._main_session._writer.bufsize
            != self._main_session._reader.get_block_size()
        ):
            self._main_session._reader.set_block_size(
                self._main_session._writer.bufsize
            )

        self.type_factory = tds_types.SerializerFactory(self.tds_version)
        self.type_inferrer = tds_types.TdsTypeInferrer(
            type_factory=self.type_factory,
            collation=self.collation,
            bytes_to_unicode=self._login.bytes_to_unicode,
            allow_tz=not self.use_tz,
        )
        if self._mars_enabled:
            self._smp_manager = SmpManager(self.sock)
            self._main_session = _TdsSession(
                tds=self,
                bufsize=self.bufsize,
                transport=self._smp_manager.create_session(),
                tzinfo_factory=self._tzinfo_factory,
                row_strategy=self._row_strategy,
                env=self.env,
            )
        self._is_connected = True
        q = []
        if self._login.database and self.env.database != self._login.database:
            q.append("use " + tds_base.tds_quote_id(self._login.database))
        if q:
            self._main_session.submit_plain_query("".join(q))
            self._main_session.process_simple_request()
        return None

    @property
    def mars_enabled(self) -> bool:
        return self._mars_enabled

    @property
    def main_session(self) -> _TdsSession:
        return self._main_session

    def create_session(self) -> _TdsSession:
        if not self._smp_manager:
            raise RuntimeError(
                "Calling create_session on a non-MARS connection does not work"
            )
        return _TdsSession(
            tds=self,
            transport=self._smp_manager.create_session(),
            tzinfo_factory=self._tzinfo_factory,
            row_strategy=self._row_strategy,
            bufsize=self.bufsize,
            env=self.env,
        )

    def is_connected(self) -> bool:
        return self._is_connected

    def close(self) -> None:
        self._is_connected = False
        if self.sock is not None:
            self.sock.close()
        if self._smp_manager:
            self._smp_manager.transport_closed()
        self._main_session.state = tds_base.TDS_DEAD
        if self._main_session.authentication:
            self._main_session.authentication.close()
            self._main_session.authentication = None

    def close_all_mars_sessions(self) -> None:
        if self._smp_manager:
            self._smp_manager.close_all_sessions(keep=self.main_session._transport)
