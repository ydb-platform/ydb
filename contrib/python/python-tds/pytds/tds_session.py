"""
This module implements TdsSession class
"""
from __future__ import annotations

import codecs
import collections.abc
import contextlib
import datetime
import struct
import typing
import warnings
from typing import Callable, Iterable, Any, List

from pytds import tds_base, tds_types
from pytds.collate import lcid2charset, raw_collation
from pytds.tds_base import (
    readall,
    skipall,
    PreLoginToken,
    PreLoginEnc,
    Message,
    logging_enabled,
    _create_exception_by_message,
    output,
    default,
    _TdsLogin,
    tds7_crypt_pass,
    logger,
    _Results,
    _TdsEnv,
)
from pytds.tds_reader import _TdsReader, ResponseMetadata
from pytds.tds_writer import _TdsWriter
from pytds.row_strategies import list_row_strategy, RowStrategy, RowGenerator
from pytds.fedauth import fedauth_packet

if typing.TYPE_CHECKING:
    from pytds.tds_socket import _TdsSocket


class _TdsSession:
    """TDS session

    This class has the following responsibilities:
    * Track state of a single TDS session if MARS enabled there could be multiple TDS sessions
      within one connection.
    * Provides API to send requests and receive responses
    * Does serialization of requests and deserialization of responses
    """

    def __init__(
        self,
        tds: _TdsSocket,
        transport: tds_base.TransportProtocol,
        tzinfo_factory: tds_types.TzInfoFactoryType | None,
        env: _TdsEnv,
        bufsize: int,
        row_strategy: RowStrategy = list_row_strategy,
    ):
        self.out_pos = 8
        self.res_info: _Results | None = None
        self.in_cancel = False
        self.wire_mtx = None
        self.param_info = None
        self.has_status = False
        self.ret_status: int | None = None
        self.skipped_to_status = False
        self._transport = transport
        self._reader = _TdsReader(
            transport=transport, bufsize=bufsize, tds_session=self
        )
        self._writer = _TdsWriter(
            transport=transport, bufsize=bufsize, tds_session=self
        )
        self.in_buf_max = 0
        self.state = tds_base.TDS_IDLE
        self._tds = tds
        self.messages: list[Message] = []
        self.rows_affected = -1
        self.use_tz = tds.use_tz
        self._spid = 0
        self.tzinfo_factory = tzinfo_factory
        self.more_rows = False
        self.done_flags = 0
        self.internal_sp_called = 0
        self.output_params: dict[int, tds_base.Column] = {}
        self.authentication: tds_base.AuthProtocol | None = None
        self.return_value_index = 0
        self._out_params_indexes: list[int] = []
        self.row: list[Any] | None = None
        self.end_marker = 0
        self._row_strategy = row_strategy
        self._env = env
        self._row_convertor: RowGenerator = list

    @property
    def autocommit(self):
        return self._env.autocommit

    @autocommit.setter
    def autocommit(self, value: bool):
        if self._env.autocommit != value:
            if value:
                if self._tds.tds72_transaction:
                    self.rollback(cont=False)
            else:
                self.begin_tran()
            self._env.autocommit = value

    @property
    def isolation_level(self):
        return self._env.isolation_level

    @isolation_level.setter
    def isolation_level(self, value: int):
        """
        Set transaction isolation level.
        Will roll back current transaction if it has different isolation level.
        """
        if self._env.isolation_level != value:
            if self._tds.tds72_transaction:
                # Setting cont=False to delay reopening of new transaction until
                # next command execution in case isolation_level changes again
                self.rollback(cont=False)
            self._env.isolation_level = value

    @property
    def row_strategy(self) -> Callable[[Iterable[str]], Callable[[Iterable[Any]], Any]]:
        return self._row_strategy

    @row_strategy.setter
    def row_strategy(
        self, value: Callable[[Iterable[str]], Callable[[Iterable[Any]], Any]]
    ) -> None:
        warnings.warn(
            "Changing row_strategy on live connection is now deprecated, you should set it when creating new connection",
            DeprecationWarning,
        )
        self._row_strategy = value

    def log_response_message(self, msg):
        # logging is disabled by default
        if logging_enabled:
            logger.info("[%d] %s", self._spid, msg)

    def __repr__(self):
        fmt = "<_TdsSession state={} tds={} messages={} rows_affected={} use_tz={} spid={} in_cancel={}>"
        res = fmt.format(
            repr(self.state),
            repr(self._tds),
            repr(self.messages),
            repr(self.rows_affected),
            repr(self.use_tz),
            repr(self._spid),
            self.in_cancel,
        )
        return res

    def raise_db_exception(self) -> None:
        """Raises exception from last server message

        This function will skip messages: The statement has been terminated
        """
        if not self.messages:
            raise tds_base.Error("Request failed, server didn't send error message")
        msg = None
        while True:
            msg = self.messages[-1]
            if msg["msgno"] == 3621:  # the statement has been terminated
                self.messages = self.messages[:-1]
            else:
                break

        error_msg = " ".join(m["message"] for m in self.messages)
        ex = _create_exception_by_message(msg, error_msg)
        raise ex

    def get_type_info(self, curcol):
        """Reads TYPE_INFO structure (http://msdn.microsoft.com/en-us/library/dd358284.aspx)

        :param curcol: An instance of :class:`Column` that will receive read information
        """
        r = self._reader
        # User defined data type of the column
        if tds_base.IS_TDS72_PLUS(self):
            user_type = r.get_uint()
        else:
            user_type = r.get_usmallint()
        curcol.column_usertype = user_type
        curcol.flags = r.get_usmallint()  # Flags
        type_id = r.get_byte()
        serializer_class = self._tds.type_factory.get_type_serializer(type_id)
        curcol.serializer = serializer_class.from_stream(r)

    def tds7_process_result(self):
        """Reads and processes COLMETADATA stream

        This stream contains a list of returned columns.
        Stream format link: http://msdn.microsoft.com/en-us/library/dd357363.aspx
        """
        self.log_response_message("got COLMETADATA")
        r = self._reader

        # read number of columns and allocate the columns structure

        num_cols = r.get_smallint()

        # This can be a DUMMY results token from a cursor fetch

        if num_cols == -1:
            return

        self.param_info = None
        self.has_status = False
        self.ret_status = None
        self.skipped_to_status = False
        self.rows_affected = tds_base.TDS_NO_COUNT
        self.more_rows = True
        self.row = [None] * num_cols
        self.res_info = info = _Results()

        #
        # loop through the columns populating COLINFO struct from
        # server response
        #
        header_tuple = []
        for col in range(num_cols):
            curcol = tds_base.Column()
            info.columns.append(curcol)
            self.get_type_info(curcol)

            curcol.column_name = r.read_ucs2(r.get_byte())
            precision = curcol.serializer.precision
            scale = curcol.serializer.scale
            size = curcol.serializer.size
            header_tuple.append(
                (
                    curcol.column_name,
                    curcol.serializer.get_typeid(),
                    None,
                    size,
                    precision,
                    scale,
                    curcol.flags & tds_base.Column.fNullable,
                )
            )
        info.description = tuple(header_tuple)
        self._setup_row_factory()
        return info

    def process_param(self):
        """Reads and processes RETURNVALUE stream.

        This stream is used to send OUTPUT parameters from RPC to client.
        Stream format url: http://msdn.microsoft.com/en-us/library/dd303881.aspx
        """
        self.log_response_message("got RETURNVALUE message")
        r = self._reader
        if tds_base.IS_TDS72_PLUS(self):
            ordinal = r.get_usmallint()
        else:
            r.get_usmallint()  # ignore size
            ordinal = self._out_params_indexes[self.return_value_index]
        name = r.read_ucs2(r.get_byte())
        r.get_byte()  # 1 - OUTPUT of sp, 2 - result of udf
        param = tds_base.Column()
        param.column_name = name
        self.get_type_info(param)
        param.value = param.serializer.read(r)
        self.output_params[ordinal] = param
        self.return_value_index += 1

    def process_cancel(self):
        """
        Process the incoming token stream until it finds
        an end token DONE with the cancel flag set.
        At that point the connection should be ready to handle a new query.

        In case when no cancel request is pending this function does nothing.
        """
        self.log_response_message("got CANCEL message")
        # silly cases, nothing to do
        if not self.in_cancel:
            return

        while True:
            while not self._reader.stream_finished():
                token_id = self.get_token_id()
                self.process_token(token_id)
                if not self.in_cancel:
                    return
            self.begin_response()

    def process_msg(self, marker: int) -> None:
        """Reads and processes ERROR/INFO streams

        Stream formats:

        - ERROR: http://msdn.microsoft.com/en-us/library/dd304156.aspx
        - INFO: http://msdn.microsoft.com/en-us/library/dd303398.aspx

        :param marker: TDS_ERROR_TOKEN or TDS_INFO_TOKEN
        """
        self.log_response_message("got ERROR/INFO message")
        r = self._reader
        r.get_smallint()  # size
        msg: Message = {
            "marker": marker,
            "msgno": r.get_int(),
            "state": r.get_byte(),
            "severity": r.get_byte(),
            "sql_state": None,
            "priv_msg_type": 0,
            "message": "",
            "server": "",
            "proc_name": "",
            "line_number": 0,
        }
        if marker == tds_base.TDS_INFO_TOKEN:
            msg["priv_msg_type"] = 0
        elif marker == tds_base.TDS_ERROR_TOKEN:
            msg["priv_msg_type"] = 1
        else:
            logger.error('tds_process_msg() called with unknown marker "%d"', marker)
        msg["message"] = r.read_ucs2(r.get_smallint())
        # server name
        msg["server"] = r.read_ucs2(r.get_byte())
        # stored proc name if available
        msg["proc_name"] = r.read_ucs2(r.get_byte())
        msg["line_number"] = (
            r.get_int() if tds_base.IS_TDS72_PLUS(self) else r.get_smallint()
        )
        # in case extended error data is sent, we just try to discard it

        # special case
        self.messages.append(msg)

    def process_row(self):
        """Reads and handles ROW stream.

        This stream contains list of values of one returned row.
        Stream format url: http://msdn.microsoft.com/en-us/library/dd357254.aspx
        """
        self.log_response_message("got ROW message")
        r = self._reader
        info = self.res_info
        info.row_count += 1
        for i, curcol in enumerate(info.columns):
            curcol.value = self.row[i] = curcol.serializer.read(r)

    def process_nbcrow(self):
        """Reads and handles NBCROW stream.

        This stream contains list of values of one returned row in a compressed way,
        introduced in TDS 7.3.B
        Stream format url: http://msdn.microsoft.com/en-us/library/dd304783.aspx
        """
        self.log_response_message("got NBCROW message")
        r = self._reader
        info = self.res_info
        if not info:
            self.bad_stream("got row without info")
        assert len(info.columns) > 0
        info.row_count += 1

        # reading bitarray for nulls, 1 represent null values for
        # corresponding fields
        nbc = readall(r, (len(info.columns) + 7) // 8)
        for i, curcol in enumerate(info.columns):
            if tds_base.my_ord(nbc[i // 8]) & (1 << (i % 8)):
                value = None
            else:
                value = curcol.serializer.read(r)
            self.row[i] = value

    def process_orderby(self):
        """Reads and processes ORDER stream

        Used to inform client by which column dataset is ordered.
        Stream format url: http://msdn.microsoft.com/en-us/library/dd303317.aspx
        """
        r = self._reader
        skipall(r, r.get_smallint())

    def process_end(self, marker):
        """Reads and processes DONE/DONEINPROC/DONEPROC streams

        Stream format urls:

        - DONE: http://msdn.microsoft.com/en-us/library/dd340421.aspx
        - DONEINPROC: http://msdn.microsoft.com/en-us/library/dd340553.aspx
        - DONEPROC: http://msdn.microsoft.com/en-us/library/dd340753.aspx

        :param marker: Can be TDS_DONE_TOKEN or TDS_DONEINPROC_TOKEN or TDS_DONEPROC_TOKEN
        """
        code_to_str = {
            tds_base.TDS_DONE_TOKEN: "DONE",
            tds_base.TDS_DONEINPROC_TOKEN: "DONEINPROC",
            tds_base.TDS_DONEPROC_TOKEN: "DONEPROC",
        }
        self.end_marker = marker
        self.more_rows = False
        r = self._reader
        status = r.get_usmallint()
        r.get_usmallint()  # cur_cmd
        more_results = status & tds_base.TDS_DONE_MORE_RESULTS != 0
        was_cancelled = status & tds_base.TDS_DONE_CANCELLED != 0
        done_count_valid = status & tds_base.TDS_DONE_COUNT != 0
        if self.res_info:
            self.res_info.more_results = more_results
        rows_affected = r.get_int8() if tds_base.IS_TDS72_PLUS(self) else r.get_int()
        self.log_response_message(
            "got {} message, more_res={}, cancelled={}, rows_affected={}".format(
                code_to_str[marker], more_results, was_cancelled, rows_affected
            )
        )
        if was_cancelled or (not more_results and not self.in_cancel):
            self.in_cancel = False
            self.set_state(tds_base.TDS_IDLE)
        if done_count_valid:
            self.rows_affected = rows_affected
        else:
            self.rows_affected = -1
        self.done_flags = status
        if (
            self.done_flags & tds_base.TDS_DONE_ERROR
            and not was_cancelled
            and not self.in_cancel
        ):
            self.raise_db_exception()

    def _ensure_transaction(self) -> None:
        if not self._env.autocommit and not self._tds.tds72_transaction:
            self.begin_tran()

    def process_env_chg(self):
        """Reads and processes ENVCHANGE stream.

        Stream info url: http://msdn.microsoft.com/en-us/library/dd303449.aspx
        """
        self.log_response_message("got ENVCHANGE message")
        r = self._reader
        size = r.get_smallint()
        type_id = r.get_byte()
        if type_id == tds_base.TDS_ENV_SQLCOLLATION:
            size = r.get_byte()
            self.conn.collation = r.get_collation()
            logger.info("switched collation to %s", self.conn.collation)
            skipall(r, size - 5)
            # discard old one
            skipall(r, r.get_byte())
        elif type_id == tds_base.TDS_ENV_BEGINTRANS:
            size = r.get_byte()
            assert size == 8
            self.conn.tds72_transaction = r.get_uint8()
            # old val, should be 0
            skipall(r, r.get_byte())
        elif (
            type_id == tds_base.TDS_ENV_COMMITTRANS
            or type_id == tds_base.TDS_ENV_ROLLBACKTRANS
        ):
            self.conn.tds72_transaction = 0
            # new val, should be 0
            skipall(r, r.get_byte())
            # old val, should have previous transaction id
            skipall(r, r.get_byte())
        elif type_id == tds_base.TDS_ENV_PACKSIZE:
            newval = r.read_ucs2(r.get_byte())
            r.read_ucs2(r.get_byte())
            new_block_size = int(newval)
            if new_block_size >= 512:
                # Is possible to have a shrink if server limits packet
                # size more than what we specified
                #
                # Reallocate buffer if possible (strange values from server or out of memory) use older buffer */
                self._writer.bufsize = new_block_size
        elif type_id == tds_base.TDS_ENV_DATABASE:
            newval = r.read_ucs2(r.get_byte())
            logger.info("switched to database %s", newval)
            r.read_ucs2(r.get_byte())
            self.conn.env.database = newval
        elif type_id == tds_base.TDS_ENV_LANG:
            newval = r.read_ucs2(r.get_byte())
            logger.info("switched language to %s", newval)
            r.read_ucs2(r.get_byte())
            self.conn.env.language = newval
        elif type_id == tds_base.TDS_ENV_CHARSET:
            newval = r.read_ucs2(r.get_byte())
            logger.info("switched charset to %s", newval)
            r.read_ucs2(r.get_byte())
            self.conn.env.charset = newval
            remap = {"iso_1": "iso8859-1"}
            self.conn.server_codec = codecs.lookup(remap.get(newval, newval))
        elif type_id == tds_base.TDS_ENV_DB_MIRRORING_PARTNER:
            newval = r.read_ucs2(r.get_byte())
            logger.info("got mirroring partner %s", newval)
            r.read_ucs2(r.get_byte())
        elif type_id == tds_base.TDS_ENV_LCID:
            lcid = int(r.read_ucs2(r.get_byte()))
            logger.info("switched lcid to %s", lcid)
            self.conn.server_codec = codecs.lookup(lcid2charset(lcid))
            r.read_ucs2(r.get_byte())
        elif type_id == tds_base.TDS_ENV_UNICODE_DATA_SORT_COMP_FLAGS:
            r.read_ucs2(r.get_byte())
            comp_flags = r.read_ucs2(r.get_byte())
            self.conn.comp_flags = comp_flags
        elif type_id == 20:
            # routing
            r.get_usmallint()
            protocol = r.get_byte()
            protocol_property = r.get_usmallint()
            alt_server = r.read_ucs2(r.get_usmallint())
            logger.info(
                "got routing info proto=%d proto_prop=%d alt_srv=%s",
                protocol,
                protocol_property,
                alt_server,
            )
            self.conn.route = {
                "server": alt_server,
                "port": protocol_property,
            }
            # OLDVALUE = 0x00, 0x00
            r.get_usmallint()
        else:
            logger.warning("unknown env type: %d, skipping", type_id)
            # discard byte values, not still supported
            skipall(r, size - 1)

    def process_auth(self) -> None:
        """Reads and processes SSPI stream.

        Stream info: http://msdn.microsoft.com/en-us/library/dd302844.aspx
        """
        r = self._reader
        w = self._writer
        pdu_size = r.get_smallint()
        if not self.authentication:
            raise tds_base.Error("Got unexpected token")
        packet = self.authentication.handle_next(readall(r, pdu_size))
        if packet:
            w.write(packet)
            w.flush()

    def is_connected(self) -> bool:
        """
        :return: True if transport is connected
        """
        return self._transport.is_connected()  # type: ignore # needs fixing

    def bad_stream(self, msg) -> None:
        """Called when input stream contains unexpected data.

        Will close stream and raise :class:`InterfaceError`
        :param msg: Message for InterfaceError exception.
        :return: Never returns, always raises exception.
        """
        self.close()
        raise tds_base.InterfaceError(msg)

    @property
    def tds_version(self) -> int:
        """Returns integer encoded current TDS protocol version"""
        return self._tds.tds_version

    @property
    def conn(self) -> _TdsSocket:
        """Reference to owning :class:`_TdsSocket`"""
        return self._tds

    def close(self) -> None:
        self._transport.close()

    def set_state(self, state: int) -> int:
        """Switches state of the TDS session.

        It also does state transitions checks.
        :param state: New state, one of TDS_PENDING/TDS_READING/TDS_IDLE/TDS_DEAD/TDS_QUERING
        """
        prior_state = self.state
        if state == prior_state:
            return state
        if state == tds_base.TDS_PENDING:
            if prior_state in (tds_base.TDS_READING, tds_base.TDS_QUERYING):
                self.state = tds_base.TDS_PENDING
            else:
                raise tds_base.InterfaceError(
                    "logic error: cannot chage query state from {0} to {1}".format(
                        tds_base.state_names[prior_state], tds_base.state_names[state]
                    )
                )
        elif state == tds_base.TDS_READING:
            # transition to READING are valid only from PENDING
            if self.state != tds_base.TDS_PENDING:
                raise tds_base.InterfaceError(
                    "logic error: cannot change query state from {0} to {1}".format(
                        tds_base.state_names[prior_state], tds_base.state_names[state]
                    )
                )
            else:
                self.state = state
        elif state == tds_base.TDS_IDLE:
            if prior_state == tds_base.TDS_DEAD:
                raise tds_base.InterfaceError(
                    "logic error: cannot change query state from {0} to {1}".format(
                        tds_base.state_names[prior_state], tds_base.state_names[state]
                    )
                )
            self.state = state
        elif state == tds_base.TDS_DEAD:
            self.state = state
        elif state == tds_base.TDS_QUERYING:
            if self.state == tds_base.TDS_DEAD:
                raise tds_base.InterfaceError(
                    "logic error: cannot change query state from {0} to {1}".format(
                        tds_base.state_names[prior_state], tds_base.state_names[state]
                    )
                )
            elif self.state != tds_base.TDS_IDLE:
                raise tds_base.InterfaceError(
                    "logic error: cannot change query state from {0} to {1}".format(
                        tds_base.state_names[prior_state], tds_base.state_names[state]
                    )
                )
            else:
                self.rows_affected = tds_base.TDS_NO_COUNT
                self.internal_sp_called = 0
                self.state = state
        else:
            assert False
        return self.state

    @contextlib.contextmanager
    def querying_context(self, packet_type: int) -> typing.Iterator[None]:
        """Context manager for querying.

        Sets state to TDS_QUERYING, and reverts it to TDS_IDLE if exception happens inside managed block,
        and to TDS_PENDING if managed block succeeds and flushes buffer.
        """
        if self.set_state(tds_base.TDS_QUERYING) != tds_base.TDS_QUERYING:
            raise tds_base.Error("Couldn't switch to state")
        self._writer.begin_packet(packet_type)
        try:
            yield
        except:
            if self.state != tds_base.TDS_DEAD:
                self.set_state(tds_base.TDS_IDLE)
            raise
        else:
            self.set_state(tds_base.TDS_PENDING)
            self._writer.flush()

    def make_param(self, name: str, value: Any) -> tds_base.Param:
        """Generates instance of :class:`Param` from value and name

        Value can also be of a special types:

        - An instance of :class:`Param`, in which case it is just returned.
        - An instance of :class:`output`, in which case parameter will become
          an output parameter.
        - A singleton :var:`default`, in which case default value will be passed
          into a stored proc.

        :param name: Name of the parameter, will populate column_name property of returned column.
        :param value: Value of the parameter, also used to guess the type of parameter.
        :return: An instance of :class:`Column`
        """
        if isinstance(value, tds_base.Param):
            value.name = name
            return value

        if isinstance(value, tds_base.Column):
            warnings.warn(
                "Usage of Column class as parameter is deprecated, use Param class instead",
                DeprecationWarning,
            )
            return tds_base.Param(
                name=name,
                type=value.type,
                value=value.value,
            )

        param_type = None
        param_flags = 0

        if isinstance(value, output):
            param_flags |= tds_base.fByRefValue
            if isinstance(value.type, str):
                param_type = tds_types.sql_type_by_declaration(value.type)
            elif value.type:
                param_type = self.conn.type_inferrer.from_class(value.type)
            value = value.value

        if value is default:
            param_flags |= tds_base.fDefaultValue
            value = None

        param_value = value
        if param_type is None:
            param_type = self.conn.type_inferrer.from_value(value)
        param = tds_base.Param(
            name=name, type=param_type, flags=param_flags, value=param_value
        )
        return param

    def _convert_params(
        self, parameters: dict[str, Any] | typing.Iterable[Any]
    ) -> List[tds_base.Param]:
        """Converts a dict of list of parameters into a list of :class:`Column` instances.

        :param parameters: Can be a list of parameter values, or a dict of parameter names to values.
        :return: A list of :class:`Column` instances.
        """
        if isinstance(parameters, dict):
            return [self.make_param(name, value) for name, value in parameters.items()]
        else:
            params = []
            for parameter in parameters:
                params.append(self.make_param("", parameter))
            return params

    def cancel_if_pending(self) -> None:
        """Cancels current pending request.

        Does nothing if no request is pending, otherwise sends cancel request,
        and waits for response.
        """
        if self.state == tds_base.TDS_IDLE:
            return
        if not self.in_cancel:
            self.put_cancel()
        self.process_cancel()

    def submit_rpc(
        self,
        rpc_name: tds_base.InternalProc | str,
        params: List[tds_base.Param],
        flags: int = 0,
    ) -> None:
        """Sends an RPC request.

        This call will transition session into pending state.
        If some operation is currently pending on the session, it will be
        cancelled before sending this request.

        Spec: http://msdn.microsoft.com/en-us/library/dd357576.aspx

        :param rpc_name: Name of the RPC to call, can be an instance of :class:`InternalProc`
        :param params: Stored proc parameters, should be a list of :class:`Column` instances.
        :param flags: See spec for possible flags.
        """
        logger.info("Sending RPC %s flags=%d", rpc_name, flags)
        self.messages = []
        self.output_params = {}
        self.cancel_if_pending()
        self.res_info = None
        w = self._writer
        with self.querying_context(tds_base.PacketType.RPC):
            if tds_base.IS_TDS72_PLUS(self):
                self._start_query()
            if tds_base.IS_TDS71_PLUS(self) and isinstance(
                rpc_name, tds_base.InternalProc
            ):
                w.put_smallint(-1)
                w.put_smallint(rpc_name.proc_id)
            else:
                if isinstance(rpc_name, tds_base.InternalProc):
                    proc_name = rpc_name.name
                else:
                    proc_name = rpc_name
                w.put_smallint(len(proc_name))
                w.write_ucs2(proc_name)
            #
            # TODO support flags
            # bit 0 (1 as flag) in TDS7/TDS5 is "recompile"
            # bit 1 (2 as flag) in TDS7+ is "no metadata" bit this will prevent sending of column infos
            #
            w.put_usmallint(flags)
            self._out_params_indexes = []
            for i, param in enumerate(params):
                if param.flags & tds_base.fByRefValue:
                    self._out_params_indexes.append(i)
                w.put_byte(len(param.name))
                w.write_ucs2(param.name)
                #
                # TODO support other flags (use defaul null/no metadata)
                # bit 1 (2 as flag) in TDS7+ is "default value" bit
                # (what's the meaning of "default value" ?)
                #
                w.put_byte(param.flags)

                # TYPE_INFO structure: https://msdn.microsoft.com/en-us/library/dd358284.aspx
                serializer = self._tds.type_factory.serializer_by_type(
                    sql_type=param.type, collation=self._tds.collation or raw_collation
                )
                type_id = serializer.type
                w.put_byte(type_id)
                serializer.write_info(w)

                serializer.write(w, param.value)

    def _setup_row_factory(self) -> None:
        self._row_convertor = list
        if self.res_info:
            column_names = [col[0] for col in self.res_info.description]
            self._row_convertor = self._row_strategy(column_names)

    def callproc(
        self,
        procname: tds_base.InternalProc | str,
        parameters: dict[str, Any] | typing.Iterable[Any],
    ) -> list[Any]:
        self._ensure_transaction()
        results = list(parameters)
        conv_parameters = self._convert_params(parameters)
        self.submit_rpc(procname, conv_parameters, 0)
        self.begin_response()
        self.process_rpc()
        for key, param in self.output_params.items():
            results[key] = param.value
        return results

    def get_proc_outputs(self) -> list[Any]:
        """
        If stored procedure has result sets and OUTPUT parameters use this method
        after you processed all result sets to get values of the OUTPUT parameters.
        :return: A list of output parameter values.
        """

        self.complete_rpc()
        results = [None] * len(self.output_params.items())
        for key, param in self.output_params.items():
            results[key] = param.value
        return results

    def get_proc_return_status(self) -> int | None:
        """Last executed stored procedure's return value

        Returns integer value returned by `RETURN` statement from last executed stored procedure.
        If no value was not returned or no stored procedure was executed return `None`.
        """
        if not self.has_status:
            self.find_return_status()
        return self.ret_status if self.has_status else None

    def executemany(
        self,
        operation: str,
        params_seq: Iterable[list[Any] | tuple[Any, ...] | dict[str, Any]],
    ) -> None:
        """
        Execute same SQL query multiple times for each parameter set in the `params_seq` list.
        """
        counts = []
        for params in params_seq:
            self.execute(operation, params)
            if self.rows_affected != -1:
                counts.append(self.rows_affected)
        if counts:
            self.rows_affected = sum(counts)

    def execute(
        self,
        operation: str,
        params: list[Any] | tuple[Any, ...] | dict[str, Any] | None = None,
    ) -> None:
        self._ensure_transaction()
        if params is not None:
            named_params = {}
            if isinstance(params, (list, tuple)):
                names = []
                pid = 1
                for val in params:
                    if val is None:
                        names.append("NULL")
                    else:
                        name = f"@P{pid}"
                        names.append(name)
                        named_params[name] = val
                        pid += 1
                if len(names) == 1:
                    operation = operation % names[0]
                else:
                    operation = operation % tuple(names)
            elif isinstance(params, dict):
                # rename parameters
                rename: dict[str, Any] = {}
                pid = 1
                for name, value in params.items():
                    if value is None:
                        rename[name] = "NULL"
                    else:
                        mssql_name = f"@P{pid}"
                        rename[name] = mssql_name
                        named_params[mssql_name] = value
                        pid += 1
                operation = operation % rename
            if named_params:
                list_named_params = self._convert_params(named_params)
                param_definition = ",".join(
                    f"{p.name} {p.type.get_declaration()}" for p in list_named_params
                )
                self.submit_rpc(
                    tds_base.SP_EXECUTESQL,
                    [
                        self.make_param("", operation),
                        self.make_param("", param_definition),
                    ]
                    + list_named_params,
                    0,
                )
            else:
                self.submit_plain_query(operation)
        else:
            self.submit_plain_query(operation)
        self.begin_response()
        self.find_result_or_done()

    def execute_scalar(
        self,
        query_string: str,
        params: list[Any] | tuple[Any, ...] | dict[str, Any] | None = None,
    ) -> Any:
        """
        This method executes SQL query then returns first column of first row or the
        result.

        Query can be parametrized, see :func:`execute` method for details.

        This method is useful if you want just a single value, as in:

        .. code-block::

           conn.execute_scalar('SELECT COUNT(*) FROM employees')

        This method works in the same way as ``iter(conn).next()[0]``.
        Remaining rows, if any, can still be iterated after calling this
        method.
        """
        self.execute(operation=query_string, params=params)
        row = self._fetchone()
        if not row:
            return None
        return row[0]

    def submit_plain_query(self, operation: str) -> None:
        """Sends a plain query to server.

        This call will transition session into pending state.
        If some operation is currently pending on the session, it will be
        cancelled before sending this request.

        Spec: http://msdn.microsoft.com/en-us/library/dd358575.aspx

        :param operation: A string representing sql statement.
        """
        self.messages = []
        self.cancel_if_pending()
        self.res_info = None
        logger.info("Sending query %s", operation[:100])
        w = self._writer
        with self.querying_context(tds_base.PacketType.QUERY):
            if tds_base.IS_TDS72_PLUS(self):
                self._start_query()
            w.write_ucs2(operation)

    def submit_bulk(
        self,
        metadata: list[tds_base.Column],
        rows: Iterable[collections.abc.Sequence[Any]],
    ) -> None:
        """Sends insert bulk command.

        Spec: http://msdn.microsoft.com/en-us/library/dd358082.aspx

        :param metadata: A list of :class:`Column` instances.
        :param rows: A collection of rows, each row is a collection of values.
        :return:
        """
        logger.info("Sending INSERT BULK")
        num_cols = len(metadata)
        w = self._writer
        serializers = []
        with self.querying_context(tds_base.PacketType.BULK):
            w.put_byte(tds_base.TDS7_RESULT_TOKEN)
            w.put_usmallint(num_cols)
            for col in metadata:
                if tds_base.IS_TDS72_PLUS(self):
                    w.put_uint(col.column_usertype)
                else:
                    w.put_usmallint(col.column_usertype)
                w.put_usmallint(col.flags)
                serializer = col.choose_serializer(
                    type_factory=self._tds.type_factory,
                    collation=self._tds.collation,
                )
                type_id = serializer.type
                w.put_byte(type_id)
                serializers.append(serializer)
                serializer.write_info(w)
                w.put_byte(len(col.column_name))
                w.write_ucs2(col.column_name)
            for row in rows:
                w.put_byte(tds_base.TDS_ROW_TOKEN)
                for i, col in enumerate(metadata):
                    serializers[i].write(w, row[i])

            # https://msdn.microsoft.com/en-us/library/dd340421.aspx
            w.put_byte(tds_base.TDS_DONE_TOKEN)
            w.put_usmallint(tds_base.TDS_DONE_FINAL)
            w.put_usmallint(0)  # curcmd
            # row count
            if tds_base.IS_TDS72_PLUS(self):
                w.put_int8(0)
            else:
                w.put_int(0)

    def put_cancel(self) -> None:
        """Sends a cancel request to the server.

        Switches connection to IN_CANCEL state.
        """
        logger.info("Sending CANCEL")
        self._writer.begin_packet(tds_base.PacketType.CANCEL)
        self._writer.flush()
        self.in_cancel = True

    _begin_tran_struct_72 = struct.Struct("<HBB")

    def begin_tran(self) -> None:
        logger.info("Sending BEGIN TRAN il=%x", self._env.isolation_level)
        self.submit_begin_tran(isolation_level=self._env.isolation_level)
        self.process_simple_request()

    def submit_begin_tran(self, isolation_level: int = 0) -> None:
        if tds_base.IS_TDS72_PLUS(self):
            self.messages = []
            self.cancel_if_pending()
            w = self._writer
            with self.querying_context(tds_base.PacketType.TRANS):
                self._start_query()
                w.pack(
                    self._begin_tran_struct_72,
                    5,  # TM_BEGIN_XACT
                    isolation_level,
                    0,  # new transaction name
                )
        else:
            self.submit_plain_query("BEGIN TRANSACTION")
            self.conn.tds72_transaction = 1

    _commit_rollback_tran_struct72_hdr = struct.Struct("<HBB")
    _continue_tran_struct72 = struct.Struct("<BB")

    def rollback(self, cont: bool) -> None:
        """
        Rollback current transaction if it exists.
        If `cont` parameter is set to true, new transaction will start immediately
        after current transaction is rolled back
        """
        if self._env.autocommit:
            return

        # if not self._conn or not self._conn.is_connected():
        #    return

        if not self._tds.tds72_transaction:
            return
        logger.info("Sending ROLLBACK TRAN")
        self.submit_rollback(cont, isolation_level=self._env.isolation_level)
        prev_timeout = self._tds.sock.gettimeout()
        self._tds.sock.settimeout(None)
        try:
            self.process_simple_request()
        finally:
            self._tds.sock.settimeout(prev_timeout)

    def submit_rollback(self, cont: bool, isolation_level: int = 0) -> None:
        """
        Send transaction rollback request.
        If `cont` parameter is set to true, new transaction will start immediately
        after current transaction is rolled back
        """
        if tds_base.IS_TDS72_PLUS(self):
            self.messages = []
            self.cancel_if_pending()
            w = self._writer
            with self.querying_context(tds_base.PacketType.TRANS):
                self._start_query()
                flags = 0
                if cont:
                    flags |= 1
                w.pack(
                    self._commit_rollback_tran_struct72_hdr,
                    8,  # TM_ROLLBACK_XACT
                    0,  # transaction name
                    flags,
                )
                if cont:
                    w.pack(
                        self._continue_tran_struct72,
                        isolation_level,
                        0,  # new transaction name
                    )
        else:
            self.submit_plain_query(
                "IF @@TRANCOUNT > 0 ROLLBACK BEGIN TRANSACTION"
                if cont
                else "IF @@TRANCOUNT > 0 ROLLBACK"
            )
            self.conn.tds72_transaction = 1 if cont else 0

    def commit(self, cont: bool) -> None:
        if self._env.autocommit:
            return
        if not self._tds.tds72_transaction:
            return
        logger.info("Sending COMMIT TRAN")
        self.submit_commit(cont, isolation_level=self._env.isolation_level)
        prev_timeout = self._tds.sock.gettimeout()
        self._tds.sock.settimeout(None)
        try:
            self.process_simple_request()
        finally:
            self._tds.sock.settimeout(prev_timeout)

    def submit_commit(self, cont: bool, isolation_level: int = 0) -> None:
        if tds_base.IS_TDS72_PLUS(self):
            self.messages = []
            self.cancel_if_pending()
            w = self._writer
            with self.querying_context(tds_base.PacketType.TRANS):
                self._start_query()
                flags = 0
                if cont:
                    flags |= 1
                w.pack(
                    self._commit_rollback_tran_struct72_hdr,
                    7,  # TM_COMMIT_XACT
                    0,  # transaction name
                    flags,
                )
                if cont:
                    w.pack(
                        self._continue_tran_struct72,
                        isolation_level,
                        0,  # new transaction name
                    )
        else:
            self.submit_plain_query(
                "IF @@TRANCOUNT > 0 COMMIT BEGIN TRANSACTION"
                if cont
                else "IF @@TRANCOUNT > 0 COMMIT"
            )
            self.conn.tds72_transaction = 1 if cont else 0

    _tds72_query_start = struct.Struct("<IIHQI")

    def _start_query(self) -> None:
        w = self._writer
        w.pack(
            _TdsSession._tds72_query_start,
            0x16,  # total length
            0x12,  # length
            2,  # type
            self.conn.tds72_transaction,
            1,  # request count
        )

    def send_prelogin(self, login: _TdsLogin) -> None:
        from . import intversion

        # https://msdn.microsoft.com/en-us/library/dd357559.aspx
        instance_name = login.instance_name or "MSSQLServer"
        instance_name_encoded = instance_name.encode("ascii")
        if len(instance_name_encoded) > 65490:
            raise ValueError("Instance name is too long")
        prelogin_fields = {PreLoginToken.VERSION: struct.pack(">LH", intversion, 0), # intversion, build number
                           PreLoginToken.ENCRYPTION: struct.pack("B", login.enc_flag),
                           PreLoginToken.INSTOPT: instance_name_encoded + b"\x00", # zero terminate instance_name
                           PreLoginToken.THREADID: struct.pack(">L", 0)} # TODO: change this to thread id        
        attribs: dict[str, str | int | bool] = {
            "lib_ver": f"{intversion:x}",
            "enc_flag": f"{login.enc_flag:x}",
            "inst_name": instance_name,
        }
        if tds_base.IS_TDS72_PLUS(self):
            # MARS (1 enabled)
            prelogin_fields[PreLoginToken.MARS] = b'\x01' if login.use_mars else b'\x00'
            attribs["mars"] = login.use_mars
        if tds_base.IS_TDS74_PLUS(self):
            if login.access_token:
                prelogin_fields[PreLoginToken.FEDAUTHREQUIRED] = b'\x01'
                attribs["fedauth"] = bool(login.access_token)

        w = self._writer
        w.begin_packet(tds_base.PacketType.PRELOGIN)
        start_pos = 5*len(prelogin_fields) + 1

        for field, value in prelogin_fields.items():
            value_len = len(value)
            buf = struct.pack(">BHH", field, start_pos, value_len)
            w.write(buf)
            start_pos += value_len

        w.write(struct.pack("B",PreLoginToken.TERMINATOR))

        for value in prelogin_fields.values():
            w.write(value)

        logger.info(
            "Sending PRELOGIN %s", " ".join(f"{n}={v!r}" for n, v in attribs.items())
        )

        w.flush()

    def begin_response(self) -> ResponseMetadata:
        """Begins reading next response from server.

        If timeout happens during reading of first packet will
        send cancellation message.
        """
        try:
            return self._reader.begin_response()
        except tds_base.TimeoutError:
            self.put_cancel()
            raise

    def process_prelogin(self, login: _TdsLogin) -> None:
        # https://msdn.microsoft.com/en-us/library/dd357559.aspx
        resp_header = self.begin_response()
        p = self._reader.read_whole_packet()
        size = len(p)
        if size <= 0 or resp_header.type != tds_base.PacketType.REPLY:
            self.bad_stream(
                "Invalid packet type: {0}, expected REPLY(4)".format(
                    self._reader.packet_type
                )
            )
        self.parse_prelogin(octets=p, login=login)

    def parse_prelogin(self, octets: bytes, login: _TdsLogin) -> None:
        from . import tls

        # https://msdn.microsoft.com/en-us/library/dd357559.aspx
        size = len(octets)
        p = octets
        # default 2, no certificate, no encryptption
        crypt_flag = 2
        i = 0
        byte_struct = struct.Struct("B")
        off_len_struct = struct.Struct(">HH")
        prod_version_struct = struct.Struct(">LH")
        while True:
            if i >= size:
                self.bad_stream("Invalid size of PRELOGIN structure")
            (type_id,) = byte_struct.unpack_from(p, i)
            if type_id == PreLoginToken.TERMINATOR:
                break
            if i + 4 > size:
                self.bad_stream("Invalid size of PRELOGIN structure")
            off, length = off_len_struct.unpack_from(p, i + 1)
            if off > size or off + length > size:
                self.bad_stream("Invalid offset in PRELOGIN structure")
            if type_id == PreLoginToken.VERSION:
                self.conn.server_library_version = prod_version_struct.unpack_from(
                    p, off
                )
            elif type_id == PreLoginToken.ENCRYPTION and length >= 1:
                (crypt_flag,) = byte_struct.unpack_from(p, off)
            elif type_id == PreLoginToken.MARS:
                self.conn._mars_enabled = bool(byte_struct.unpack_from(p, off)[0])
            elif type_id == PreLoginToken.INSTOPT:
                # ignore instance name mismatch
                pass
            elif type_id == PreLoginToken.FEDAUTHREQUIRED:
                self.conn.fedauth_required = bool(byte_struct.unpack_from(p,off)[0])
            elif type_id == PreLoginToken.NONCEOPT:
                login.nonce = p[off : off + length]
            i += 5
        logger.info(
            "Got PRELOGIN response crypt=%x mars=%d",
            crypt_flag,
            self.conn._mars_enabled,
        )
        # if server do not has certificate do normal login
        login.server_enc_flag = crypt_flag
        if crypt_flag == PreLoginEnc.ENCRYPT_OFF:
            if login.enc_flag == PreLoginEnc.ENCRYPT_ON:
                self.bad_stream("Server returned unexpected ENCRYPT_ON value")
            else:
                # encrypt login packet only
                tls.establish_channel(self)
        elif crypt_flag == PreLoginEnc.ENCRYPT_ON:
            # encrypt entire connection
            tls.establish_channel(self)
        elif crypt_flag == PreLoginEnc.ENCRYPT_REQ:
            if login.enc_flag == PreLoginEnc.ENCRYPT_NOT_SUP:
                # connection terminated by server and client
                raise tds_base.Error(
                    "Client does not have encryption enabled but it is required by server, "
                    "enable encryption and try connecting again"
                )
            else:
                # encrypt entire connection
                tls.establish_channel(self)
        elif crypt_flag == PreLoginEnc.ENCRYPT_NOT_SUP:
            if login.enc_flag == PreLoginEnc.ENCRYPT_ON:
                # connection terminated by server and client
                raise tds_base.Error(
                    "You requested encryption but it is not supported by server"
                )
            # do not encrypt anything
        else:
            self.bad_stream(
                "Unexpected value of enc_flag returned by server: {}".format(crypt_flag)
            )

    def tds7_send_login(self, login: _TdsLogin) -> None:
        # https://msdn.microsoft.com/en-us/library/dd304019.aspx
        self.validate_login(login)

        w = self._writer
        w.begin_packet(tds_base.PacketType.LOGIN)
        current_pos = 86 + 8 if tds_base.IS_TDS72_PLUS(self) else 86
        packet_size = (
            current_pos
            + (
                len(login.client_host_name)
                + len(login.app_name)
                + len(login.server_name)
                + len(login.library)
                + len(login.language)
                + len(login.database)
            ) * 2
        )
        self.authentication = None
        if login.auth:
            self.authentication = login.auth
            auth_packet = login.auth.create_packet()
            ext_packet = b""
            packet_size += len(auth_packet)
        elif login.access_token:
            auth_packet = b""
            ext_packet = fedauth_packet(login, self.conn.fedauth_required)
            packet_size += len(ext_packet) + 4
        else:
            auth_packet = b""
            ext_packet = b""
            packet_size += (len(login.user_name) + len(login.password)) * 2

        w.put_int(packet_size)
        w.put_uint(login.tds_version)
        w.put_int(login.blocksize)
        from . import intversion

        w.put_uint(intversion)
        w.put_int(login.pid)
        w.put_uint(0)  # connection id
        option_flag1 = (
            tds_base.TDS_SET_LANG_ON
            | tds_base.TDS_USE_DB_NOTIFY
            | tds_base.TDS_INIT_DB_FATAL
        )
        if not login.bulk_copy:
            option_flag1 |= tds_base.TDS_DUMPLOAD_OFF
        w.put_byte(option_flag1)
        option_flag2 = login.option_flag2
        if self.authentication and not login.access_token:
            option_flag2 |= tds_base.TDS_INTEGRATED_SECURITY_ON
        w.put_byte(option_flag2)
        type_flags = 0
        if login.readonly:
            type_flags |= tds_base.TDS_FREADONLY_INTENT
        w.put_byte(type_flags)
        option_flag3 = tds_base.TDS_UNKNOWN_COLLATION_HANDLING
        if login.access_token:
            option_flag3 |= 0x10 # fExtension
        w.put_byte(option_flag3 if tds_base.IS_TDS73_PLUS(self) else 0)
        mins_fix = (
            int(
                (
                    login.client_tz.utcoffset(datetime.datetime.now())
                    or datetime.timedelta()
                ).total_seconds()
            )
            // 60
        )
        logger.info(
            "Sending LOGIN tds_ver=%x bufsz=%d pid=%d opt1=%x opt2=%x opt3=%x cli_tz=%d cli_lcid=%s "
            "cli_host=%s lang=%s db=%s",
            login.tds_version,
            w.bufsize,
            login.pid,
            option_flag1,
            option_flag2,
            option_flag3,
            mins_fix,
            login.client_lcid,
            login.client_host_name,
            login.language,
            login.database,
        )
        w.put_int(mins_fix)
        w.put_int(login.client_lcid)
        # OFFSET
        w.put_smallint(current_pos)
        w.put_smallint(len(login.client_host_name))
        current_pos += len(login.client_host_name) * 2
        if self.authentication or login.access_token:
            w.put_smallint(0)
            w.put_smallint(0)
            w.put_smallint(0)
            w.put_smallint(0)
        else:
            w.put_smallint(current_pos)
            w.put_smallint(len(login.user_name))
            current_pos += len(login.user_name) * 2
            w.put_smallint(current_pos)
            w.put_smallint(len(login.password))
            current_pos += len(login.password) * 2
        w.put_smallint(current_pos)
        w.put_smallint(len(login.app_name))
        current_pos += len(login.app_name) * 2
        # server name
        w.put_smallint(current_pos)
        w.put_smallint(len(login.server_name))
        current_pos += len(login.server_name) * 2
        # extension
        if login.access_token:
            w.put_smallint(current_pos)
            w.put_usmallint(4) # size of the token length field
            current_pos += 4
        else:
            # reserved
            w.put_smallint(0)
            w.put_smallint(0)
        # library name
        w.put_smallint(current_pos)
        w.put_smallint(len(login.library))
        current_pos += len(login.library) * 2
        # language
        w.put_smallint(current_pos)
        w.put_smallint(len(login.language))
        current_pos += len(login.language) * 2
        # database name
        w.put_smallint(current_pos)
        w.put_smallint(len(login.database))
        current_pos += len(login.database) * 2
        # extension
        extension_offset = None
        if login.access_token:
            extension_offset = current_pos
            current_pos += len(ext_packet)
        # ClientID
        client_id = struct.pack(">Q", login.client_id)[2:]
        w.write(client_id)
        # SSPI
        if self.authentication:
            w.put_smallint(current_pos)
            w.put_smallint(len(auth_packet))
            current_pos += len(auth_packet)
        else:
            w.put_smallint(0)
            w.put_smallint(0)
        # db file
        w.put_smallint(current_pos)
        w.put_smallint(len(login.attach_db_file))
        current_pos += len(login.attach_db_file) * 2
        if tds_base.IS_TDS72_PLUS(self):
            # new password
            w.put_smallint(current_pos)
            w.put_smallint(len(login.change_password))
            # sspi long
            w.put_int(0)

        # DATA
        w.write_ucs2(login.client_host_name)
        if not self.authentication and not login.access_token:
            w.write_ucs2(login.user_name)
            w.write(tds7_crypt_pass(login.password))
        w.write_ucs2(login.app_name)
        w.write_ucs2(login.server_name)
        if extension_offset:
            w.put_uint(extension_offset)
        w.write_ucs2(login.library)
        w.write_ucs2(login.language)
        w.write_ucs2(login.database)
        if self.authentication:
            w.write(auth_packet)
        elif login.access_token:
            w.write(ext_packet)
        w.write_ucs2(login.attach_db_file)
        w.write_ucs2(login.change_password)
        w.flush()

    def validate_login(self, login: _TdsLogin) -> None:
        if len(login.user_name) > 128:
            raise ValueError("User name should be no longer that 128 characters")
        if len(login.password) > 128:
            raise ValueError("Password should be not longer than 128 characters")
        if len(login.change_password) > 128:
            raise ValueError("Password should be not longer than 128 characters")
        if len(login.client_host_name) > 128:
            raise ValueError("Host name should be not longer than 128 characters")
        if len(login.app_name) > 128:
            raise ValueError("App name should be not longer than 128 characters")
        if len(login.server_name) > 128:
            raise ValueError("Server name should be not longer than 128 characters")
        if len(login.database) > 128:
            raise ValueError("Database name should be not longer than 128 characters")
        if len(login.language) > 128:
            raise ValueError("Language should be not longer than 128 characters")
        if len(login.attach_db_file) > 260:
            raise ValueError("File path should be not longer than 260 characters")
        if login.access_token == "":
            raise ValueError("Access token must not be an empty string")
        if login.access_token and not tds_base.IS_TDS74_PLUS(self):
                raise ValueError("Access token authentication requires TDS version 7.4 or higher")

    _SERVER_TO_CLIENT_MAPPING = {
        0x07000000: tds_base.TDS70,
        0x07010000: tds_base.TDS71,
        0x71000001: tds_base.TDS71rev1,
        tds_base.TDS72: tds_base.TDS72,
        tds_base.TDS73A: tds_base.TDS73A,
        tds_base.TDS73B: tds_base.TDS73B,
        tds_base.TDS74: tds_base.TDS74,
    }

    def process_login_tokens(self) -> bool:
        r = self._reader
        succeed = False
        while True:
            # When handling login requests that involve special mechanisms such as SSPI,
            # it's crucial to be aware that multiple response streams may be generated.
            # Therefore, it becomes necessary to iterate through these streams during
            # the response processing phase.
            if r.stream_finished():
                r.begin_response()

            marker = r.get_byte()
            if marker == tds_base.TDS_LOGINACK_TOKEN:
                # https://msdn.microsoft.com/en-us/library/dd340651.aspx
                succeed = True
                size = r.get_smallint()
                r.get_byte()  # interface
                version = r.get_uint_be()
                self.conn.tds_version = self._SERVER_TO_CLIENT_MAPPING.get(
                    version, version
                )
                if not tds_base.IS_TDS7_PLUS(self):
                    self.bad_stream("Only TDS 7.0 and higher are supported")
                # get server product name
                # ignore product name length, some servers seem to set it incorrectly
                r.get_byte()
                size -= 10
                self.conn.product_name = r.read_ucs2(size // 2)
                product_version = r.get_uint_be()
                logger.info(
                    "Got LOGINACK tds_ver=%x srv_name=%s srv_ver=%x",
                    self.conn.tds_version,
                    self.conn.product_name,
                    product_version,
                )
                # MSSQL 6.5 and 7.0 seem to return strange values for this
                # using TDS 4.2, something like 5F 06 32 FF for 6.50
                self.conn.product_version = product_version
                if self.authentication:
                    self.authentication.close()
                    self.authentication = None
            else:
                self.process_token(marker)
                if marker == tds_base.TDS_DONE_TOKEN:
                    break
        return succeed

    def process_returnstatus(self) -> None:
        self.log_response_message("got RETURNSTATUS message")
        self.ret_status = self._reader.get_int()
        self.has_status = True

    def process_token(self, marker: int) -> Any:
        handler = _token_map.get(marker)
        if not handler:
            self.bad_stream(f"Invalid TDS marker: {marker}({marker:x})")
            return
        return handler(self)

    def get_token_id(self) -> int:
        self.set_state(tds_base.TDS_READING)
        try:
            marker = self._reader.get_byte()
        except tds_base.TimeoutError:
            self.set_state(tds_base.TDS_PENDING)
            raise
        except:
            self._tds.close()
            raise
        return marker

    def process_simple_request(self) -> None:
        self.begin_response()
        while True:
            marker = self.get_token_id()
            if marker in (
                tds_base.TDS_DONE_TOKEN,
                tds_base.TDS_DONEPROC_TOKEN,
                tds_base.TDS_DONEINPROC_TOKEN,
            ):
                self.process_end(marker)
                if not self.done_flags & tds_base.TDS_DONE_MORE_RESULTS:
                    return
            else:
                self.process_token(marker)

    def next_set(self) -> bool | None:
        while self.more_rows:
            self.next_row()
        if self.state == tds_base.TDS_IDLE:
            return False
        if self.find_result_or_done():
            return True
        return None

    def fetchone(self) -> Any | None:
        row = self._fetchone()
        if row is None:
            return None
        else:
            return self._row_convertor(row)

    def _fetchone(self) -> list[Any] | None:
        if self.res_info is None:
            raise tds_base.ProgrammingError(
                "Previous statement didn't produce any results"
            )

        if self.skipped_to_status:
            raise tds_base.ProgrammingError(
                "Unable to fetch any rows after accessing return_status"
            )

        if not self.next_row():
            return None

        return self.row

    def next_row(self) -> bool:
        if not self.more_rows:
            return False
        while True:
            marker = self.get_token_id()
            if marker in (tds_base.TDS_ROW_TOKEN, tds_base.TDS_NBC_ROW_TOKEN):
                self.process_token(marker)
                return True
            elif marker in (
                tds_base.TDS_DONE_TOKEN,
                tds_base.TDS_DONEPROC_TOKEN,
                tds_base.TDS_DONEINPROC_TOKEN,
            ):
                self.process_end(marker)
                return False
            else:
                self.process_token(marker)

    def find_result_or_done(self) -> bool:
        self.done_flags = 0
        while True:
            marker = self.get_token_id()
            if marker == tds_base.TDS7_RESULT_TOKEN:
                self.process_token(marker)
                return True
            elif marker in (
                tds_base.TDS_DONE_TOKEN,
                tds_base.TDS_DONEPROC_TOKEN,
                tds_base.TDS_DONEINPROC_TOKEN,
            ):
                self.process_end(marker)
                if self.done_flags & tds_base.TDS_DONE_MORE_RESULTS:
                    if self.done_flags & tds_base.TDS_DONE_COUNT:
                        return True
                else:
                    return False
            else:
                self.process_token(marker)

    def process_rpc(self) -> bool:
        self.done_flags = 0
        self.return_value_index = 0
        while True:
            marker = self.get_token_id()
            if marker == tds_base.TDS7_RESULT_TOKEN:
                self.process_token(marker)
                return True
            elif marker in (tds_base.TDS_DONE_TOKEN, tds_base.TDS_DONEPROC_TOKEN):
                self.process_end(marker)
                if (
                    self.done_flags & tds_base.TDS_DONE_MORE_RESULTS
                    and not self.done_flags & tds_base.TDS_DONE_COUNT
                ):
                    # skip results that don't event have rowcount
                    continue
                return False
            else:
                self.process_token(marker)

    def complete_rpc(self) -> None:
        # go through all result sets
        while self.next_set():
            pass

    def find_return_status(self) -> None:
        self.skipped_to_status = True
        while True:
            marker = self.get_token_id()
            self.process_token(marker)
            if marker == tds_base.TDS_RETURNSTATUS_TOKEN:
                return

    def process_tabname(self):
        """
        Processes TABNAME token

        Ref: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/140e3348-da08-409a-b6c3-f0fc9cee2d6e
        """
        r = self._reader
        total_length = r.get_smallint()
        if not tds_base.IS_TDS71_PLUS(self):
            r.get_smallint()  # name length
        tds_base.skipall(r, total_length)

    def process_colinfo(self):
        """
        Process COLNAME token

        Ref: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/aa8466c5-ca3d-48ca-a638-7c1becebe754
        """
        r = self._reader
        total_length = r.get_smallint()
        tds_base.skipall(r, total_length)
    
    def process_featureextack(self):
        """
        Process FEATUREEXTACK token

        Ref: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/2eb82f8e-11f0-46dc-b42d-27302fa4701a
        """
        r = self._reader
        features = []
        def get_featureackopt():
            feature_id = r.get_byte()
            if feature_id == 0xFF:
                return None, None
            feature_ack_len = r.get_uint()
            if feature_id == tds_base.TDS_LOGIN_FEATURE_FEDAUTH and feature_ack_len >= 32:
                sig = None
                nonce = readall(r, 32)
                if feature_ack_len > 32:
                    sig = readall(r, 32)
                return feature_id, (nonce, sig)
            elif feature_id == tds_base.TDS_LOGIN_FEATURE_UTF8_SUPPORT and feature_ack_len > 0: 
                utf8_support = r.get_byte()
                return feature_id, utf8_support
            else:
                 feature_ack = readall(r, feature_ack_len)  
            return feature_id, feature_ack
        while True:
            feature_id, feature_ack = get_featureackopt()
            features.append(feature_id)
            if feature_id is None:
                break

        if self.conn.fedauth_required and tds_base.TDS_LOGIN_FEATURE_FEDAUTH not in features:
            self.bad_stream("Server didn't send expected FEDAUTH in FEATUREEXTACK")


_token_map = {
    tds_base.TDS_AUTH_TOKEN: _TdsSession.process_auth,
    tds_base.TDS_ENVCHANGE_TOKEN: _TdsSession.process_env_chg,
    tds_base.TDS_CONTROL_TOKEN: lambda self: self.process_featureextack(),
    tds_base.TDS_DONE_TOKEN: lambda self: self.process_end(tds_base.TDS_DONE_TOKEN),
    tds_base.TDS_DONEPROC_TOKEN: lambda self: self.process_end(
        tds_base.TDS_DONEPROC_TOKEN
    ),
    tds_base.TDS_DONEINPROC_TOKEN: lambda self: self.process_end(
        tds_base.TDS_DONEINPROC_TOKEN
    ),
    tds_base.TDS_ERROR_TOKEN: lambda self: self.process_msg(tds_base.TDS_ERROR_TOKEN),
    tds_base.TDS_INFO_TOKEN: lambda self: self.process_msg(tds_base.TDS_INFO_TOKEN),
    tds_base.TDS_CAPABILITY_TOKEN: lambda self: self.process_msg(
        tds_base.TDS_CAPABILITY_TOKEN
    ),
    tds_base.TDS_PARAM_TOKEN: lambda self: self.process_param(),
    tds_base.TDS7_RESULT_TOKEN: lambda self: self.tds7_process_result(),
    tds_base.TDS_ROW_TOKEN: lambda self: self.process_row(),
    tds_base.TDS_NBC_ROW_TOKEN: lambda self: self.process_nbcrow(),
    tds_base.TDS_ORDERBY_TOKEN: lambda self: self.process_orderby(),
    tds_base.TDS_RETURNSTATUS_TOKEN: lambda self: self.process_returnstatus(),
    tds_base.TDS_TABNAME_TOKEN: lambda self: self.process_tabname(),
    tds_base.TDS_COLINFO_TOKEN: lambda self: self.process_colinfo(),
}
