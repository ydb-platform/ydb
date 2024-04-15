import codecs
import socket
from collections import defaultdict, deque
from hashlib import md5
from importlib.metadata import version
from io import IOBase, TextIOBase
from itertools import count
from struct import Struct

import scramp

from pg8000.converters import (
    PG_PY_ENCODINGS,
    PG_TYPES,
    PY_TYPES,
    make_params,
    string_in,
)
from pg8000.exceptions import DatabaseError, Error, InterfaceError


ver = version("pg8000")


def pack_funcs(fmt):
    struc = Struct(f"!{fmt}")
    return struc.pack, struc.unpack_from


i_pack, i_unpack = pack_funcs("i")
H_pack, H_unpack = pack_funcs("H")
ii_pack, ii_unpack = pack_funcs("ii")
ihihih_pack, ihihih_unpack = pack_funcs("ihihih")
ci_pack, ci_unpack = pack_funcs("ci")
bh_pack, bh_unpack = pack_funcs("bh")
cccc_pack, cccc_unpack = pack_funcs("cccc")


# Copyright (c) 2007-2009, Mathieu Fenniak
# Copyright (c) The Contributors
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
# * The name of the author may not be used to endorse or promote products
# derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

__author__ = "Mathieu Fenniak"


NULL_BYTE = b"\x00"


# Message codes
NOTICE_RESPONSE = b"N"
AUTHENTICATION_REQUEST = b"R"
PARAMETER_STATUS = b"S"
BACKEND_KEY_DATA = b"K"
READY_FOR_QUERY = b"Z"
ROW_DESCRIPTION = b"T"
ERROR_RESPONSE = b"E"
DATA_ROW = b"D"
COMMAND_COMPLETE = b"C"
PARSE_COMPLETE = b"1"
BIND_COMPLETE = b"2"
CLOSE_COMPLETE = b"3"
PORTAL_SUSPENDED = b"s"
NO_DATA = b"n"
PARAMETER_DESCRIPTION = b"t"
NOTIFICATION_RESPONSE = b"A"
COPY_DONE = b"c"
COPY_DATA = b"d"
COPY_IN_RESPONSE = b"G"
COPY_OUT_RESPONSE = b"H"
EMPTY_QUERY_RESPONSE = b"I"

BIND = b"B"
PARSE = b"P"
QUERY = b"Q"
EXECUTE = b"E"
FLUSH = b"H"
SYNC = b"S"
PASSWORD = b"p"
DESCRIBE = b"D"
TERMINATE = b"X"
CLOSE = b"C"


def _create_message(code, data=b""):
    return code + i_pack(len(data) + 4) + data


FLUSH_MSG = _create_message(FLUSH)
SYNC_MSG = _create_message(SYNC)
TERMINATE_MSG = _create_message(TERMINATE)
COPY_DONE_MSG = _create_message(COPY_DONE)
EXECUTE_MSG = _create_message(EXECUTE, NULL_BYTE + i_pack(0))

# DESCRIBE constants
STATEMENT = b"S"
PORTAL = b"P"

# ErrorResponse codes
RESPONSE_SEVERITY = "S"  # always present
RESPONSE_SEVERITY = "V"  # always present
RESPONSE_CODE = "C"  # always present
RESPONSE_MSG = "M"  # always present
RESPONSE_DETAIL = "D"
RESPONSE_HINT = "H"
RESPONSE_POSITION = "P"
RESPONSE__POSITION = "p"
RESPONSE__QUERY = "q"
RESPONSE_WHERE = "W"
RESPONSE_FILE = "F"
RESPONSE_LINE = "L"
RESPONSE_ROUTINE = "R"

IDLE = b"I"
IN_TRANSACTION = b"T"
IN_FAILED_TRANSACTION = b"E"


def _flush(sock):
    try:
        sock.flush()
    except OSError as e:
        raise InterfaceError("network error") from e


def _read(sock, size):
    got = 0
    buff = []
    try:
        while got < size:
            block = sock.read(size - got)
            if block == b"":
                raise InterfaceError("network error")
            buff.append(block)
            got += len(block)
    except OSError as e:
        raise InterfaceError("network error") from e

    return b"".join(buff)


def _write(sock, d):
    try:
        sock.write(d)
    except OSError as e:
        raise InterfaceError("network error") from e


def _make_socket(
    unix_sock,
    orig_sock,
    host,
    port,
    timeout,
    source_address,
    tcp_keepalive,
    orig_ssl_context,
):
    if unix_sock is not None:
        if orig_sock is not None:
            raise InterfaceError("If unix_sock is provided, sock must be None")

        try:
            if not hasattr(socket, "AF_UNIX"):
                raise InterfaceError(
                    "attempt to connect to unix socket on unsupported platform"
                )
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect(unix_sock)
            if tcp_keepalive:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except socket.error as e:
            if sock is not None:
                sock.close()
            raise InterfaceError("communication error") from e

    elif orig_sock is not None:
        sock = orig_sock

    elif host is not None:
        try:
            sock = socket.create_connection((host, port), timeout, source_address)
        except socket.error as e:
            raise InterfaceError(
                f"Can't create a connection to host {host} and port {port} "
                f"(timeout is {timeout} and source_address is {source_address})."
            ) from e

        if tcp_keepalive:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    else:
        raise InterfaceError("one of host, sock or unix_sock must be provided")

    channel_binding = None
    if orig_ssl_context is not False:
        try:
            import ssl

            if orig_ssl_context is True or orig_ssl_context is None:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            else:
                ssl_context = orig_ssl_context

            # Int32(8) - Message length, including self.
            # Int32(80877103) - The SSL request code.
            sock.sendall(ii_pack(8, 80877103))
            resp = sock.recv(1).decode("ascii")
            if resp == "S":
                sock = ssl_context.wrap_socket(sock, server_hostname=host)
                channel_binding = scramp.make_channel_binding(
                    "tls-server-end-point", sock
                )
            elif orig_ssl_context is not None:
                if sock is not None:
                    sock.close()
                raise InterfaceError("Server refuses SSL")

        except ImportError:
            raise InterfaceError(
                "SSL required but ssl module not available in this python "
                "installation."
            )
    return channel_binding, sock


class CoreConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __init__(
        self,
        user,
        host="localhost",
        database=None,
        port=5432,
        password=None,
        source_address=None,
        unix_sock=None,
        ssl_context=None,
        timeout=None,
        tcp_keepalive=True,
        application_name=None,
        replication=None,
        sock=None,
    ):
        self._client_encoding = "utf8"
        self._commands_with_count = (
            b"INSERT",
            b"DELETE",
            b"UPDATE",
            b"MOVE",
            b"FETCH",
            b"COPY",
            b"SELECT",
        )
        self.notifications = deque(maxlen=100)
        self.notices = deque(maxlen=100)
        self.parameter_statuses = {}

        if user is None:
            raise InterfaceError("The 'user' connection parameter cannot be None")

        init_params = {
            "user": user,
            "database": database,
            "application_name": application_name,
            "replication": replication,
        }

        for k, v in tuple(init_params.items()):
            if isinstance(v, str):
                init_params[k] = v.encode("utf8")
            elif v is None:
                del init_params[k]
            elif not isinstance(v, (bytes, bytearray)):
                raise InterfaceError(f"The parameter {k} can't be of type {type(v)}.")

        self.user = init_params["user"]

        if isinstance(password, str):
            self.password = password.encode("utf8")
        else:
            self.password = password

        self._xid = None
        self._statement_nums = set()

        self._caches = {}

        self.channel_binding, self._usock = _make_socket(
            unix_sock,
            sock,
            host,
            port,
            timeout,
            source_address,
            tcp_keepalive,
            ssl_context,
        )

        self._sock = self._usock.makefile(mode="rwb")

        self._backend_key_data = None

        self.pg_types = defaultdict(lambda: string_in, PG_TYPES)
        self.py_types = dict(PY_TYPES)

        self.message_types = {
            NOTICE_RESPONSE: self.handle_NOTICE_RESPONSE,
            AUTHENTICATION_REQUEST: self.handle_AUTHENTICATION_REQUEST,
            PARAMETER_STATUS: self.handle_PARAMETER_STATUS,
            BACKEND_KEY_DATA: self.handle_BACKEND_KEY_DATA,
            READY_FOR_QUERY: self.handle_READY_FOR_QUERY,
            ROW_DESCRIPTION: self.handle_ROW_DESCRIPTION,
            ERROR_RESPONSE: self.handle_ERROR_RESPONSE,
            EMPTY_QUERY_RESPONSE: self.handle_EMPTY_QUERY_RESPONSE,
            DATA_ROW: self.handle_DATA_ROW,
            COMMAND_COMPLETE: self.handle_COMMAND_COMPLETE,
            PARSE_COMPLETE: self.handle_PARSE_COMPLETE,
            BIND_COMPLETE: self.handle_BIND_COMPLETE,
            CLOSE_COMPLETE: self.handle_CLOSE_COMPLETE,
            PORTAL_SUSPENDED: self.handle_PORTAL_SUSPENDED,
            NO_DATA: self.handle_NO_DATA,
            PARAMETER_DESCRIPTION: self.handle_PARAMETER_DESCRIPTION,
            NOTIFICATION_RESPONSE: self.handle_NOTIFICATION_RESPONSE,
            COPY_DONE: self.handle_COPY_DONE,
            COPY_DATA: self.handle_COPY_DATA,
            COPY_IN_RESPONSE: self.handle_COPY_IN_RESPONSE,
            COPY_OUT_RESPONSE: self.handle_COPY_OUT_RESPONSE,
        }

        # Int32 - Message length, including self.
        # Int32(196608) - Protocol version number.  Version 3.0.
        # Any number of key/value pairs, terminated by a zero byte:
        #   String - A parameter name (user, database, or options)
        #   String - Parameter value
        protocol = 196608
        val = bytearray(i_pack(protocol))

        for k, v in init_params.items():
            val.extend(k.encode("ascii") + NULL_BYTE + v + NULL_BYTE)
        val.append(0)
        _write(self._sock, i_pack(len(val) + 4))
        _write(self._sock, val)
        _flush(self._sock)

        try:
            code = None
            context = Context(None)
            while code not in (READY_FOR_QUERY, ERROR_RESPONSE):
                code, data_len = ci_unpack(_read(self._sock, 5))

                self.message_types[code](_read(self._sock, data_len - 4), context)

            if context.error is not None:
                raise context.error

        except Error as e:
            self.close()
            raise e

        self._transaction_status = None

    def register_out_adapter(self, typ, out_func):
        self.py_types[typ] = out_func

    def register_in_adapter(self, oid, in_func):
        self.pg_types[oid] = in_func

    def handle_ERROR_RESPONSE(self, data, context):
        msg = {
            s[:1].decode("ascii"): s[1:].decode(self._client_encoding, errors="replace")
            for s in data.split(NULL_BYTE)
            if s != b""
        }

        context.error = DatabaseError(msg)

    def handle_EMPTY_QUERY_RESPONSE(self, data, context):
        pass

    def handle_CLOSE_COMPLETE(self, data, context):
        pass

    def handle_PARSE_COMPLETE(self, data, context):
        # Byte1('1') - Identifier.
        # Int32(4) - Message length, including self.
        pass

    def handle_BIND_COMPLETE(self, data, context):
        pass

    def handle_PORTAL_SUSPENDED(self, data, context):
        pass

    def handle_PARAMETER_DESCRIPTION(self, data, context):
        """https://www.postgresql.org/docs/current/protocol-message-formats.html"""

        # count = h_unpack(data)[0]
        # context.parameter_oids = unpack_from("!" + "i" * count, data, 2)

    def handle_COPY_DONE(self, data, context):
        pass

    def handle_COPY_OUT_RESPONSE(self, data, context):
        """https://www.postgresql.org/docs/current/protocol-message-formats.html"""

        is_binary, num_cols = bh_unpack(data)
        # column_formats = unpack_from('!' + 'h' * num_cols, data, 3)

        if context.stream is None:
            raise InterfaceError(
                "An output stream is required for the COPY OUT response."
            )

        elif isinstance(context.stream, TextIOBase):
            if is_binary:
                raise InterfaceError(
                    "The COPY OUT stream is binary, but the stream parameter is text."
                )
            else:
                decode = codecs.getdecoder(self._client_encoding)

                def w(data):
                    context.stream.write(decode(data)[0])

                context.stream_write = w

        else:
            context.stream_write = context.stream.write

    def handle_COPY_DATA(self, data, context):
        context.stream_write(data)

    def handle_COPY_IN_RESPONSE(self, data, context):
        """https://www.postgresql.org/docs/current/protocol-message-formats.html"""
        is_binary, num_cols = bh_unpack(data)
        # column_formats = unpack_from('!' + 'h' * num_cols, data, 3)

        if context.stream is None:
            raise InterfaceError(
                "The 'stream' parameter is required for the COPY IN response. The "
                "'stream' parameter can be an I/O stream or an iterable."
            )

        if isinstance(context.stream, IOBase):
            if isinstance(context.stream, TextIOBase):
                if is_binary:
                    raise InterfaceError(
                        "The COPY IN stream is binary, but the stream parameter is a "
                        "text stream."
                    )

                else:

                    def ri(bffr):
                        bffr.clear()
                        bffr.extend(
                            context.stream.read(4096).encode(self._client_encoding)
                        )
                        return len(bffr)

                    readinto = ri
            else:
                readinto = context.stream.readinto

            bffr = bytearray(8192)
            while True:
                bytes_read = readinto(bffr)
                if bytes_read == 0:
                    break
                _write(self._sock, COPY_DATA)
                _write(self._sock, i_pack(bytes_read + 4))
                _write(self._sock, bffr[:bytes_read])
                _flush(self._sock)

        else:
            for k in context.stream:
                if isinstance(k, str):
                    if is_binary:
                        raise InterfaceError(
                            "The COPY IN stream is binary, but the stream parameter "
                            "is an iterable with str type items."
                        )
                    b = k.encode(self._client_encoding)
                else:
                    b = k

                self._send_message(COPY_DATA, b)
                _flush(self._sock)

        # Send CopyDone
        _write(self._sock, COPY_DONE_MSG)
        _write(self._sock, SYNC_MSG)
        _flush(self._sock)

    def handle_NOTIFICATION_RESPONSE(self, data, context):
        """https://www.postgresql.org/docs/current/protocol-message-formats.html"""
        backend_pid = i_unpack(data)[0]
        idx = 4
        null_idx = data.find(NULL_BYTE, idx)
        channel = data[idx:null_idx].decode("ascii")
        payload = data[null_idx + 1 : -1].decode("ascii")

        self.notifications.append((backend_pid, channel, payload))

    def close(self):
        """Closes the database connection.

        This function is part of the `DBAPI 2.0 specification
        <http://www.python.org/dev/peps/pep-0249/>`_.
        """

        if self._usock is None:
            raise InterfaceError("connection is closed")

        try:
            _write(self._sock, TERMINATE_MSG)
            _flush(self._sock)
        finally:
            try:
                self._usock.close()
            except socket.error as e:
                raise InterfaceError("network error") from e
            finally:
                self._sock = None
                self._usock = None

    def handle_AUTHENTICATION_REQUEST(self, data, context):
        """https://www.postgresql.org/docs/current/protocol-message-formats.html"""

        auth_code = i_unpack(data)[0]
        if auth_code == 0:
            pass
        elif auth_code == 3:
            if self.password is None:
                raise InterfaceError(
                    "server requesting password authentication, but no password was "
                    "provided"
                )
            self._send_message(PASSWORD, self.password + NULL_BYTE)
            _flush(self._sock)

        elif auth_code == 5:
            salt = b"".join(cccc_unpack(data, 4))
            if self.password is None:
                raise InterfaceError(
                    "server requesting MD5 password authentication, but no password "
                    "was provided"
                )
            pwd = b"md5" + md5(
                md5(self.password + self.user).hexdigest().encode("ascii") + salt
            ).hexdigest().encode("ascii")
            self._send_message(PASSWORD, pwd + NULL_BYTE)
            _flush(self._sock)

        elif auth_code == 10:
            # AuthenticationSASL
            mechanisms = [m.decode("ascii") for m in data[4:-2].split(NULL_BYTE)]

            self.auth = scramp.ScramClient(
                mechanisms,
                self.user.decode("utf8"),
                self.password.decode("utf8"),
                channel_binding=self.channel_binding,
            )

            init = self.auth.get_client_first().encode("utf8")
            mech = self.auth.mechanism_name.encode("ascii") + NULL_BYTE

            # SASLInitialResponse
            self._send_message(PASSWORD, mech + i_pack(len(init)) + init)
            _flush(self._sock)

        elif auth_code == 11:
            # AuthenticationSASLContinue
            self.auth.set_server_first(data[4:].decode("utf8"))

            # SASLResponse
            msg = self.auth.get_client_final().encode("utf8")
            self._send_message(PASSWORD, msg)
            _flush(self._sock)

        elif auth_code == 12:
            # AuthenticationSASLFinal
            self.auth.set_server_final(data[4:].decode("utf8"))

        elif auth_code in (2, 4, 6, 7, 8, 9):
            raise InterfaceError(
                f"Authentication method {auth_code} not supported by pg8000."
            )
        else:
            raise InterfaceError(
                f"Authentication method {auth_code} not recognized by pg8000."
            )

    def handle_READY_FOR_QUERY(self, data, context):
        self._transaction_status = data

    def handle_BACKEND_KEY_DATA(self, data, context):
        self._backend_key_data = data

    def handle_ROW_DESCRIPTION(self, data, context):
        count = H_unpack(data)[0]
        idx = 2
        columns = []
        input_funcs = []
        for i in range(count):
            name = data[idx : data.find(NULL_BYTE, idx)]
            idx += len(name) + 1
            field = dict(
                zip(
                    (
                        "table_oid",
                        "column_attrnum",
                        "type_oid",
                        "type_size",
                        "type_modifier",
                        "format",
                    ),
                    ihihih_unpack(data, idx),
                )
            )
            field["name"] = name.decode(self._client_encoding)
            idx += 18
            columns.append(field)
            input_funcs.append(self.pg_types[field["type_oid"]])

        context.columns = columns
        context.input_funcs = input_funcs
        if context.rows is None:
            context.rows = []

    def send_PARSE(self, statement_name_bin, statement, oids=()):
        val = bytearray(statement_name_bin)
        val.extend(statement.encode(self._client_encoding) + NULL_BYTE)
        val.extend(H_pack(len(oids)))
        for oid in oids:
            val.extend(i_pack(0 if oid == -1 else oid))

        self._send_message(PARSE, val)
        _write(self._sock, FLUSH_MSG)

    def send_DESCRIBE_STATEMENT(self, statement_name_bin):
        self._send_message(DESCRIBE, STATEMENT + statement_name_bin)
        _write(self._sock, FLUSH_MSG)

    def send_QUERY(self, sql):
        self._send_message(QUERY, sql.encode(self._client_encoding) + NULL_BYTE)

    def execute_simple(self, statement):
        context = Context(statement)

        self.send_QUERY(statement)
        _flush(self._sock)
        self.handle_messages(context)

        return context

    def execute_unnamed(self, statement, vals=(), oids=(), stream=None):
        context = Context(statement, stream=stream)

        self.send_PARSE(NULL_BYTE, statement, oids)
        _write(self._sock, SYNC_MSG)
        _flush(self._sock)
        self.handle_messages(context)
        self.send_DESCRIBE_STATEMENT(NULL_BYTE)

        _write(self._sock, SYNC_MSG)

        try:
            _flush(self._sock)
        except AttributeError as e:
            if self._sock is None:
                raise InterfaceError("connection is closed")
            else:
                raise e
        params = make_params(self.py_types, vals)
        self.send_BIND(NULL_BYTE, params)
        self.handle_messages(context)
        self.send_EXECUTE()

        _write(self._sock, SYNC_MSG)
        _flush(self._sock)
        self.handle_messages(context)

        return context

    def prepare_statement(self, statement, oids=None):
        for i in count():
            statement_name = f"pg8000_statement_{i}"
            statement_name_bin = statement_name.encode("ascii") + NULL_BYTE
            if statement_name_bin not in self._statement_nums:
                self._statement_nums.add(statement_name_bin)
                break

        self.send_PARSE(statement_name_bin, statement, oids)
        self.send_DESCRIBE_STATEMENT(statement_name_bin)
        _write(self._sock, SYNC_MSG)

        try:
            _flush(self._sock)
        except AttributeError as e:
            if self._sock is None:
                raise InterfaceError("connection is closed")
            else:
                raise e

        context = Context(statement)
        self.handle_messages(context)

        return statement_name_bin, context.columns, context.input_funcs

    def execute_named(
        self, statement_name_bin, params, columns, input_funcs, statement
    ):
        context = Context(columns=columns, input_funcs=input_funcs, statement=statement)

        self.send_BIND(statement_name_bin, params)
        self.send_EXECUTE()
        _write(self._sock, SYNC_MSG)
        _flush(self._sock)
        self.handle_messages(context)
        return context

    def _send_message(self, code, data):
        try:
            _write(self._sock, code)
            _write(self._sock, i_pack(len(data) + 4))
            _write(self._sock, data)
        except ValueError as e:
            if str(e) == "write to closed file":
                raise InterfaceError("connection is closed")
            else:
                raise e
        except AttributeError:
            raise InterfaceError("connection is closed")

    def send_BIND(self, statement_name_bin, params):
        """https://www.postgresql.org/docs/current/protocol-message-formats.html"""

        retval = bytearray(
            NULL_BYTE + statement_name_bin + H_pack(0) + H_pack(len(params))
        )

        for value in params:
            if value is None:
                retval.extend(i_pack(-1))
            else:
                val = value.encode(self._client_encoding)
                retval.extend(i_pack(len(val)))
                retval.extend(val)
        retval.extend(H_pack(0))

        self._send_message(BIND, retval)
        _write(self._sock, FLUSH_MSG)

    def send_EXECUTE(self):
        """https://www.postgresql.org/docs/current/protocol-message-formats.html"""
        _write(self._sock, EXECUTE_MSG)
        _write(self._sock, FLUSH_MSG)

    def handle_NO_DATA(self, msg, context):
        pass

    def handle_COMMAND_COMPLETE(self, data, context):
        if self._transaction_status == IN_FAILED_TRANSACTION and context.error is None:
            sql = context.statement.split()[0].rstrip(";").upper()
            if sql != "ROLLBACK":
                context.error = InterfaceError("in failed transaction block")

        values = data[:-1].split(b" ")
        try:
            row_count = int(values[-1])
            if context.row_count == -1:
                context.row_count = row_count
            else:
                context.row_count += row_count
        except ValueError:
            pass

    def handle_DATA_ROW(self, data, context):
        idx = 2
        row = []
        for func in context.input_funcs:
            vlen = i_unpack(data, idx)[0]
            idx += 4
            if vlen == -1:
                v = None
            else:
                v = func(str(data[idx : idx + vlen], encoding=self._client_encoding))
                idx += vlen
            row.append(v)
        context.rows.append(row)

    def handle_messages(self, context):
        code = None

        while code != READY_FOR_QUERY:
            code, data_len = ci_unpack(_read(self._sock, 5))

            self.message_types[code](_read(self._sock, data_len - 4), context)

        if context.error is not None:
            raise context.error

    def close_prepared_statement(self, statement_name_bin):
        """https://www.postgresql.org/docs/current/protocol-message-formats.html"""
        self._send_message(CLOSE, STATEMENT + statement_name_bin)
        _write(self._sock, FLUSH_MSG)
        _write(self._sock, SYNC_MSG)
        _flush(self._sock)
        context = Context(None)
        self.handle_messages(context)
        self._statement_nums.remove(statement_name_bin)

    def handle_NOTICE_RESPONSE(self, data, context):
        """https://www.postgresql.org/docs/current/protocol-message-formats.html"""
        self.notices.append({s[0:1]: s[1:] for s in data.split(NULL_BYTE)})

    def handle_PARAMETER_STATUS(self, data, context):
        pos = data.find(NULL_BYTE)
        key, value = data[:pos].decode("ascii"), data[pos + 1 : -1].decode("ascii")
        self.parameter_statuses[key] = value
        if key == "client_encoding":
            encoding = value.lower()
            self._client_encoding = PG_PY_ENCODINGS.get(encoding, encoding)

        elif key == "integer_datetimes":
            if value == "on":
                pass

            else:
                pass

        elif key == "server_version":
            pass


class Context:
    def __init__(self, statement, stream=None, columns=None, input_funcs=None):
        self.statement = statement
        self.rows = None if columns is None else []
        self.row_count = -1
        self.columns = columns
        self.stream = stream
        self.input_funcs = [] if input_funcs is None else input_funcs
        self.error = None
