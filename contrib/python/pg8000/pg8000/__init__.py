from pg8000.legacy import (
    BIGINTEGER,
    BINARY,
    BOOLEAN,
    BOOLEAN_ARRAY,
    BYTES,
    Binary,
    CHAR,
    CHAR_ARRAY,
    Connection,
    Cursor,
    DATE,
    DATETIME,
    DECIMAL,
    DECIMAL_ARRAY,
    DataError,
    DatabaseError,
    Date,
    DateFromTicks,
    Error,
    FLOAT,
    FLOAT_ARRAY,
    INET,
    INT2VECTOR,
    INTEGER,
    INTEGER_ARRAY,
    INTERVAL,
    IntegrityError,
    InterfaceError,
    InternalError,
    JSON,
    JSONB,
    MACADDR,
    NAME,
    NAME_ARRAY,
    NULLTYPE,
    NUMBER,
    NotSupportedError,
    OID,
    OperationalError,
    PGInterval,
    ProgrammingError,
    ROWID,
    Range,
    STRING,
    TEXT,
    TEXT_ARRAY,
    TIME,
    TIMEDELTA,
    TIMESTAMP,
    TIMESTAMPTZ,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
    UNKNOWN,
    UUID_TYPE,
    VARCHAR,
    VARCHAR_ARRAY,
    Warning,
    XID,
    __version__,
    pginterval_in,
    pginterval_out,
    timedelta_in,
)

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


def connect(
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
):
    return Connection(
        user,
        host=host,
        database=database,
        port=port,
        password=password,
        source_address=source_address,
        unix_sock=unix_sock,
        ssl_context=ssl_context,
        timeout=timeout,
        tcp_keepalive=tcp_keepalive,
        application_name=application_name,
        replication=replication,
    )


apilevel = "2.0"
"""The DBAPI level supported, currently "2.0".

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.
"""

threadsafety = 1
"""Integer constant stating the level of thread safety the DBAPI interface
supports. This DBAPI module supports sharing of the module only. Connections
and cursors my not be shared between threads. This gives pg8000 a threadsafety
value of 1.

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.
"""

paramstyle = "format"


__all__ = [
    "BIGINTEGER",
    "BINARY",
    "BOOLEAN",
    "BOOLEAN_ARRAY",
    "BYTES",
    "Binary",
    "CHAR",
    "CHAR_ARRAY",
    "Connection",
    "Cursor",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DECIMAL_ARRAY",
    "DataError",
    "DatabaseError",
    "Date",
    "DateFromTicks",
    "Error",
    "FLOAT",
    "FLOAT_ARRAY",
    "INET",
    "INT2VECTOR",
    "INTEGER",
    "INTEGER_ARRAY",
    "INTERVAL",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "JSON",
    "JSONB",
    "MACADDR",
    "NAME",
    "NAME_ARRAY",
    "NULLTYPE",
    "NUMBER",
    "NotSupportedError",
    "OID",
    "OperationalError",
    "PGInterval",
    "ProgrammingError",
    "ROWID",
    "Range",
    "STRING",
    "TEXT",
    "TEXT_ARRAY",
    "TIME",
    "TIMEDELTA",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "UNKNOWN",
    "UUID_TYPE",
    "VARCHAR",
    "VARCHAR_ARRAY",
    "Warning",
    "XID",
    "__version__",
    "connect",
    "pginterval_in",
    "pginterval_out",
    "timedelta_in",
]
