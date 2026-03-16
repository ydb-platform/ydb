PY2_LIBRARY()

LICENSE(LGPL-3.0-or-later)

VERSION(2.8.6)

PEERDIR(
    contrib/libs/libpq
)

NO_COMPILER_WARNINGS()

NO_LINT()

ADDINCL(
    contrib/libs/libpq/src/include
    contrib/libs/libpq/src/interfaces/libpq
    contrib/python/psycopg2/py2
)

CFLAGS(
    -DPSYCOPG_VERSION='2.8.6 \(dt dec pq3 ext lo64\)'
    -DPG_VERSION_NUM=140003
    -DHAVE_LO64=1
)

IF (OS_WINDOWS)
    # To avoid symbol character conflict when using libpq
    CFLAGS(
        -Dgettimeofday=gettimeofday_pycopg2
    )
ENDIF()

SRCS(
    psycopg/psycopgmodule.c
    psycopg/green.c
    psycopg/pqpath.c
    psycopg/utils.c
    psycopg/bytes_format.c
    psycopg/column_type.c
    psycopg/connection_int.c
    psycopg/connection_type.c
    psycopg/conninfo_type.c
    psycopg/cursor_int.c
    psycopg/cursor_type.c
    psycopg/diagnostics_type.c
    psycopg/error_type.c
    psycopg/lobject_int.c
    psycopg/lobject_type.c
    psycopg/notify_type.c
    psycopg/xid_type.c
    psycopg/adapter_asis.c
    psycopg/adapter_binary.c
    psycopg/adapter_datetime.c
    psycopg/adapter_list.c
    psycopg/adapter_pboolean.c
    psycopg/adapter_pdecimal.c
    psycopg/adapter_pint.c
    psycopg/adapter_pfloat.c
    psycopg/adapter_qstring.c
    psycopg/microprotocols.c
    psycopg/microprotocols_proto.c
    psycopg/typecast.c
    psycopg/libpq_support.c
    psycopg/replication_connection_type.c
    psycopg/replication_cursor_type.c
    psycopg/replication_message_type.c
    psycopg/win32_support.c
)

PY_REGISTER(psycopg2._psycopg)

SRCDIR(contrib/python/psycopg2/py2/lib)

PY_SRCS(
    NAMESPACE psycopg2
    __init__.py
    _ipaddress.py
    _json.py
    _lru_cache.py
    _range.py
    compat.py
    errorcodes.py
    errors.py
    extensions.py
    extras.py
    pool.py
    sql.py
    tz.py
)

RESOURCE_FILES(
    PREFIX contrib/python/psycopg2/py2/
    .dist-info/METADATA
    .dist-info/top_level.txt
)

END()
