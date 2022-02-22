PY23_LIBRARY()

OWNER(g:kikimr)

PY_SRCS(
    NAMESPACE ydb
    __init__.py
    _apis.py
    _session_impl.py
    _sp_impl.py
    _tx_ctx_impl.py
    _utilities.py
    credentials.py
    connection.py
    convert.py
    default_pem.py
    driver.py
    issues.py
    operation.py
    pool.py
    resolver.py
    scheme.py
    settings.py
    table.py
    export.py
    types.py
    interceptor.py
    s3list.py
    auth_helpers.py
    experimental.py
    scripting.py
    import_client.py
    ydb_version.py
    tracing.py

    dbapi/__init__.py
    dbapi/connection.py
    dbapi/cursor.py
    dbapi/errors.py

    sqlalchemy/__init__.py
    sqlalchemy/types.py

    iam/__init__.py
    iam/auth.py
)

IF (PYTHON3)

    PY_SRCS(
        NAMESPACE ydb
        tornado/__init__.py
        tornado/tornado_helpers.py

        aio/connection.py
        aio/driver.py
        aio/iam.py
        aio/__init__.py
        aio/pool.py
        aio/resolver.py
        aio/scheme.py
        aio/table.py
        aio/_utilities.py
    )

ENDIF()

IF (PYTHON2)
    PEERDIR(
        contrib/python/enum34
        contrib/python/futures
    )
ENDIF()

PEERDIR(
    contrib/libs/grpc/python
    contrib/python/protobuf
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
)

END()
