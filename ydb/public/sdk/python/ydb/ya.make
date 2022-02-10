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
    auth_helpers.py
    connection.py
    convert.py
    credentials.py
    dbapi/__init__.py
    dbapi/connection.py
    dbapi/cursor.py
    dbapi/errors.py
    default_pem.py
    driver.py
    experimental.py
    export.py
    iam/__init__.py
    iam/auth.py
    import_client.py
    interceptor.py
    issues.py
    operation.py
    pool.py
    resolver.py
    s3list.py
    scheme.py
    scripting.py
    settings.py
    sqlalchemy/__init__.py
    sqlalchemy/types.py
    table.py
    tracing.py
    types.py
    ydb_version.py
) 
 
IF (PYTHON3) 
 
    PY_SRCS( 
        NAMESPACE ydb 
        aio/__init__.py
        aio/_utilities.py
        aio/connection.py 
        aio/driver.py 
        aio/iam.py 
        aio/pool.py 
        aio/resolver.py 
        aio/scheme.py 
        aio/table.py 
        tornado/__init__.py
        tornado/tornado_helpers.py
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
