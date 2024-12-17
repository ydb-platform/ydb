RECURSE(
    bin
)

PY23_LIBRARY()

PY_SRCS(
    __init__.py
    base.py
    configurator_setup.py
    dynamic.py
    static.py
    templates.py
    types.py
    utils.py
    validation.py
)

IF (PYTHON2)
    PEERDIR(
        contrib/deprecated/python/enum34
    )
ENDIF()

PEERDIR(
    contrib/python/protobuf
    contrib/python/PyYAML
    contrib/python/jsonschema
    contrib/python/requests
    contrib/python/six
    ydb/tools/cfg/walle
    library/cpp/resource
    library/python/resource
    ydb/core/protos
)

END()
