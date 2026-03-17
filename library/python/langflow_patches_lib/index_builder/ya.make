PY3_PROGRAM(prepare_index)

NO_BUILD_IF(STRICT OS_WINDOWS MUSL)

PEERDIR(
    contrib/python/lfx
    contrib/python/orjson
)

PY_SRCS(
    __main__.py
    prepare_index.py
    components_whitelist.py
)

END()
