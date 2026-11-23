PY3_LIBRARY()

PY_SRCS(
    __init__.py
    placement.py
    journal.py
    server.py
    static_recovery.py
    storage.py
    workload.py
)

PEERDIR(contrib/python/PyYAML)

RESOURCE_FILES(
    PREFIX ydb/tools/dc_admin/
    index.html
)

END()

RECURSE(prepare_solo)

RECURSE_FOR_TESTS(tests)
