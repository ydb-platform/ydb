PY3_PROGRAM() 

OWNER(
    spuchin
    g:yql_ydb_core
)

PY_SRCS(__main__.py)

PEERDIR(
    contrib/python/MarkupSafe
    contrib/python/Jinja2
)

END()
