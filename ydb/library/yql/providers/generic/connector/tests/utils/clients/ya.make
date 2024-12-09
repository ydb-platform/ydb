PY3_LIBRARY()

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
ENDIF()

PY_SRCS(
    postgresql.py
)

PEERDIR(
    contrib/python/pg8000
    ydb/library/yql/providers/generic/connector/tests/utils
)

END()
