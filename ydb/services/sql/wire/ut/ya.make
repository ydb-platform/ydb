UNITTEST()

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ENDIF()

SRCS(
    columns_ut.cpp
    constraints_ut.cpp
    create_table_ut.cpp
    delete_ut.cpp
    returning_ut.cpp
    tx_ut.cpp
    update_ut.cpp
    extended_ut.cpp
    insert_ut.cpp
    listener_ut.cpp
    schemata_ut.cpp
    sequences_ut.cpp
    select_ut.cpp
    set_ut.cpp
    session_ut.cpp
)

PEERDIR(
    ydb/core/testlib/default
    ydb/services/sql/wire
)

DATA(
    arcadia/contrib/python/pg8000
    arcadia/contrib/python/scramp
    arcadia/contrib/python/asn1crypto
    arcadia/contrib/python/python-dateutil/py3
    arcadia/contrib/python/six/py3
)

YQL_LAST_ABI_VERSION()

END()
