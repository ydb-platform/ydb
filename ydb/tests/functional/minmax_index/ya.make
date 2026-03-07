IF (NOT SANITIZER_TYPE)

PY3TEST()

TEST_SRCS(test.py)

SIZE(MEDIUM)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(YDB_ENDPOINT="vla5-7716.search.yandex.net")
ENV(YDB_DATABASE="/Root/testdb")

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ELSE()
    REQUIREMENTS(ram:16)
ENDIF()

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    ydb/public/sdk/python
    contrib/python/PyHamcrest
)


FORK_SUBTESTS()
END()

ENDIF()
