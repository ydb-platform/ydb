LIBRARY()

PEERDIR(
    contrib/libs/re2
)

ADDINCL(
    GLOBAL library/cpp/clickhouse_deps/re2_st_stub/include
)

END()
