PROGRAM(datashard_read_memory_bench)

ALLOCATOR(TCMALLOC_256K)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/getopt
    ydb/core/scheme
    ydb/core/tx/datashard
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
