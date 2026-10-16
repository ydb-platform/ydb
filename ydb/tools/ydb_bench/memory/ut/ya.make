GTEST()

SRCDIR(ydb/tools/ydb_bench/memory)

REQUIREMENTS(cpu:1)
SRCS(
    main_ut.cpp
    options.cpp
)

END()
