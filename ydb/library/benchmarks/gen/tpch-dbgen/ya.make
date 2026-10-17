LIBRARY()

CONLYFLAGS(
    -Wno-deprecated-non-prototype
    -Wno-format
    -Wno-misleading-indentation
    -Wno-missing-field-initializers
    -Wno-string-plus-int
    -Wno-unused-but-set-variable
    -Wno-unused-parameter
    -Wno-unused-variable
    -Wno-void-pointer-to-int-cast
    -Wno-int-to-void-pointer-cast
)

IF (OS_MACOS OR OS_DARWIN)
    CONLYFLAGS(-D_POSIX_SOURCE -DLINUX)
ELSEIF (OS_WINDOWS)
    CXXFLAGS(-D_POSIX_)
ELSEIF (OS_LINUX)
    CONLYFLAGS(-D_POSIX_SOURCE -DLINUX)
ENDIF()

CONLYFLAGS(GLOBAL -DVECTORWISE GLOBAL -DTPCH GLOBAL -DRNG_TEST)

SRCS(
    build.c  # gn: into ydb/library/workload/tpch
    bm_utils.c  # gn: into ydb/library/workload/tpch
    rnd.c  # gn: into ydb/library/workload/tpch
    print.c  # gn: into ydb/library/workload/tpch
    load_stub.c  # gn: into ydb/library/workload/tpch
    bcd2.c  # gn: into ydb/library/workload/tpch
    speed_seed.c  # gn: into ydb/library/workload/tpch
    text.c  # gn: into ydb/library/workload/tpch
    permute.c  # gn: into ydb/library/workload/tpch
    rng64.c  # gn: into ydb/library/workload/tpch
)

END()
