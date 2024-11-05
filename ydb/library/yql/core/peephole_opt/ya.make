LIBRARY()

SRCS(
    yql_opt_json_peephole_physical.h
    yql_opt_json_peephole_physical.cpp
    yql_opt_peephole_physical.h
    yql_opt_peephole_physical.cpp
)

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/core/common_opt
    ydb/library/yql/core/type_ann
    library/cpp/svnversion
)

YQL_LAST_ABI_VERSION()

END()
