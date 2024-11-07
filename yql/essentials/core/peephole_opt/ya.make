LIBRARY()

SRCS(
    yql_opt_json_peephole_physical.h
    yql_opt_json_peephole_physical.cpp
    yql_opt_peephole_physical.h
    yql_opt_peephole_physical.cpp
)

PEERDIR(
    yql/essentials/core/sql_types
    yql/essentials/core
    yql/essentials/core/common_opt
    yql/essentials/core/type_ann
    library/cpp/svnversion
)

YQL_LAST_ABI_VERSION()

END()
