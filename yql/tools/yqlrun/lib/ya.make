LIBRARY()

SRCS(
    yqlrun_lib.cpp
)

PEERDIR(
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/gateway/file

    yql/essentials/providers/common/provider
    yql/essentials/core/cbo
    yql/essentials/core/peephole_opt
    yql/essentials/core/cbo/simple
    yql/essentials/core/services

    yql/essentials/tools/yql_facade_run

)

IF (NOT OPENSOURCE)
    PEERDIR(yql/spark/tools/tool_lib)
ELSE()
    CFLAGS(-DDONT_ADD_SPARK)
ENDIF()

YQL_LAST_ABI_VERSION()

END()
