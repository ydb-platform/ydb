LIBRARY()

SRCS(
    yql_dq_full_result_writer.cpp
    yql_dq_task_preprocessor.cpp
)

PEERDIR(
    yql/essentials/public/udf
)

YQL_LAST_ABI_VERSION()

END()
