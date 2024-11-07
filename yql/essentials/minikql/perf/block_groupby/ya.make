PROGRAM()

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/minikql/arrow
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
	library/cpp/getopt
)

SRCS(
    block_groupby.cpp
)

YQL_LAST_ABI_VERSION()

END()
