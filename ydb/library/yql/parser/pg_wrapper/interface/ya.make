LIBRARY()

SRCS(
    codec.h
    compare.h
    comp_factory.h
    context.h
    interface.h
    interface.cpp
    pack.h
    parser.h
    type_desc.h
    utils.h
)

PEERDIR(
    util
    ydb/library/yql/ast
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/arrow
)

YQL_LAST_ABI_VERSION()

END()
