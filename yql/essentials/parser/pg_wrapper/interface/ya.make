YQL_LIBRARY()

SRCS(
    codec.h
    compare.h
    comp_factory.h
    context.h
    interface.h
    interface.cpp
    pack.h
    parser.h
    in_range.h
    sign.h
    utils.h
)

PEERDIR(
    util
    yql/essentials/parser/pg_wrapper/interface/type_desc
    yql/essentials/ast
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/core/cbo
    library/cpp/disjoint_sets
    yql/essentials/providers/common/codec/yt_arrow_converter_interface
)


END()

RECURSE(
    type_desc
)
