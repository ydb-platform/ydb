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
    yql/essentials/ast
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/core/cbo
    library/cpp/disjoint_sets
    yql/essentials/providers/common/codec/yt_arrow_converter_interface
)

YQL_LAST_ABI_VERSION()

END()
