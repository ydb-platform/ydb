YQL_LIBRARY()

SRCS(
    arrow.h  # gn: slot yql_pg_runtime
    codec.h  # gn: slot yql_pg_runtime
    compare.h  # gn: slot yql_pg_runtime
    comp_factory.h  # gn: slot yql_pg_runtime
    config.h  # gn: slot yql_pg_runtime
    context.h  # gn: slot yql_pg_runtime
    interface.h
    interface.cpp
    optimizer.h  # gn: slot yql_pg_sql_translator
    pack.h  # gn: slot yql_pg_runtime
    parser.h  # gn: slot yql_pg_sql_translator
    in_range.h  # gn: slot yql_pg_runtime
    raw_parser.h  # gn: slot yql_pg_runtime
    sign.h  # gn: slot yql_pg_runtime
    utils.h  # gn: slot yql_pg_runtime
)
# gn: yql_pg_sql_translator headers optimizer.h parser.h

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
