LIBRARY()

SRCS(
    scheme_borders.cpp
    scheme_tablecell.cpp
    scheme_tabledefs.cpp
    scheme_types_defs.cpp
    scheme_type_info.cpp
    scheme_types_proto.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/charset
    library/cpp/containers/bitseq
    library/cpp/deprecated/enum_codegen
    library/cpp/yson
    ydb/core/base
    ydb/core/scheme_types
    ydb/core/util
    ydb/library/aclib
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/public/lib/scheme_types
)

END()

RECURSE_FOR_TESTS(
    ut
)
