LIBRARY()

SRCS(
    scheme_borders.cpp
    scheme_tablecell.cpp
    scheme_tabledefs.cpp
    scheme_types_defs.cpp
    scheme_type_info.cpp
    scheme_types_proto.cpp
    scheme_pathid.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/charset
    library/cpp/containers/bitseq
    library/cpp/deprecated/enum_codegen
    library/cpp/yson
    ydb/core/scheme/protos
    ydb/core/scheme_types
    ydb/library/aclib
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/public/lib/scheme_types
    # temporary.
    ydb/library/pretty_types_print/protobuf
    library/cpp/lwtrace/mon
)

END()

RECURSE_FOR_TESTS(
    ut
)
