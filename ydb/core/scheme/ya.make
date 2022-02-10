LIBRARY()

OWNER(
    ddoarn
    vvvv
    g:kikimr
)

SRCS(
    scheme_borders.cpp
    scheme_tablecell.cpp
    scheme_tabledefs.cpp
    scheme_types_defs.cpp
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
    ydb/public/lib/scheme_types 
)

END()
 
RECURSE_FOR_TESTS( 
    ut 
) 
