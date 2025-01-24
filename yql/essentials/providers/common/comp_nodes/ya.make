LIBRARY()

SRCS(
    yql_factory.h
    yql_factory.cpp
    yql_formatcode.h
    yql_formatcode.cpp
    yql_formattype.cpp
    yql_formattype.h
    yql_formattypediff.cpp
    yql_formattypediff.h
    yql_makecode.h
    yql_makecode.cpp
    yql_maketype.h
    yql_maketype.cpp
    yql_parsetypehandle.h
    yql_parsetypehandle.cpp
    yql_position.cpp
    yql_position.h
    yql_reprcode.h
    yql_reprcode.cpp
    yql_serializetypehandle.h
    yql_serializetypehandle.cpp
    yql_splittype.h
    yql_splittype.cpp
    yql_type_resource.cpp
    yql_type_resource.h
    yql_typehandle.cpp
    yql_typehandle.h
    yql_typekind.cpp
    yql_typekind.h
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/ast/serialize
    yql/essentials/minikql/computation
    yql/essentials/core
    yql/essentials/core/type_ann
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/schema/expr
    yql/essentials/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

END()
