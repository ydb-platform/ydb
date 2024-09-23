LIBRARY()

SRCS(
    scheme_decimal_type.cpp
    scheme_type_metadata.cpp
    scheme_type_registry.cpp
    scheme_types_defs.cpp
)

PEERDIR(
    ydb/public/lib/scheme_types
)

END()
