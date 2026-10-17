LIBRARY()

SRCDIR(yql/essentials/parser/pg_wrapper/interface)

SRCS(
    type_desc.h  # gn: slot yql_pg_runtime
)

PEERDIR(
    util
)

END()
