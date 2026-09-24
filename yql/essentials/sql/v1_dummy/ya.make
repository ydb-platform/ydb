LIBRARY()

PROVIDES(
    yql_v1_sql_translator
)

SRCS(
    v1_sql_dummy.cpp
)

PEERDIR(
    yql/essentials/sql/settings
    yql/essentials/sql/v1/translator_iface
)

END()
