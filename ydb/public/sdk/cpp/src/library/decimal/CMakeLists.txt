_ydb_sdk_add_library(yql-public-decimal)

target_link_libraries(yql-public-decimal PUBLIC
  yutil
)

target_sources(yql-public-decimal
  PRIVATE
    yql_decimal.cpp
)

_ydb_sdk_install_targets(TARGETS yql-public-decimal)
