_ydb_sdk_add_library(yql-utils)

target_link_libraries(yql-utils PUBLIC
  yutil
  string_utils-quote
  threading-future
)

target_sources(yql-utils PRIVATE
  utf8.cpp
)

_ydb_sdk_install_targets(TARGETS yql-utils)
