add_library(client-ydb_types-fatal_error_handlers)
target_link_libraries(client-ydb_types-fatal_error_handlers PUBLIC
  contrib-libs-cxxsupp
  yutil
  client-ydb_types-exceptions
)
target_sources(client-ydb_types-fatal_error_handlers PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers/handlers.cpp
)
