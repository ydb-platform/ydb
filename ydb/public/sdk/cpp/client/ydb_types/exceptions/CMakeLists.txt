add_library(client-ydb_types-exceptions)
target_link_libraries(client-ydb_types-exceptions PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(client-ydb_types-exceptions PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.cpp
)
