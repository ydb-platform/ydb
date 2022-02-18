add_library(client-extensions-discovery_mutator)
target_link_libraries(client-extensions-discovery_mutator PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-client-ydb_extension
)
target_sources(client-extensions-discovery_mutator PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/extensions/discovery_mutator/discovery_mutator.cpp
)
