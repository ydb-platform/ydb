add_library(public-lib-yq)
target_link_libraries(public-lib-yq PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
  api-grpc-draft
  cpp-client-ydb_table
)
target_sources(public-lib-yq PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/yq/yq.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/yq/scope.cpp
)
