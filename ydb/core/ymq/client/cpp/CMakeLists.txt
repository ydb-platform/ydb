add_library(ymq-client-cpp)
target_link_libraries(ymq-client-cpp PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-protos
  lib-deprecated-client
)
target_sources(ymq-client-cpp PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/client/cpp/client.cpp
)
