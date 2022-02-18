add_library(core-client-minikql_result_lib)
target_link_libraries(core-client-minikql_result_lib PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  ydb-core-protos
  ydb-core-scheme
  library-mkql_proto-protos
  lib-deprecated-kicli
)
target_sources(core-client-minikql_result_lib PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/client/minikql_result_lib/objects.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/client/minikql_result_lib/converter.cpp
)
