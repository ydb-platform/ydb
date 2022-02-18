add_library(ydb-library-mkql_proto)
target_compile_options(ydb-library-mkql_proto PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(ydb-library-mkql_proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-mkql_proto-protos
  mkql_proto-ut-helpers
  api-protos
  library-yql-minikql
  yql-minikql-computation
)
target_sources(ydb-library-mkql_proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/mkql_proto/mkql_proto.cpp
)
