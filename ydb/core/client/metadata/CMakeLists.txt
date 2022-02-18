add_library(core-client-metadata)
target_compile_options(core-client-metadata PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-client-metadata PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-protos
  ydb-core-scheme_types
  yql-minikql-invoke_builtins
)
target_sources(core-client-metadata PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/client/metadata/types_metadata.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/client/metadata/functions_metadata.cpp
)
