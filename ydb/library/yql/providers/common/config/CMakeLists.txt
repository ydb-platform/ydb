add_library(providers-common-config)
target_compile_options(providers-common-config PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-common-config PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-core
  yql-core-expr_nodes
  library-yql-ast
  cpp-containers-sorted_vector
  cpp-string_utils-parse_size
  cpp-string_utils-levenshtein_diff
)
target_sources(providers-common-config PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/config/yql_configuration_transformer.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/config/yql_dispatch.cpp
)
