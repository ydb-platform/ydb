add_library(providers-common-transform)
target_link_libraries(providers-common-transform PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-ast
  library-yql-utils
  yql-utils-log
  library-yql-core
  yql-core-expr_nodes
)
target_sources(providers-common-transform PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/transform/yql_exec.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/transform/yql_lazy_init.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/transform/yql_optimize.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/transform/yql_visit.cpp
)
