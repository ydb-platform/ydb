add_library(providers-dq-opt)
target_compile_options(providers-dq-opt PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-dq-opt PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-utils-log
  yql-dq-opt
  yql-dq-type_ann
  providers-common-transform
  providers-dq-expr_nodes
)
target_sources(providers-dq-opt PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/opt/dqs_opt.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/opt/logical_optimize.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/opt/physical_optimize.cpp
)
