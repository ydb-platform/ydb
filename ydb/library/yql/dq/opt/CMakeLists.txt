add_library(yql-dq-opt)
target_compile_options(yql-dq-opt PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-dq-opt PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-ast
  library-yql-core
  yql-dq-common
  yql-dq-expr_nodes
  yql-dq-proto
  yql-dq-type_ann
)
target_sources(yql-dq-opt PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/opt/dq_opt.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/opt/dq_opt_build.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/opt/dq_opt_join.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/opt/dq_opt_log.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/opt/dq_opt_peephole.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/opt/dq_opt_phy_finalizing.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/opt/dq_opt_phy.cpp
)
