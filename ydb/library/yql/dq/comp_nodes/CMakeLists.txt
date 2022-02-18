add_library(yql-dq-comp_nodes)
target_compile_options(yql-dq-comp_nodes PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-dq-comp_nodes PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  yql-minikql-computation
  library-yql-minikql
  dq-actors-compute
)
target_sources(yql-dq-comp_nodes PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/comp_nodes/yql_common_dq_transform.cpp
)
