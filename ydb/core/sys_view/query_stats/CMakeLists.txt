add_library(core-sys_view-query_stats)
target_compile_options(core-sys_view-query_stats PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-sys_view-query_stats PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  core-kqp-runtime
  core-sys_view-common
  core-sys_view-service
)
target_sources(core-sys_view-query_stats PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/query_stats/query_metrics.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/query_stats/query_stats.cpp
)
