add_library(ydb-core-sys_view)
target_compile_options(ydb-core-sys_view PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(ydb-core-sys_view PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-kqp-runtime
  core-sys_view-common
  core-sys_view-nodes
  core-sys_view-partition_stats
  core-sys_view-query_stats
  core-sys_view-service
  core-sys_view-storage
  core-sys_view-tablets
)
target_sources(ydb-core-sys_view PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/scan.cpp
)
