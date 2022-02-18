add_library(core-sys_view-partition_stats)
target_compile_options(core-sys_view-partition_stats PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-sys_view-partition_stats PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  core-kqp-runtime
  core-sys_view-common
)
target_sources(core-sys_view-partition_stats PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/partition_stats/partition_stats.cpp
)
