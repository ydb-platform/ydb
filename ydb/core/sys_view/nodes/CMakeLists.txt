add_library(core-sys_view-nodes)
target_compile_options(core-sys_view-nodes PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-sys_view-nodes PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  core-kqp-runtime
  core-sys_view-common
)
target_sources(core-sys_view-nodes PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/sys_view/nodes/nodes.cpp
)
