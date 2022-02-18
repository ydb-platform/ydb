add_library(core-kqp-node)
target_compile_options(core-kqp-node PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-node PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-actorlib_impl
  ydb-core-base
  core-cms-console
  core-kqp-common
  core-kqp-counters
  ydb-core-mind
  ydb-core-protos
  ydb-core-tablet
  dq-actors-compute
)
target_sources(core-kqp-node PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/node/kqp_node.cpp
)
