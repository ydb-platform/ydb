add_library(core-kqp-rm)
target_compile_options(core-kqp-rm PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-rm PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-actorlib_impl
  ydb-core-base
  core-cms-console
  core-kqp-common
  core-kqp-counters
  ydb-core-mind
  ydb-core-mon
  ydb-core-protos
  ydb-core-tablet
)
target_sources(core-kqp-rm PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/rm/kqp_resource_estimation.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/rm/kqp_snapshot_manager.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/rm/kqp_rm.cpp
)
