add_library(core-kqp-proxy)
target_compile_options(core-kqp-proxy PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-proxy PUBLIC
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
)
target_sources(core-kqp-proxy PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/proxy/kqp_proxy_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/proxy/kqp_proxy_peer_stats_calculator.cpp
)
