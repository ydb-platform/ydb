add_library(core-tx-sequenceproxy)
target_link_libraries(core-tx-sequenceproxy PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-tx-scheme_cache
  tx-sequenceproxy-public
  tx-sequenceshard-public
)
target_sources(core-tx-sequenceproxy PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/sequenceproxy/sequenceproxy.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/sequenceproxy/sequenceproxy_allocate.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/sequenceproxy/sequenceproxy_impl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/sequenceproxy/sequenceproxy_resolve.cpp
)
