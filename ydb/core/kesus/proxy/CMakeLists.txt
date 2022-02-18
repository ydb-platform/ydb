add_library(core-kesus-proxy)
target_link_libraries(core-kesus-proxy PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-erasure
  core-kesus-tablet
  ydb-core-scheme
  core-tx-scheme_cache
  api-protos
)
target_sources(core-kesus-proxy PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kesus/proxy/events.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kesus/proxy/proxy.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kesus/proxy/proxy_actor.cpp
)
