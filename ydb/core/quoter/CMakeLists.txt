add_library(ydb-core-quoter)
target_link_libraries(ydb-core-quoter PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-containers-ring_buffer
  ydb-core-base
  core-kesus-tablet
  core-tx-scheme_cache
  ydb-core-util
  yql-public-issue
)
target_sources(ydb-core-quoter PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/quoter/debug_info.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/quoter/kesus_quoter_proxy.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/quoter/probes.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/quoter/quoter_service.cpp
)
