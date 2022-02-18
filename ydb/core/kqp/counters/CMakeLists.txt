add_library(core-kqp-counters)
target_link_libraries(core-kqp-counters PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
  core-sys_view-service
)
target_sources(core-kqp-counters PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/counters/kqp_counters.cpp
)
