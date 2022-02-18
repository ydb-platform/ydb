add_library(dq-worker_manager-interface)
target_compile_options(dq-worker_manager-interface PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(dq-worker_manager-interface PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-interconnect
  yql-utils-log
  yql-dq-common
  providers-common-metrics
  dq-api-grpc
  dq-api-protos
)
target_sources(dq-worker_manager-interface PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/worker_manager/interface/events.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/worker_manager/interface/worker_info.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/worker_manager/interface/counters.cpp
)
