add_library(core-grpc_services-counters)
target_compile_options(core-grpc_services-counters PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-grpc_services-counters PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
  core-sys_view-service
)
target_sources(core-grpc_services-counters PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_services/counters/counters.cpp
)
