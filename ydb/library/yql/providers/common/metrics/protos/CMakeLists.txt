add_library(common-metrics-protos)
target_link_libraries(common-metrics-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(common-metrics-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/metrics/protos/metrics_registry.proto
)
target_proto_plugin(common-metrics-protos
  cpp_styleguide
  cpp_styleguide
)
