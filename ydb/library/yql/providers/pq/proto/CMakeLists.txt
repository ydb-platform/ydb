add_library(providers-pq-proto)
target_link_libraries(providers-pq-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(providers-pq-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/proto/dq_io.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/proto/dq_io_state.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/proto/dq_task_params.proto
)
target_proto_plugin(providers-pq-proto
  cpp_styleguide
  cpp_styleguide
)
