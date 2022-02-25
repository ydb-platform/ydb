add_library(core-ymq-proto)
target_link_libraries(core-ymq-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-protos
  contrib-libs-protobuf
)
target_proto_messages(core-ymq-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/proto/records.proto
)
