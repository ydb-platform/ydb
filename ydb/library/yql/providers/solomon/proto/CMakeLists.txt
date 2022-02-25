add_library(providers-solomon-proto)
target_link_libraries(providers-solomon-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(providers-solomon-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/solomon/proto/dq_solomon_shard.proto
)
