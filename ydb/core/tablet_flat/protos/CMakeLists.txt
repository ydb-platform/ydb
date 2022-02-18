add_library(core-tablet_flat-protos)
target_link_libraries(core-tablet_flat-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  ydb-core-protos
)
target_proto_messages(core-tablet_flat-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tablet_flat/protos/flat_table_part.proto
  ${CMAKE_SOURCE_DIR}/ydb/core/tablet_flat/protos/flat_table_shard.proto
)
