add_library(core-file_storage-proto)
target_link_libraries(core-file_storage-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(core-file_storage-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/file_storage/proto/file_storage.proto
)
