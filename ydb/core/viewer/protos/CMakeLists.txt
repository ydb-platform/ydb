add_library(core-viewer-protos)
target_link_libraries(core-viewer-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-protos
  contrib-libs-protobuf
)
target_proto_messages(core-viewer-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/viewer/protos/viewer.proto
)
