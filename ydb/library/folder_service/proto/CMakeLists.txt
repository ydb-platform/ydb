add_library(library-folder_service-proto)
target_link_libraries(library-folder_service-proto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(library-folder_service-proto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/folder_service/proto/config.proto
  ${CMAKE_SOURCE_DIR}/ydb/library/folder_service/proto/folder_service.proto
)
