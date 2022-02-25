add_library(api-protos-annotations)
target_link_libraries(api-protos-annotations PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(api-protos-annotations PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/api/protos/annotations/sensitive.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/api/protos/annotations/validation.proto
)
