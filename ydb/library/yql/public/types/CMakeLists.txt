add_library(yql-public-types)
target_link_libraries(yql-public-types PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(yql-public-types PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/types/yql_types.proto
)
