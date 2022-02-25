add_library(providers-dq-config)
target_link_libraries(providers-dq-config PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(providers-dq-config PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/dq/config/config.proto
)
