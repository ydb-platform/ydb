add_library(api-grpc-draft)
set_property(TARGET api-grpc-draft PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
target_link_libraries(api-grpc-draft PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  api-protos
  contrib-libs-protobuf
)
target_proto_messages(api-grpc-draft PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/api/grpc/draft/dummy.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/api/grpc/draft/ydb_clickhouse_internal_v1.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/api/grpc/draft/ydb_persqueue_v1.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/api/grpc/draft/ydb_datastreams_v1.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/api/grpc/draft/ydb_experimental_v1.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/api/grpc/draft/ydb_s3_internal_v1.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/api/grpc/draft/ydb_long_tx_v1.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/api/grpc/draft/ydb_logstore_v1.proto
  ${CMAKE_SOURCE_DIR}/ydb/public/api/grpc/draft/yql_db_v1.proto
)
target_proto_plugin(api-grpc-draft
  grpc_cpp
  grpc_cpp
)
