add_library(common-token_accessor-grpc)
set_property(TARGET common-token_accessor-grpc PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
target_link_libraries(common-token_accessor-grpc PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-protobuf
)
target_proto_messages(common-token_accessor-grpc PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/common/token_accessor/grpc/token_accessor_pb.proto
)
target_proto_plugin(common-token_accessor-grpc
  cpp_styleguide
  cpp_styleguide
)
target_proto_plugin(common-token_accessor-grpc
  grpc_cpp
  grpc_cpp
)
