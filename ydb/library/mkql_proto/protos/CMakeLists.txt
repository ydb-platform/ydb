add_library(library-mkql_proto-protos)
set_property(TARGET library-mkql_proto-protos PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
target_link_libraries(library-mkql_proto-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  contrib-libs-protobuf
)
target_proto_messages(library-mkql_proto-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/mkql_proto/protos/minikql.proto
)
target_proto_plugin(library-mkql_proto-protos
  grpc_cpp
  grpc_cpp
)
