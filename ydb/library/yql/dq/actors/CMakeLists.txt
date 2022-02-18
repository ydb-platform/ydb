add_library(yql-dq-actors)
set_property(TARGET yql-dq-actors PROPERTY
  PROTOC_EXTRA_OUTS .grpc.pb.cc .grpc.pb.h
)
target_link_libraries(yql-dq-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
  dq-actors-protos
)
target_sources(yql-dq-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/dq.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/actors/dq_events_ids.cpp
)
target_proto_plugin(yql-dq-actors
  grpc_cpp
  grpc_cpp
)
