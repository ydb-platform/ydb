_ydb_sdk_add_library(proto_output)

target_link_libraries(proto_output PUBLIC
  yutil
  api-protos
  protobuf::libprotobuf
)

target_sources(proto_output PRIVATE
  proto_output.cpp
)

_ydb_sdk_install_targets(TARGETS proto_output)

