_ydb_sdk_add_library(impl-ydb_internal-plain_status)

target_link_libraries(impl-ydb_internal-plain_status PUBLIC
  yutil
  protobuf::libprotobuf
  grpc-client
  yql-public-issue
)

target_sources(impl-ydb_internal-plain_status PRIVATE
  status.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-plain_status)
