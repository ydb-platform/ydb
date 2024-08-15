_ydb_sdk_add_library(impl-ydb_internal-common)

target_link_libraries(impl-ydb_internal-common PUBLIC
  yutil
  grpc-client
  yql-public-issue
)

target_sources(impl-ydb_internal-common PRIVATE
  parser.cpp
  getenv.cpp
  client_pid.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-common)
