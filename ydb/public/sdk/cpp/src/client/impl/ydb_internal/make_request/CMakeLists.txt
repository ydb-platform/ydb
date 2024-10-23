_ydb_sdk_add_library(impl-ydb_internal-make_request)

target_link_libraries(impl-ydb_internal-make_request PUBLIC
  yutil
  protobuf::libprotobuf
  api-protos
)

target_sources(impl-ydb_internal-make_request PRIVATE
  make.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-make_request)
