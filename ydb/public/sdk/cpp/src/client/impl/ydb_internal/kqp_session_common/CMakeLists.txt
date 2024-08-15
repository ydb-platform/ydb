_ydb_sdk_add_library(impl-ydb_internal-kqp_session_common)

target_link_libraries(impl-ydb_internal-kqp_session_common PUBLIC
  yutil
  threading-future
  lib-operation_id-protos
  client-impl-ydb_endpoints
)

target_sources(impl-ydb_internal-kqp_session_common PRIVATE
  kqp_session_common.cpp
)

_ydb_sdk_install_targets(TARGETS impl-ydb_internal-kqp_session_common)
