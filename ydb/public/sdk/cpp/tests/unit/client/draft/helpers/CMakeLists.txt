_ydb_sdk_add_library(client-draft_ut_helpers)

target_link_libraries(client-draft_ut_helpers
  PRIVATE
    api-grpc
    api-grpc-draft
)

target_sources(client-draft_ut_helpers
  PRIVATE
    grpc_services/scripting.cpp
    grpc_services/view.cpp
)
