add_library(libs-audit-events)
target_link_libraries(libs-audit-events PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-interconnect
  yq-libs-checkpointing_common
  libs-control_plane_storage-events
  api-protos
  yql-public-issue
)
target_sources(libs-audit-events PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/audit/events/events.cpp
)
