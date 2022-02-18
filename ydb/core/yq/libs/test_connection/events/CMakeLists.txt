add_library(libs-test_connection-events)
target_link_libraries(libs-test_connection-events PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-control_plane_storage-events
  yq-libs-events
  api-protos
  public-issue-protos
)
target_sources(libs-test_connection-events PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/test_connection/events/events.cpp
)
