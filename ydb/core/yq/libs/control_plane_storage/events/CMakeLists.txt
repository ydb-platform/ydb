add_library(libs-control_plane_storage-events)
target_link_libraries(libs-control_plane_storage-events PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-interconnect
  libs-control_plane_storage-proto
  yq-libs-events
)
target_sources(libs-control_plane_storage-events PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/control_plane_storage/events/events.cpp
)
