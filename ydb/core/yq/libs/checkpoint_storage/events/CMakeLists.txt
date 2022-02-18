add_library(libs-checkpoint_storage-events)
target_link_libraries(libs-checkpoint_storage-events PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-interconnect
  yq-libs-checkpointing_common
  yq-libs-events
  libs-checkpoint_storage-proto
  yql-public-issue
)
target_sources(libs-checkpoint_storage-events PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpoint_storage/events/events.cpp
)
