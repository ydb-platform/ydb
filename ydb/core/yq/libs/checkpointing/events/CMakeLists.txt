add_library(libs-checkpointing-events)
target_link_libraries(libs-checkpointing-events PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-interconnect
  yq-libs-checkpointing_common
)
target_sources(libs-checkpointing-events PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpointing/events/events.cpp
)
