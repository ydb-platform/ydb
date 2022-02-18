add_library(cpp-actors-helpers)
target_link_libraries(cpp-actors-helpers PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-monlib-dynamic_counters
)
target_sources(cpp-actors-helpers PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/helpers/activeactors.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/helpers/flow_controlled_queue.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/helpers/selfping_actor.cpp
)
