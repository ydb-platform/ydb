add_library(cpp-actors-util)
target_link_libraries(cpp-actors-util PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-actors-util PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/util/affinity.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/util/should_continue.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/util/threadparkpad.cpp
)
