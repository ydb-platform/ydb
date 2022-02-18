add_library(cpp-actors-testlib)
target_link_libraries(cpp-actors-testlib PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  actors-interconnect-mock
  cpp-actors-protos
  library-cpp-random_provider
  library-cpp-time_provider
)
target_sources(cpp-actors-testlib PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/testlib/test_runtime.cpp
)
