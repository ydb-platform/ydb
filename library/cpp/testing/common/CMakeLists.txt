add_library(cpp-testing-common)
target_link_libraries(cpp-testing-common PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
)
target_sources(cpp-testing-common PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/common/env.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/common/network.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/common/probe.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/common/scope.cpp
)
