add_library(cpp-testing-unittest)
target_link_libraries(cpp-testing-unittest PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-colorizer
  library-cpp-dbg_output
  library-cpp-diff
  cpp-json-writer
  cpp-testing-common
  cpp-testing-hook
)
target_sources(cpp-testing-unittest PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/unittest/gtest.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/unittest/checks.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/unittest/plugin.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/unittest/registar.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/unittest/tests_data.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/unittest/utmain.cpp
)
