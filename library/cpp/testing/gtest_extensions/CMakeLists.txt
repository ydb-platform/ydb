add_library(cpp-testing-gtest_extensions)
target_link_libraries(cpp-testing-gtest_extensions PUBLIC
  contrib-libs-cxxsupp
  yutil
  restricted-googletest-googlemock
  restricted-googletest-googletest
)
target_sources(cpp-testing-gtest_extensions PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/gtest_extensions/assertions.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/gtest_extensions/gtest_extensions.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/gtest_extensions/matchers.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/gtest_extensions/pretty_printers.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/gtest_extensions/probe.cpp
)
