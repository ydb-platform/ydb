add_library(cpp-testing-gmock_in_unittest)
target_link_libraries(cpp-testing-gmock_in_unittest PUBLIC
  contrib-libs-cxxsupp
  yutil
  restricted-googletest-googlemock
  restricted-googletest-googletest
  cpp-testing-gtest_extensions
  cpp-testing-unittest
)
target_sources(cpp-testing-gmock_in_unittest PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/gmock_in_unittest/events.cpp
)

add_global_library_for(cpp-testing-gmock_in_unittest.global cpp-testing-gmock_in_unittest)
target_link_libraries(cpp-testing-gmock_in_unittest.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  restricted-googletest-googlemock
  restricted-googletest-googletest
  cpp-testing-gtest_extensions
  cpp-testing-unittest
)
target_sources(cpp-testing-gmock_in_unittest.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/gmock_in_unittest/registration.cpp
)
