add_library(cpp-testing-unittest_main)
target_link_libraries(cpp-testing-unittest_main PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-testing-unittest
  library-cpp-terminate_handler
)
target_sources(cpp-testing-unittest_main PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/testing/unittest_main/main.cpp
)
