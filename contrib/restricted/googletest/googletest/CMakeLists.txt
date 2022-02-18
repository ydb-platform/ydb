add_library(restricted-googletest-googletest)
target_compile_options(restricted-googletest-googletest PUBLIC
  -DGTEST_HAS_ABSL=0
  -DGTEST_OS_FUCHSIA=0
  -DGTEST_HAS_STD_WSTRING=1
)
target_include_directories(restricted-googletest-googletest PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest/include
)
target_include_directories(restricted-googletest-googletest PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest/include
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest
)
target_link_libraries(restricted-googletest-googletest PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-re2
)
target_sources(restricted-googletest-googletest PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest/src/gtest-all.cc
)
