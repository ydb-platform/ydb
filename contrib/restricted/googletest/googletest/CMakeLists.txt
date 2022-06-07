
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-googletest-googletest)
target_compile_options(restricted-googletest-googletest PUBLIC
  -DGTEST_HAS_STD_WSTRING=1
)
target_include_directories(restricted-googletest-googletest PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest/include
)
target_link_libraries(restricted-googletest-googletest PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-re2
)
target_sources(restricted-googletest-googletest PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest/src/gtest-all.cc
)
