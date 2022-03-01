
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(restricted-googletest-googlemock)
target_compile_options(restricted-googletest-googlemock PUBLIC
  -DGTEST_HAS_ABSL=0
  -DGTEST_OS_FUCHSIA=0
  -DGTEST_HAS_STD_WSTRING=1
)
target_include_directories(restricted-googletest-googlemock PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googlemock/include
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest/include
)
target_include_directories(restricted-googletest-googlemock PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googlemock/include
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest/include
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googlemock
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googletest
)
target_link_libraries(restricted-googletest-googlemock PUBLIC
  contrib-libs-cxxsupp
  restricted-googletest-googletest
)
target_sources(restricted-googletest-googlemock PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/googletest/googlemock/src/gmock-all.cc
)
