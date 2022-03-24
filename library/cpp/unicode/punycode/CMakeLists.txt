
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


find_package(IDN REQUIRED)

add_library(cpp-unicode-punycode)
target_link_libraries(cpp-unicode-punycode PUBLIC
  contrib-libs-cxxsupp
  yutil
  IDN::IDN
  $CONAN_OPTS_SEM
)
target_sources(cpp-unicode-punycode PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/unicode/punycode/punycode.cpp
)
