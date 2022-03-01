
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(library-cpp-ipv6_address)
target_link_libraries(library-cpp-ipv6_address PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-int128
)
target_sources(library-cpp-ipv6_address PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/ipv6_address/ipv6_address.cpp
)
