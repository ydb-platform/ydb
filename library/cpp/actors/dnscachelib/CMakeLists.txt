
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


find_package(c-ares REQUIRED)

add_library(cpp-actors-dnscachelib)
target_link_libraries(cpp-actors-dnscachelib PUBLIC
  contrib-libs-cxxsupp
  yutil
  c-ares::c-ares
  library-cpp-lwtrace
)
target_sources(cpp-actors-dnscachelib PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/dnscachelib/dnscache.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/dnscachelib/probes.cpp
)
