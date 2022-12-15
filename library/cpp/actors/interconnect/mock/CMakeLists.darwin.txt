
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(actors-interconnect-mock)
target_link_libraries(actors-interconnect-mock PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-interconnect
)
target_sources(actors-interconnect-mock PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/interconnect/mock/ic_mock.cpp
)
