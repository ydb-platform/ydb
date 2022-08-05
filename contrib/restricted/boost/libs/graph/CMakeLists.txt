
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(boost-libs-graph INTERFACE)
target_link_libraries(boost-libs-graph INTERFACE
  contrib-libs-cxxsupp
  contrib-restricted-boost
  restricted-boost-atomic
  restricted-boost-exception
  boost-libs-filesystem
)
