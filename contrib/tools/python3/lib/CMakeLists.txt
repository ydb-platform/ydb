
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(tools-python3-lib INTERFACE)
target_link_libraries(tools-python3-lib INTERFACE
  contrib-libs-cxxsupp
  yutil
  tools-python3-src
  contrib-libs-cxxsupp
  yutil
  certs
  contrib-libs-cxxsupp
  yutil
  python3-src-Modules
)
