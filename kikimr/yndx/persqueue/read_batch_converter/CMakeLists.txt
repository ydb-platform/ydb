
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yndx-persqueue-read_batch_converter)
target_link_libraries(yndx-persqueue-read_batch_converter PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos-yndx
)
target_sources(yndx-persqueue-read_batch_converter PRIVATE
  ${CMAKE_SOURCE_DIR}/kikimr/yndx/persqueue/read_batch_converter/read_batch_converter.cpp
)
