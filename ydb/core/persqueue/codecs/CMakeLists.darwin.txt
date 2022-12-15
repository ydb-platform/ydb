
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-persqueue-codecs)
target_link_libraries(core-persqueue-codecs PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos
  cpp-client-ydb_persqueue_core
)
target_sources(core-persqueue-codecs PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/persqueue/codecs/pqv1.cpp
)
