
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-blobstorage-base)
target_link_libraries(core-blobstorage-base PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-wilson
  library-cpp-lwtrace
  ydb-core-protos
)
target_sources(core-blobstorage-base PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/base/blobstorage_vdiskid.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/base/blobstorage_syncstate.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/base/html.cpp
)
