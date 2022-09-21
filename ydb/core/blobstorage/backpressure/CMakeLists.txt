
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)
add_subdirectory(ut_client)

add_library(core-blobstorage-backpressure)
target_link_libraries(core-blobstorage-backpressure PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  cpp-actors-core
  cpp-containers-intrusive_rb_tree
  ydb-core-base
  core-blobstorage-groupinfo
  core-blobstorage-lwtrace_probes
  ydb-core-protos
)
target_sources(core-blobstorage-backpressure PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/backpressure/event.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/backpressure/queue.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/backpressure/queue_backpressure_client.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/backpressure/unisched.cpp
)
