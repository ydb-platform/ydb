
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

add_library(blobstorage-vdisk-query)
target_link_libraries(blobstorage-vdisk-query PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-streams-bzip2
  ydb-core-base
  core-blobstorage-base
  vdisk-hulldb-barriers
  vdisk-hulldb-base
)
target_sources(blobstorage-vdisk-query PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/query/query_barrier.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/query/query_extr.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/query/query_public.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/query/query_range.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/query/query_readactor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/query/query_readbatch.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/query/query_statdb.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/query/query_stathuge.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/query/query_stattablet.cpp
)
