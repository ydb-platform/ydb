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
