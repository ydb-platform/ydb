add_library(blobstorage-dsproxy-mock)
target_link_libraries(blobstorage-dsproxy-mock PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-blobstorage
  blobstorage-vdisk-common
)
target_sources(blobstorage-dsproxy-mock PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/dsproxy/mock/dsproxy_mock.cpp
)
