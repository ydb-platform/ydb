add_library(blobstorage-pdisk-mock)
target_link_libraries(blobstorage-pdisk-mock PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  core-blobstorage-pdisk
)
target_sources(blobstorage-pdisk-mock PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/pdisk/mock/pdisk_mock.cpp
)
