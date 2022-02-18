add_library(core-blobstorage-base)
target_link_libraries(core-blobstorage-base PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lwtrace
  ydb-core-protos
  ydb-library-wilson
)
target_sources(core-blobstorage-base PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/base/blobstorage_vdiskid.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/base/blobstorage_syncstate.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/base/html.cpp
)
