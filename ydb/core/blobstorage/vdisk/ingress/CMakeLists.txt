add_library(blobstorage-vdisk-ingress)
target_link_libraries(blobstorage-vdisk-ingress PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lwtrace
  library-cpp-pop_count
  ydb-core-base
  ydb-core-protos
)
target_sources(blobstorage-vdisk-ingress PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress_matrix.cpp
)
