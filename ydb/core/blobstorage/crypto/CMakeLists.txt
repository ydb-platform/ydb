add_library(core-blobstorage-crypto)
target_link_libraries(core-blobstorage-crypto PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-t1ha
  library-cpp-sse
)
target_sources(core-blobstorage-crypto PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/crypto/chacha.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/crypto/crypto.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/crypto/poly1305.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/crypto/chacha_vec.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/crypto/poly1305_vec.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/crypto/secured_block.cpp
)
