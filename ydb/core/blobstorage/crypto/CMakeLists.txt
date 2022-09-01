
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

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
