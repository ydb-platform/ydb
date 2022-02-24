find_package(ZLIB REQUIRED)

add_library(monlib-encode-spack)
target_link_libraries(monlib-encode-spack PUBLIC
  contrib-libs-cxxsupp
  yutil
  monlib-encode-buffered
  cpp-monlib-exception
  contrib-libs-lz4
  contrib-libs-xxhash
  ZLIB::ZLIB
  contrib-libs-zstd
)
target_sources(monlib-encode-spack PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/spack/spack_v1_decoder.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/spack/spack_v1_encoder.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/spack/varint.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/encode/spack/compression.cpp
)
