add_library(library-cpp-blockcodecs)
target_link_libraries(library-cpp-blockcodecs PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-blockcodecs-core
  blockcodecs-codecs-brotli
  blockcodecs-codecs-bzip
  blockcodecs-codecs-fastlz
  blockcodecs-codecs-legacy_zstd06
  blockcodecs-codecs-lz4
  blockcodecs-codecs-lzma
  blockcodecs-codecs-snappy
  blockcodecs-codecs-zlib
  blockcodecs-codecs-zstd
)
target_sources(library-cpp-blockcodecs PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/stream.cpp
)
