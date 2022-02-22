add_library(blockcodecs-codecs-zstd INTERFACE)
target_link_libraries(blockcodecs-codecs-zstd INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-zstd
  cpp-blockcodecs-core
)

add_global_library_for(blockcodecs-codecs-zstd.global blockcodecs-codecs-zstd)
target_link_libraries(blockcodecs-codecs-zstd.global INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-zstd
  cpp-blockcodecs-core
)
target_sources(blockcodecs-codecs-zstd.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs/zstd/zstd.cpp
)
