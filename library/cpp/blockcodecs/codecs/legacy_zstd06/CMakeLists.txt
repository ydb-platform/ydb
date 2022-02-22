add_library(blockcodecs-codecs-legacy_zstd06 INTERFACE)
target_link_libraries(blockcodecs-codecs-legacy_zstd06 INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-zstd06
  cpp-blockcodecs-core
)

add_global_library_for(blockcodecs-codecs-legacy_zstd06.global blockcodecs-codecs-legacy_zstd06)
target_link_libraries(blockcodecs-codecs-legacy_zstd06.global INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-zstd06
  cpp-blockcodecs-core
)
target_sources(blockcodecs-codecs-legacy_zstd06.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs/legacy_zstd06/legacy_zstd06.cpp
)
