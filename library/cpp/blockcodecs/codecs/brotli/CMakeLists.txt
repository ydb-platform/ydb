add_library(blockcodecs-codecs-brotli INTERFACE)
target_link_libraries(blockcodecs-codecs-brotli INTERFACE
  contrib-libs-cxxsupp
  yutil
  libs-brotli-enc
  libs-brotli-dec
  cpp-blockcodecs-core
)

add_global_library_for(blockcodecs-codecs-brotli.global blockcodecs-codecs-brotli)
target_link_libraries(blockcodecs-codecs-brotli.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-brotli-enc
  libs-brotli-dec
  cpp-blockcodecs-core
)
target_sources(blockcodecs-codecs-brotli.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs/brotli/brotli.cpp
)
