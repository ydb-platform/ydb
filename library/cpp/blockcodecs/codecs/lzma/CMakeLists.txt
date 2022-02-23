add_library(blockcodecs-codecs-lzma INTERFACE)
target_link_libraries(blockcodecs-codecs-lzma INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-lzmasdk
  cpp-blockcodecs-core
)

add_global_library_for(blockcodecs-codecs-lzma.global blockcodecs-codecs-lzma)
target_link_libraries(blockcodecs-codecs-lzma.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-lzmasdk
  cpp-blockcodecs-core
)
target_sources(blockcodecs-codecs-lzma.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs/lzma/lzma.cpp
)
