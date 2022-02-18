add_library(blockcodecs-codecs-lz4 INTERFACE)
target_link_libraries(blockcodecs-codecs-lz4 INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-lz4
  libs-lz4-generated
  cpp-blockcodecs-core
)

add_global_library_for(blockcodecs-codecs-lz4.global blockcodecs-codecs-lz4)
target_sources(blockcodecs-codecs-lz4.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs/lz4/lz4.cpp
)
