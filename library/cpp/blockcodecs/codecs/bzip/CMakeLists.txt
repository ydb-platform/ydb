add_library(blockcodecs-codecs-bzip INTERFACE)
target_link_libraries(blockcodecs-codecs-bzip INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-libbz2
  cpp-blockcodecs-core
)

add_global_library_for(blockcodecs-codecs-bzip.global blockcodecs-codecs-bzip)
target_sources(blockcodecs-codecs-bzip.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs/bzip/bzip.cpp
)
