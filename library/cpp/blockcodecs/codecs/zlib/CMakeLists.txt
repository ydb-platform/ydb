add_library(blockcodecs-codecs-zlib INTERFACE)
target_link_libraries(blockcodecs-codecs-zlib INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-blockcodecs-core
)

add_global_library_for(blockcodecs-codecs-zlib.global blockcodecs-codecs-zlib)
target_link_libraries(blockcodecs-codecs-zlib.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-blockcodecs-core
)
target_sources(blockcodecs-codecs-zlib.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs/zlib/zlib.cpp
)
