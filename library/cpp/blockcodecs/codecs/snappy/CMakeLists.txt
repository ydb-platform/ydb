add_library(blockcodecs-codecs-snappy INTERFACE)
target_link_libraries(blockcodecs-codecs-snappy INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-snappy
  cpp-blockcodecs-core
)

add_global_library_for(blockcodecs-codecs-snappy.global blockcodecs-codecs-snappy)
target_link_libraries(blockcodecs-codecs-snappy.global INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-snappy
  cpp-blockcodecs-core
)
target_sources(blockcodecs-codecs-snappy.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs/snappy/snappy.cpp
)
