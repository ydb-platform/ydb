add_library(blockcodecs-codecs-fastlz INTERFACE)
target_link_libraries(blockcodecs-codecs-fastlz INTERFACE
  contrib-libs-cxxsupp
  yutil
  contrib-libs-fastlz
  cpp-blockcodecs-core
)

add_global_library_for(blockcodecs-codecs-fastlz.global blockcodecs-codecs-fastlz)
target_link_libraries(blockcodecs-codecs-fastlz.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-fastlz
  cpp-blockcodecs-core
)
target_sources(blockcodecs-codecs-fastlz.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/codecs/fastlz/fastlz.cpp
)
