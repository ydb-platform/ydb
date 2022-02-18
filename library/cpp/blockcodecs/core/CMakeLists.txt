add_library(cpp-blockcodecs-core)
target_link_libraries(cpp-blockcodecs-core PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-blockcodecs-core PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/core/codecs.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/blockcodecs/core/stream.cpp
)
