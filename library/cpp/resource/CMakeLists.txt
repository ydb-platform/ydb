add_library(library-cpp-resource)
target_link_libraries(library-cpp-resource PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-blockcodecs-core
  blockcodecs-codecs-zstd
)
target_sources(library-cpp-resource PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/resource/registry.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/resource/resource.cpp
)
