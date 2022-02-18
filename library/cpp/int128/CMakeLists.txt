add_library(library-cpp-int128)
target_link_libraries(library-cpp-int128 PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-int128 PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/int128/int128.cpp
)
