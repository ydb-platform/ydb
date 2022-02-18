add_library(library-cpp-pop_count)
target_link_libraries(library-cpp-pop_count PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-pop_count PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/pop_count/popcount.cpp
)
