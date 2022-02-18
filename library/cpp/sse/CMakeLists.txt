add_library(library-cpp-sse)
target_link_libraries(library-cpp-sse PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-sse PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/sse/sse.cpp
)
