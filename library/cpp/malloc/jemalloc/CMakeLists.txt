add_library(cpp-malloc-jemalloc)
target_link_libraries(cpp-malloc-jemalloc PUBLIC
  contrib-libs-cxxsupp
  cpp-malloc-api
  contrib-libs-jemalloc
)
target_sources(cpp-malloc-jemalloc PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/malloc/jemalloc/malloc-info.cpp
)
