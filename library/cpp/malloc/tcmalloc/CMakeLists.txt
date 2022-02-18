add_library(cpp-malloc-tcmalloc)
target_link_libraries(cpp-malloc-tcmalloc PUBLIC
  contrib-libs-cxxsupp
  cpp-malloc-api
  libs-tcmalloc-malloc_extension
)
target_sources(cpp-malloc-tcmalloc PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/malloc/tcmalloc/malloc-info.cpp
)
