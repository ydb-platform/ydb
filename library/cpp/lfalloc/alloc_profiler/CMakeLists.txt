add_library(cpp-lfalloc-alloc_profiler)
target_link_libraries(cpp-lfalloc-alloc_profiler PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-lfalloc-dbg_info
  library-cpp-cache
)
target_sources(cpp-lfalloc-alloc_profiler PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/lfalloc/alloc_profiler/profiler.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/lfalloc/alloc_profiler/stackcollect.cpp
)
