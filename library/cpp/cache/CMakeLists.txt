add_library(library-cpp-cache)
target_link_libraries(library-cpp-cache PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-cache PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/cache/cache.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/cache/thread_safe_cache.cpp
)
