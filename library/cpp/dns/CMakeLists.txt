add_library(library-cpp-dns)
target_link_libraries(library-cpp-dns PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(library-cpp-dns PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/dns/cache.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/dns/thread.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/dns/magic.cpp
)
