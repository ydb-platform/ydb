add_library(library-cpp-getopt INTERFACE)
target_link_libraries(library-cpp-getopt INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-getopt-small
  library-cpp-svnversion
  library-cpp-build_info
)

add_global_library_for(library-cpp-getopt.global library-cpp-getopt)
target_sources(library-cpp-getopt.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/getopt/print.cpp
)
