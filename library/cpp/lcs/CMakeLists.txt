add_library(library-cpp-lcs)
target_link_libraries(library-cpp-lcs PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-containers-paged_vector
)
target_sources(library-cpp-lcs PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/lcs/lcs_via_lis.cpp
)
