add_library(library-cpp-ipmath)
target_link_libraries(library-cpp-ipmath PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-ipv6_address
)
target_sources(library-cpp-ipmath PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/ipmath/ipmath.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/ipmath/range_set.cpp
)
