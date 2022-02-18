add_library(library-cpp-ipv6_address)
target_link_libraries(library-cpp-ipv6_address PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-int128
)
target_sources(library-cpp-ipv6_address PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/ipv6_address/ipv6_address.cpp
)
