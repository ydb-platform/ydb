add_library(farmhash-arch-sse41)
target_compile_options(farmhash-arch-sse41 PRIVATE
  -msse4.1
)
target_link_libraries(farmhash-arch-sse41 PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(farmhash-arch-sse41 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhashnt.cc
)
