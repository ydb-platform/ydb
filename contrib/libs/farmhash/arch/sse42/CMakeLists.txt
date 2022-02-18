add_library(farmhash-arch-sse42)
target_compile_options(farmhash-arch-sse42 PRIVATE
  -msse4.2
)
target_link_libraries(farmhash-arch-sse42 PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(farmhash-arch-sse42 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhashsa.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhashte.cc
)
