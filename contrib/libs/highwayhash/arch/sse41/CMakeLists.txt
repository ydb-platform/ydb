add_library(highwayhash-arch-sse41)
target_compile_options(highwayhash-arch-sse41 PRIVATE
  -msse4.1
)
target_include_directories(highwayhash-arch-sse41 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash
)
target_link_libraries(highwayhash-arch-sse41 PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(highwayhash-arch-sse41 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/hh_sse41.cc
)
