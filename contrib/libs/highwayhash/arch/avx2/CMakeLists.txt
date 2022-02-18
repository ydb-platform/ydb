add_library(highwayhash-arch-avx2)
target_compile_options(highwayhash-arch-avx2 PRIVATE
  -mavx2
)
target_include_directories(highwayhash-arch-avx2 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash
)
target_link_libraries(highwayhash-arch-avx2 PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(highwayhash-arch-avx2 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/sip_tree_hash.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/hh_avx2.cc
)
