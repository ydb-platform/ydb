
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



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
