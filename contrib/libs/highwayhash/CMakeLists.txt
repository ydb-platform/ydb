add_library(contrib-libs-highwayhash)
target_include_directories(contrib-libs-highwayhash PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash
)
target_include_directories(contrib-libs-highwayhash PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash
)
target_link_libraries(contrib-libs-highwayhash PUBLIC
  contrib-libs-cxxsupp
  yutil
  highwayhash-arch-avx2
  highwayhash-arch-sse41
)
target_sources(contrib-libs-highwayhash PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/arch_specific.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/instruction_sets.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/nanobenchmark.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/os_specific.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/sip_hash.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/scalar_sip_tree_hash.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/hh_portable.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash/highwayhash/c_bindings.cc
)
