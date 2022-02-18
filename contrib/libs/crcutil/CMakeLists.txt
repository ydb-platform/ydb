add_library(contrib-libs-crcutil)
target_compile_options(contrib-libs-crcutil PRIVATE
  -DCRCUTIL_USE_MM_CRC32=1
)
target_link_libraries(contrib-libs-crcutil PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-libs-crcutil PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/crcutil/crc32c_sse4.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/crcutil/multiword_64_64_gcc_i386_mmx.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/crcutil/multiword_128_64_gcc_amd64_sse2.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/crcutil/multiword_64_64_gcc_amd64_asm.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/crcutil/interface.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/crcutil/multiword_64_64_intrinsic_i386_mmx.cc
)
set_property(
  SOURCE
  ${CMAKE_SOURCE_DIR}/contrib/libs/crcutil/crc32c_sse4.cc
  APPEND
  PROPERTY
  COMPILE_OPTIONS
  -msse4.1
  -msse4.2
  -mpopcnt
  -mcx16
)
