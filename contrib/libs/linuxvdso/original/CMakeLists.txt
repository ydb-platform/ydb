add_library(libs-linuxvdso-original)
target_sources(libs-linuxvdso-original PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/linuxvdso/original/vdso_support.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/linuxvdso/original/elf_mem_image.cc
)
