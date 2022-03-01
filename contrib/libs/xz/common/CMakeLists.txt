
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(libs-xz-common)
target_compile_options(libs-xz-common PRIVATE
  -DTUKLIB_SYMBOL_PREFIX=lzma_
)
target_include_directories(libs-xz-common PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/common
)
target_link_libraries(libs-xz-common PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(libs-xz-common PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/xz/common/tuklib_cpucores.c
)
