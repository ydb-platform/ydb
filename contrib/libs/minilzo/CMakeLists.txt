
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(contrib-libs-minilzo)
target_compile_options(contrib-libs-minilzo PRIVATE
  -DMINILZO_CFG_SKIP_LZO_STRING
)
target_link_libraries(contrib-libs-minilzo PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-libs-minilzo PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/minilzo/minilzo.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/minilzo/lzo_crc.c
)
