
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(python-symbols-libc INTERFACE)
target_link_libraries(python-symbols-libc INTERFACE
  contrib-libs-cxxsupp
  yutil
  python-symbols-registry
)

add_global_library_for(python-symbols-libc.global python-symbols-libc)
target_compile_options(python-symbols-libc.global PRIVATE
  -Wno-deprecated-declarations
)
target_link_libraries(python-symbols-libc.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  python-symbols-registry
)
target_sources(python-symbols-libc.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/python/symbols/libc/syms.cpp
)
