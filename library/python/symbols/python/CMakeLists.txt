
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(python-symbols-python INTERFACE)
target_link_libraries(python-symbols-python INTERFACE
  contrib-libs-cxxsupp
  yutil
  python-symbols-registry
)

add_global_library_for(python-symbols-python.global python-symbols-python)
target_link_libraries(python-symbols-python.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  python-symbols-registry
)
target_sources(python-symbols-python.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/python/symbols/python/syms.cpp
)
