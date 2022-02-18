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
target_sources(python-symbols-libc.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/python/symbols/libc/syms.cpp
)
