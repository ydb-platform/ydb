add_library(libs-tcmalloc-malloc_extension)
target_compile_options(libs-tcmalloc-malloc_extension PRIVATE
  -DTCMALLOC_256K_PAGES
)
target_include_directories(libs-tcmalloc-malloc_extension PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/tcmalloc
)
target_include_directories(libs-tcmalloc-malloc_extension PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/tcmalloc
)
target_link_libraries(libs-tcmalloc-malloc_extension PUBLIC
  contrib-libs-cxxsupp
  contrib-restricted-abseil-cpp
)
target_sources(libs-tcmalloc-malloc_extension PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/tcmalloc/tcmalloc/malloc_extension.cc
)
