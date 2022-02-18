add_library(libs-brotli-common)
target_compile_options(libs-brotli-common PRIVATE
  -DBROTLI_BUILD_PORTABLE
)
target_include_directories(libs-brotli-common PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/include
)
target_link_libraries(libs-brotli-common PUBLIC
  contrib-libs-cxxsupp
)
target_sources(libs-brotli-common PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/common/dictionary.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/common/transform.c
)
