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
