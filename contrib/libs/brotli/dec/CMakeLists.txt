add_library(libs-brotli-dec)
target_include_directories(libs-brotli-dec PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/include
)
target_include_directories(libs-brotli-dec PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/include
)
target_link_libraries(libs-brotli-dec PUBLIC
  contrib-libs-cxxsupp
  libs-brotli-common
)
target_sources(libs-brotli-dec PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/dec/bit_reader.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/dec/decode.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/dec/huffman.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/dec/state.c
)
