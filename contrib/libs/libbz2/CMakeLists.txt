add_library(contrib-libs-libbz2)
target_sources(contrib-libs-libbz2 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/libbz2/blocksort.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libbz2/bzlib.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libbz2/compress.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libbz2/crctable.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libbz2/decompress.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libbz2/huffman.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/libbz2/randtable.c
)
