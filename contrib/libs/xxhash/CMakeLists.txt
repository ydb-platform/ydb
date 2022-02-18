add_library(contrib-libs-xxhash)
target_sources(contrib-libs-xxhash PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/xxhash/xxhash.c
)
