add_library(contrib-libs-lz4)
target_include_directories(contrib-libs-lz4 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/xxhash
)
target_link_libraries(contrib-libs-lz4 PUBLIC
  contrib-libs-xxhash
)
target_sources(contrib-libs-lz4 PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/lz4/lz4.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/lz4/lz4frame.c
  ${CMAKE_SOURCE_DIR}/contrib/libs/lz4/lz4hc.c
)
