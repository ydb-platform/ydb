add_library(contrib-libs-snappy)
target_compile_options(contrib-libs-snappy PRIVATE
  -DHAVE_CONFIG_H
)
target_include_directories(contrib-libs-snappy PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/snappy/include
)
target_include_directories(contrib-libs-snappy PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/snappy/include
)
target_link_libraries(contrib-libs-snappy PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(contrib-libs-snappy PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/snappy/snappy-c.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/snappy/snappy-sinksource.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/snappy/snappy-stubs-internal.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/snappy/snappy.cc
)
