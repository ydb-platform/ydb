add_library(contrib-libs-double-conversion)
target_include_directories(contrib-libs-double-conversion PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion
)
target_include_directories(contrib-libs-double-conversion PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion
)
target_link_libraries(contrib-libs-double-conversion PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-libs-double-conversion PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/double-conversion/bignum-dtoa.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/double-conversion/bignum.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/double-conversion/cached-powers.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/double-conversion/double-to-string.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/double-conversion/fast-dtoa.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/double-conversion/fixed-dtoa.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/double-conversion/string-to-double.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/double-conversion/strtod.cc
)
