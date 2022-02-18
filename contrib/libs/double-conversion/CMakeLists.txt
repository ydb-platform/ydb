add_library(contrib-libs-double-conversion)
target_include_directories(contrib-libs-double-conversion PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/include
)
target_include_directories(contrib-libs-double-conversion PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/include
)
target_link_libraries(contrib-libs-double-conversion PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-libs-double-conversion PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/cached-powers.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/bignum-dtoa.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/double-conversion.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/diy-fp.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/fixed-dtoa.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/strtod.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/bignum.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/double-conversion/fast-dtoa.cc
)
