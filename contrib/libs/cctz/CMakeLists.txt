add_library(contrib-libs-cctz)
target_include_directories(contrib-libs-cctz PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/include
)
target_include_directories(contrib-libs-cctz PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/include
)
target_link_libraries(contrib-libs-cctz PUBLIC
  contrib-libs-cxxsupp
)
target_sources(contrib-libs-cctz PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/civil_time_detail.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/time_zone_fixed.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/time_zone_format.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/time_zone_if.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/time_zone_impl.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/time_zone_info.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/time_zone_libc.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/time_zone_lookup.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/time_zone_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/cctz/src/zone_info_source.cc
)
