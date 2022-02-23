add_library(yabsl-time-time_zone)
target_include_directories(yabsl-time-time_zone PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabsl-time-time_zone PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-time-time_zone PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yabsl-time-time_zone PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/time_zone_fixed.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/time_zone_format.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/time_zone_if.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/time_zone_impl.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/time_zone_info.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/time_zone_libc.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/time_zone_lookup.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/time_zone_posix.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/zone_info_source.cc
)
