add_library(yabseil-cpp-tstring-y_absl-time)
target_include_directories(yabseil-cpp-tstring-y_absl-time PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabseil-cpp-tstring-y_absl-time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabseil-cpp-tstring-y_absl-time PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabseil-cpp-tstring-y_absl-base
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  ybase-internal-throw_delegate
  yabsl-base-log_severity
  yabseil-cpp-tstring-y_absl-numeric
  yabseil-cpp-tstring-y_absl-strings
  ystrings-internal-absl_strings_internal
  yabsl-time-civil_time
  yabsl-time-time_zone
)
target_sources(yabseil-cpp-tstring-y_absl-time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/civil_time.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/clock.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/duration.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/format.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/time.cc
)
