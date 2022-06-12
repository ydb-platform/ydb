
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yabsl-strings-cord)
target_include_directories(yabsl-strings-cord PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-strings-cord PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-algorithm
  yabseil-cpp-tstring-y_absl-base
  ybase-internal-low_level_alloc
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  ybase-internal-throw_delegate
  yabsl-base-log_severity
  abseil-cpp-tstring-y_absl-container
  yabseil-cpp-tstring-y_absl-debugging
  yabsl-debugging-stacktrace
  yabsl-debugging-symbolize
  yabseil-cpp-tstring-y_absl-demangle
  abseil-cpp-tstring-y_absl-functional
  yabseil-cpp-tstring-y_absl-numeric
  yprofiling-internal-exponential_biased
  yabseil-cpp-tstring-y_absl-strings
  ystrings-internal-absl_cord_internal
  ystrings-internal-absl_strings_internal
  ystrings-internal-cordz_functions
  ystrings-internal-cordz_handle
  ystrings-internal-cordz_info
  yabseil-cpp-tstring-y_absl-synchronization
  yabsl-synchronization-internal
  yabseil-cpp-tstring-y_absl-time
  yabsl-time-civil_time
  yabsl-time-time_zone
  abseil-cpp-tstring-y_absl-types
  yabsl-types-bad_optional_access
  abseil-cpp-tstring-y_absl-utility
)
target_sources(yabsl-strings-cord PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/strings/cord.cc
)
