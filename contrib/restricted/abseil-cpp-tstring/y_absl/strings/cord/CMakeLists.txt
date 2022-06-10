
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(y_absl-strings-cord)
target_include_directories(y_absl-strings-cord PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(y_absl-strings-cord PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-algorithm
  abseil-cpp-tstring-y_absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  y_absl-base-log_severity
  abseil-cpp-tstring-y_absl-container
  abseil-cpp-tstring-y_absl-debugging
  y_absl-debugging-stacktrace
  y_absl-debugging-symbolize
  abseil-cpp-tstring-y_absl-demangle
  abseil-cpp-tstring-y_absl-functional
  abseil-cpp-tstring-y_absl-numeric
  profiling-internal-exponential_biased
  abseil-cpp-tstring-y_absl-strings
  strings-internal-absl_cord_internal
  strings-internal-absl_strings_internal
  strings-internal-cordz_functions
  strings-internal-cordz_handle
  strings-internal-cordz_info
  abseil-cpp-tstring-y_absl-synchronization
  y_absl-synchronization-internal
  abseil-cpp-tstring-y_absl-time
  y_absl-time-civil_time
  y_absl-time-time_zone
  abseil-cpp-tstring-y_absl-types
  y_absl-types-bad_optional_access
  abseil-cpp-tstring-y_absl-utility
)
target_sources(y_absl-strings-cord PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/strings/cord.cc
)
