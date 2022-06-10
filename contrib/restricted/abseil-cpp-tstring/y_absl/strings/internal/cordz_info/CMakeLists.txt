
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(strings-internal-cordz_info)
target_include_directories(strings-internal-cordz_info PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(strings-internal-cordz_info PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  y_absl-base-log_severity
  abseil-cpp-tstring-y_absl-debugging
  y_absl-debugging-stacktrace
  y_absl-debugging-symbolize
  abseil-cpp-tstring-y_absl-demangle
  abseil-cpp-tstring-y_absl-numeric
  profiling-internal-exponential_biased
  abseil-cpp-tstring-y_absl-strings
  strings-internal-absl_cord_internal
  strings-internal-absl_strings_internal
  strings-internal-cordz_functions
  strings-internal-cordz_handle
  abseil-cpp-tstring-y_absl-synchronization
  y_absl-synchronization-internal
  abseil-cpp-tstring-y_absl-time
  y_absl-time-civil_time
  y_absl-time-time_zone
)
target_sources(strings-internal-cordz_info PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/strings/internal/cordz_info.cc
)
