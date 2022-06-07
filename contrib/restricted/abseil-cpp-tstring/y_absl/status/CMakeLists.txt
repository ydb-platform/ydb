
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yabseil-cpp-tstring-y_absl-status)
target_include_directories(yabseil-cpp-tstring-y_absl-status PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabseil-cpp-tstring-y_absl-status PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabseil-cpp-tstring-y_absl-base
  ybase-internal-low_level_alloc
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  ybase-internal-throw_delegate
  yabsl-base-log_severity
  yabseil-cpp-tstring-y_absl-debugging
  yabsl-debugging-stacktrace
  yabsl-debugging-symbolize
  yabseil-cpp-tstring-y_absl-demangle
  yabseil-cpp-tstring-y_absl-numeric
  yprofiling-internal-exponential_biased
  yabseil-cpp-tstring-y_absl-strings
  yabsl-strings-cord
  ystrings-internal-absl_cord_internal
  ystrings-internal-absl_strings_internal
  ystrings-internal-cordz_functions
  ystrings-internal-cordz_handle
  ystrings-internal-cordz_info
  ystrings-internal-str_format
  yabseil-cpp-tstring-y_absl-synchronization
  yabsl-synchronization-internal
  yabseil-cpp-tstring-y_absl-time
  yabsl-time-civil_time
  yabsl-time-time_zone
  yabsl-types-bad_optional_access
)
target_sources(yabseil-cpp-tstring-y_absl-status PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/status/status.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/status/status_payload_printer.cc
)
